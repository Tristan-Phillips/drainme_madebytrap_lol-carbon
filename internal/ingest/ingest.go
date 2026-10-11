// Package ingest counts clicks in memory and credits them to the ledger in batches (D16): a click is a map
// increment, and the database sees one transaction per flush however many clicks arrived. A click carries no
// amount: each one adds exactly 1 (D86 R1).
package ingest

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

var (
	// ErrFull refuses a click from an address not yet owed anything: the address cap (D89) is reached, or the
	// in-memory batch is at its bound while an early flush runs. Addresses already owed keep counting.
	ErrFull = errors.New("ingest: no room for a new address right now")
	// ErrHalted is the vault running out (D20), unreachable in practice; every later click is refused.
	ErrHalted = errors.New("ingest: the vault cannot pay any more clicks; the service has halted")
	// ErrStopped refuses clicks after the final flush of a shutdown, so none is accepted and then lost.
	ErrStopped = errors.New("ingest: shutting down")
)

// maxPending bounds the addresses held in memory between flushes. Reaching it starts a flush early instead of
// waiting for the tick, and refuses new addresses only while that flush runs. A flush holds the ledger's one
// writer, and a person's withdrawal and the payout worker's writes wait behind it. Measured at 0.5 CPU under a
// flood of brand-new addresses (D99): withdraw to confirmed payout took up to 18.7 s at 20,000, about 4.4 s at
// 5,000 and 0.7-1.7 s at 2,000, for 18% fewer of the attacker's new addresses accepted.
const maxPending = 2_000

type Counter struct {
	ledger     *ledger.Ledger
	addressMax int64
	log        *slog.Logger
	flushNow   chan struct{}
	// flushing is held from the moment a batch leaves memory until it is in the ledger, so FlushAddress never
	// looks for a click while it is in neither place (a withdrawal straight after a click must include it)
	flushing sync.Mutex

	mu      sync.Mutex
	pending map[nanocrypto.PublicKey]uint64
	// newSlots is how many more addresses may become owed before the next flush recounts. Every address that
	// arrives while slots remain takes one, owed or not, so the count errs towards refusing early, never late.
	newSlots int64
	stopped  error

	// inFlight is a test hook (nil in the binary): it runs while a flushed batch is in neither memory nor the
	// ledger, the window FlushAddress must never fall into
	inFlight func()
}

func New(ctx context.Context, l *ledger.Ledger, addressMax int, log *slog.Logger) (*Counter, error) {
	c := &Counter{ledger: l, addressMax: int64(addressMax), log: log, flushNow: make(chan struct{}, 1), pending: map[nanocrypto.PublicKey]uint64{}}
	if err := c.recount(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// Click counts one click for pub.
func (c *Counter) Click(ctx context.Context, pub nanocrypto.PublicKey) error {
	c.mu.Lock()
	if c.stopped != nil {
		c.mu.Unlock()
		return c.stopped
	}
	if n, ok := c.pending[pub]; ok {
		defer c.mu.Unlock()
		if n == math.MaxUint64 {
			return ErrFull // D86 R7: never wrap; 2^64 clicks inside one flush is not reachable anyway
		}
		c.pending[pub] = n + 1
		return nil
	}
	if len(c.pending) >= maxPending {
		c.mu.Unlock()
		select {
		case c.flushNow <- struct{}{}:
		default: // a flush is already requested
		}
		return ErrFull
	}
	if c.newSlots > 0 {
		c.newSlots--
		c.pending[pub] = 1
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	// the cap is reached, but an address that is already owed takes no new slot
	bal, err := c.ledger.Balance(ctx, pub)
	if err != nil {
		return err
	}
	if bal.Sign() == 0 {
		return ErrFull
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped != nil {
		return c.stopped
	}
	c.pending[pub] = add(c.pending[pub], 1)
	return nil
}

// Pending is how many accepted clicks are waiting for the next flush, so the page can count them before the
// ledger does.
func (c *Counter) Pending() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var total uint64
	for _, n := range c.pending {
		total = add(total, n)
	}
	return total
}

// Flush credits every pending click in one transaction. On a database error the clicks go back into the batch
// for the next attempt; ErrVaultExhausted halts the counter for good.
func (c *Counter) Flush(ctx context.Context) error {
	c.flushing.Lock()
	defer c.flushing.Unlock()
	c.mu.Lock()
	batch := c.pending
	c.pending = map[nanocrypto.PublicKey]uint64{}
	c.mu.Unlock()
	if c.inFlight != nil {
		c.inFlight()
	}
	if err := c.credit(ctx, batch); err != nil {
		return err
	}
	return c.recount(ctx)
}

// FlushAddress credits pub's pending clicks now, so a withdrawal straight after clicking includes them.
func (c *Counter) FlushAddress(ctx context.Context, pub nanocrypto.PublicKey) error {
	c.flushing.Lock()
	defer c.flushing.Unlock()
	c.mu.Lock()
	n, ok := c.pending[pub]
	delete(c.pending, pub)
	c.mu.Unlock()
	if !ok {
		return nil
	}
	return c.credit(ctx, map[nanocrypto.PublicKey]uint64{pub: n})
}

// Run flushes every interval and whenever the batch fills, until ctx ends; then it refuses further clicks and
// flushes one last time (D16). It returns ErrVaultExhausted if the vault runs out, which halts the service.
func (c *Counter) Run(ctx context.Context, every time.Duration) error {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			c.mu.Lock()
			if c.stopped == nil {
				c.stopped = ErrStopped
			}
			c.mu.Unlock()
			// ctx is already over, so the last flush gets its own deadline
			last, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := c.Flush(last); err != nil {
				return err
			}
			return ctx.Err()
		case <-tick.C:
		case <-c.flushNow:
		}
		if err := c.Flush(ctx); errors.Is(err, ledger.ErrVaultExhausted) {
			return err
		} else if err != nil && ctx.Err() == nil { // a flush cut short by shutdown is redone by the last flush
			c.log.Warn("ingest: flush failed; clicks kept for the next one", "err", err)
		}
	}
}

func (c *Counter) credit(ctx context.Context, batch map[nanocrypto.PublicKey]uint64) error {
	if len(batch) == 0 {
		return nil
	}
	err := c.ledger.Credit(ctx, batch)
	if err == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if errors.Is(err, ledger.ErrVaultExhausted) {
		// D20: these clicks cannot be paid by anyone, so they are dropped and the service halts
		c.stopped, c.pending = ErrHalted, map[nanocrypto.PublicKey]uint64{}
		return err
	}
	for pub, n := range batch {
		c.pending[pub] = add(c.pending[pub], n)
	}
	return err
}

func (c *Counter) recount(ctx context.Context) error {
	owed, err := c.ledger.Addresses(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// addresses that arrived during the flush already hold slots, counted as new
	c.newSlots = c.addressMax - owed - int64(len(c.pending))
	return nil
}

// add saturates instead of wrapping (D86 R7); the ceiling is far beyond any real batch.
func add(a, b uint64) uint64 {
	if a > math.MaxUint64-b {
		return math.MaxUint64
	}
	return a + b
}
