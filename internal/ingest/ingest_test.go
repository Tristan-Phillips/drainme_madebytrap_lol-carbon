package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"math/big"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

var ctx = context.Background()

// any 32 bytes make a well-formed address, and deriving real keys for tens of thousands would be slow
func pub(i int) nanocrypto.PublicKey {
	return nanocrypto.PublicKey{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i), 0xc1}
}

func setup(t *testing.T, vault int64, addressMax int) (*ledger.Ledger, *Counter) {
	t.Helper()
	l, err := ledger.Open(filepath.Join(t.TempDir(), "ledger.db"), 1000)
	must(t, err)
	t.Cleanup(func() { l.Close() })
	must(t, l.SetHead(ctx, nanocrypto.Hash{1}, big.NewInt(vault)))
	c, err := New(ctx, l, addressMax, slog.New(slog.NewTextHandler(io.Discard, nil)))
	must(t, err)
	return l, c
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func clicks(t *testing.T, c *Counter, p nanocrypto.PublicKey, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		must(t, c.Click(ctx, p))
	}
}

func balance(t *testing.T, l *ledger.Ledger, p nanocrypto.PublicKey) int64 {
	t.Helper()
	b, err := l.Balance(ctx, p)
	must(t, err)
	return b.Int64()
}

// every accepted click lands exactly once, however clicks and flushes interleave
func TestConcurrentClicksAllLand(t *testing.T) {
	l, c := setup(t, 1_000_000, 100)
	const workers, each, addrs = 16, 500, 7
	stop := make(chan struct{})
	flushed := make(chan struct{})
	go func() {
		defer close(flushed)
		for {
			select {
			case <-stop:
				return
			default:
				if err := c.Flush(ctx); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if err := c.Click(ctx, pub((w+i)%addrs)); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	<-flushed
	must(t, c.Flush(ctx))
	var total int64
	for a := 0; a < addrs; a++ {
		total += balance(t, l, pub(a))
	}
	if total != workers*each {
		t.Fatalf("credited %d of %d clicks", total, workers*each)
	}
	must(t, l.Audit(ctx))
}

// D89: past the cap a new address is refused, an owed one is not, and withdrawing frees a slot
func TestAddressCap(t *testing.T) {
	l, c := setup(t, 1000, 3)
	clicks(t, c, pub(1), 2)
	clicks(t, c, pub(2), 1)
	clicks(t, c, pub(3), 1)
	if err := c.Click(ctx, pub(4)); !errors.Is(err, ErrFull) {
		t.Fatalf("fourth address: %v", err)
	}
	clicks(t, c, pub(1), 1) // pending, already holds a slot
	must(t, c.Flush(ctx))
	clicks(t, c, pub(2), 1) // owed in the ledger, not pending: found by the balance check
	if err := c.Click(ctx, pub(4)); !errors.Is(err, ErrFull) {
		t.Fatalf("fourth address after flush: %v", err)
	}
	must(t, c.Flush(ctx))
	if _, _, err := l.Withdraw(ctx, pub(3)); err != nil {
		t.Fatal(err)
	}
	must(t, c.Flush(ctx)) // recounts
	clicks(t, c, pub(4), 1)
	must(t, c.Flush(ctx))
	if n, _ := l.Addresses(ctx); n != 3 || balance(t, l, pub(1)) != 3 || balance(t, l, pub(2)) != 2 || balance(t, l, pub(4)) != 1 {
		t.Fatalf("%d owed; balances %d %d %d", n, balance(t, l, pub(1)), balance(t, l, pub(2)), balance(t, l, pub(4)))
	}
}

// the in-memory batch is bounded: at maxPending a new address asks for an early flush and is refused meanwhile
func TestPendingBound(t *testing.T) {
	l, c := setup(t, 1_000_000, maxPending+10)
	for i := 0; i < maxPending; i++ {
		must(t, c.Click(ctx, pub(i)))
	}
	if err := c.Click(ctx, pub(maxPending)); !errors.Is(err, ErrFull) {
		t.Fatalf("over the bound: %v", err)
	}
	if len(c.flushNow) != 1 {
		t.Fatal("no early flush requested")
	}
	must(t, c.Click(ctx, pub(0))) // already pending: still counts
	must(t, c.Flush(ctx))
	must(t, c.Click(ctx, pub(maxPending)))
	must(t, c.Flush(ctx))
	if n, _ := l.Addresses(ctx); n != maxPending+1 || balance(t, l, pub(0)) != 2 {
		t.Fatalf("%d owed, pub(0) has %d", n, balance(t, l, pub(0)))
	}
}

// a failed flush keeps its clicks for the next one
func TestFailedFlushKeepsClicks(t *testing.T) {
	l, c := setup(t, 100, 10)
	clicks(t, c, pub(1), 3)
	dead, cancel := context.WithCancel(ctx)
	cancel()
	if err := c.Flush(dead); err == nil {
		t.Fatal("flush with a cancelled context succeeded")
	}
	clicks(t, c, pub(1), 2)
	must(t, c.Flush(ctx))
	if b := balance(t, l, pub(1)); b != 5 {
		t.Fatalf("balance %d, want 5", b)
	}
}

// D20: a batch the vault cannot pay is refused whole, and the counter halts
func TestVaultExhaustedHalts(t *testing.T) {
	l, c := setup(t, 5, 10)
	clicks(t, c, pub(1), 4)
	must(t, c.Flush(ctx))
	clicks(t, c, pub(2), 2)
	if err := c.Flush(ctx); !errors.Is(err, ledger.ErrVaultExhausted) {
		t.Fatalf("want ErrVaultExhausted, got %v", err)
	}
	if err := c.Click(ctx, pub(1)); !errors.Is(err, ErrHalted) {
		t.Fatalf("click after halt: %v", err)
	}
	if balance(t, l, pub(1)) != 4 || balance(t, l, pub(2)) != 0 {
		t.Fatal("a partial batch was credited")
	}
	must(t, l.Audit(ctx))
}

func TestFlushAddress(t *testing.T) {
	l, c := setup(t, 100, 10)
	clicks(t, c, pub(1), 3)
	clicks(t, c, pub(2), 4)
	must(t, c.FlushAddress(ctx, pub(1)))
	must(t, c.FlushAddress(ctx, pub(3))) // nothing pending: nothing to do
	if balance(t, l, pub(1)) != 3 || balance(t, l, pub(2)) != 0 {
		t.Fatal("FlushAddress credited the wrong clicks")
	}
	must(t, c.Flush(ctx))
	if balance(t, l, pub(2)) != 4 {
		t.Fatal("the other address's clicks were lost")
	}
}

// D16: shutdown flushes what is held, and nothing is accepted after that flush
func TestRunFlushesOnShutdown(t *testing.T) {
	l, c := setup(t, 100, 10)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx, time.Hour) }()
	clicks(t, c, pub(1), 3)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if b := balance(t, l, pub(1)); b != 3 {
		t.Fatalf("balance %d after shutdown, want 3", b)
	}
	if err := c.Click(ctx, pub(1)); !errors.Is(err, ErrStopped) {
		t.Fatalf("click after shutdown: %v", err)
	}
}

// the ticker and an early-flush request both flush
func TestRunFlushes(t *testing.T) {
	l, c := setup(t, 100, 10)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go c.Run(runCtx, 5*time.Millisecond)
	clicks(t, c, pub(1), 2)
	deadline := time.Now().Add(5 * time.Second)
	for balance(t, l, pub(1)) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("ticker never flushed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAddSaturates(t *testing.T) {
	if add(math.MaxUint64-1, 5) != math.MaxUint64 || add(2, 3) != 5 {
		t.Fatal("add wraps or miscounts")
	}
}

// an address that arrives while a flush runs already holds a slot, so the recount at the end of that flush must
// not hand the slot out again
func TestRecountKeepsSlotsTakenDuringFlush(t *testing.T) {
	_, c := setup(t, 100, 1)
	clicks(t, c, pub(1), 1) // pending, not yet in the ledger, as if it arrived mid-flush
	must(t, c.recount(ctx))
	if err := c.Click(ctx, pub(2)); !errors.Is(err, ErrFull) {
		t.Fatalf("cap of 1 handed out twice: %v", err)
	}
}

func TestPending(t *testing.T) {
	_, c := setup(t, 100, 10)
	clicks(t, c, pub(1), 3)
	clicks(t, c, pub(2), 2)
	if n := c.Pending(); n != 5 {
		t.Fatalf("pending %d, want 5", n)
	}
	must(t, c.Flush(ctx))
	if n := c.Pending(); n != 0 {
		t.Fatalf("pending %d after flush", n)
	}
}

// under full load, shutting down loses nothing: every click that was accepted is in the ledger afterwards
func TestShutdownUnderLoadLosesNothing(t *testing.T) {
	l, c := setup(t, 1_000_000_000, 100_000)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx, time.Millisecond) }() // flushing constantly, so shutdown lands mid-flush
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				err := c.Click(ctx, pub(w*1_000_000+i%500))
				if errors.Is(err, ErrStopped) {
					return
				}
				if err == nil {
					accepted.Add(1)
				}
			}
		}(w)
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	wg.Wait()
	_, _, credited, err := l.Totals(ctx)
	must(t, err)
	if credited.Int64() != accepted.Load() || accepted.Load() == 0 {
		t.Fatalf("accepted %d clicks, ledger credited %s", accepted.Load(), credited)
	}
}

// a withdrawal straight after a click includes it, even while a periodic flush has it in hand: the flush is
// paused exactly between taking the batch out of memory and committing it, and FlushAddress runs inside that
func TestFlushAddressWaitsForAFlushInProgress(t *testing.T) {
	l, c := setup(t, 1_000_000, 1000)
	must(t, c.Click(ctx, pub(1)))
	inWindow, release := make(chan struct{}), make(chan struct{})
	c.inFlight = func() { close(inWindow); <-release }
	go c.Flush(ctx)
	<-inWindow
	c.inFlight = nil
	returned := make(chan struct{})
	go func() { must(t, c.FlushAddress(ctx, pub(1))); close(returned) }()
	select {
	case <-returned:
		t.Fatal("FlushAddress returned while the click was in neither memory nor the ledger")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	<-returned
	if b := balance(t, l, pub(1)); b != 1 {
		t.Fatalf("balance %d after FlushAddress, want 1", b)
	}
}
