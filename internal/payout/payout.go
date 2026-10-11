// Package payout sends queued withdrawals one block at a time. Every block is built from the ledger's own head
// (D86 R6), persisted before broadcast, and re-checked from the persisted bytes before every broadcast (R5), so a
// crash at any point either resends the identical block or sends nothing.
package payout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanoclient"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

// ErrHalt stops the worker for good: the books, the node or a block disagree with what must be true, and only
// the operator can say which is wrong (D20, D24). Funds stay where they are.
var ErrHalt = errors.New("payout: halted")

// a confirmation normally lands within a second or two, so the first check comes soon and later ones back off;
// after the last the block is rebroadcast (D104). In units of poll, which is a second in the binary.
var confirmWaits = []time.Duration{500 * time.Millisecond, 2 * time.Second, 4 * time.Second, 4 * time.Second}

const (
	// how often the send threshold is compared with the network's own minimum (D104)
	floorEvery = time.Hour
	// idle balances are swept at most this often, and only while no withdrawal waits (D96)
	sweepEvery = 30 * time.Second
	// a lagging node can reject our block or account for a moment; every endpoint still rejecting after this many
	// rounds (about a minute) means the chain and the books disagree, which only the operator can resolve
	maxRejections = 60
)

var errIdle = errors.New("payout: queue empty")

type Worker struct {
	ledger    *ledger.Ledger
	node      *nanoclient.Client
	key       nanocrypto.PrivateKey
	pub, rep  nanocrypto.PublicKey
	threshold uint64 // the configured base: every persisted block must clear it (verify)
	target    uint64 // what work is requested at: the base, raised to the network's minimum up to ceiling (D104)
	ceiling   uint64
	floorAt   time.Time
	log       *slog.Logger
	poll      time.Duration
	wake      chan struct{}
	idleAfter time.Duration // 0: never sweep
	sweep     time.Duration // sweepEvery, shortened by tests
	lastSweep time.Time

	pow *pow // proof of work for the next block, fetched as soon as its root is known (D7, D102)
	// started is when the send in flight began building; avgSend is a moving average of how long sends take, in
	// nanoseconds, for the wait estimate /withdraw gives (D104)
	started time.Time
	avgSend atomic.Int64
	// checkpoint is a crash-injection hook for tests (D60); nil in the binary
	checkpoint func(point string)
	confirmed  func(ledger.Signed)
}

// pow is one work_generate for root, running in the background. Only the worker goroutine reads or replaces the
// pointer; the fetch writes value and err, then closes done.
type pow struct {
	root   nanocrypto.Hash
	target uint64
	done   chan struct{}
	value  uint64
	err    error
}

// assumedSend is the wait per payout quoted before any send has been timed
const assumedSend = 5 * time.Second

// ETA estimates how long a withdrawal at position in line waits for its block: position times the average time
// a send has taken lately. Safe to call from any goroutine.
func (w *Worker) ETA(position int64) time.Duration {
	avg := time.Duration(w.avgSend.Load())
	if avg <= 0 {
		avg = assumedSend
	}
	return time.Duration(position) * avg
}

// timed folds one send's duration into the average, each new send weighing an eighth
func (w *Worker) timed(took time.Duration) {
	if old := w.avgSend.Load(); old > 0 {
		took = time.Duration(old) - time.Duration(old)/8 + took/8
	}
	w.avgSend.Store(int64(took))
}

// OnConfirmed registers fn to hear about each send once it is cemented, for the page's payout feed. Set it
// before Run; fn must not block, since the next payout waits for it.
func (w *Worker) OnConfirmed(fn func(ledger.Signed)) { w.confirmed = fn }

func New(l *ledger.Ledger, node *nanoclient.Client, key nanocrypto.PrivateKey, rep nanocrypto.PublicKey, sendThreshold uint64, log *slog.Logger) *Worker {
	return &Worker{ledger: l, node: node, key: key, pub: key.Public(), rep: rep, threshold: sendThreshold, target: sendThreshold,
		ceiling: sendThreshold, log: log, poll: time.Second, wake: make(chan struct{}, 1), sweep: sweepEvery}
}

// SetSendCeiling lets the work threshold follow the network's own minimum up to c (D104). Without it the
// threshold stays at the configured base. Set it before Run.
func (w *Worker) SetSendCeiling(c uint64) {
	if c > w.threshold {
		w.ceiling = c
	}
}

// followFloor raises the work target to the network's minimum when two nodes agree on one above the base, so a
// protocol change cannot strand payouts below it. It never lowers the target below the base or raises it past
// the ceiling, and failing to learn the minimum changes nothing.
func (w *Worker) followFloor(ctx context.Context) {
	w.floorAt = time.Now()
	if w.ceiling == w.threshold {
		return
	}
	network, err := w.node.NetworkMinimum(ctx)
	if err != nil {
		w.log.Warn("payout: could not learn the network's minimum threshold; keeping the current one", "target", fmt.Sprintf("%016x", w.target), "err", err)
		return
	}
	next := max(w.threshold, min(network, w.ceiling))
	if network > w.ceiling {
		w.log.Error("payout: the network's minimum threshold is above DRAINME_SEND_THRESHOLD_MAX; sends may be rejected until it is raised",
			"network", fmt.Sprintf("%016x", network), "ceiling", fmt.Sprintf("%016x", w.ceiling))
	}
	if next != w.target {
		w.log.Warn("payout: work threshold follows the network", "from", fmt.Sprintf("%016x", w.target), "to", fmt.Sprintf("%016x", next))
		w.target = next
	}
}

// SweepIdleAfter turns on idle payouts (D96): a balance with no click for d is paid to its own address, one at a
// time, only while nobody is waiting. Set it before Run.
func (w *Worker) SweepIdleAfter(d time.Duration) { w.idleAfter = d }

// Wake tells an idle worker a withdrawal was queued, so it starts at once instead of at its next poll. It never
// blocks; a wake already pending covers this one.
func (w *Worker) Wake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Run audits the books, cross-checks the chain head, then pays the queue until ctx ends or the worker halts.
// Network trouble is retried forever; only ErrHalt, a ledger invariant or cancellation returns.
func (w *Worker) Run(ctx context.Context) error {
	err := w.retry(ctx, w.ledger.Audit)
	if err == nil {
		err = w.retry(ctx, w.crossCheck)
	}
	if err == nil {
		w.followFloor(ctx)
		// the first payout after a restart should not wait for work either: it builds on the send in flight if
		// there is one, else on the frontier (D7)
		if s, ierr := w.ledger.InFlight(ctx); ierr == nil {
			w.prefetch(ctx, s.Hash)
		} else if frontier, _, herr := w.ledger.Head(ctx); herr == nil {
			w.prefetch(ctx, frontier)
		}
	}
	for err == nil {
		err = w.retry(ctx, w.step)
	}
	return err
}

// retry runs fn until it succeeds. A busy database or an unreachable node is waited out; broken books, a halt,
// or rejections that do not stop between successes end it.
func (w *Worker) retry(ctx context.Context, fn func(context.Context) error) error {
	rejections := 0
	for {
		err := fn(ctx)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, ErrHalt):
			return err
		case errors.Is(err, ledger.ErrInvariant):
			return fmt.Errorf("%w: %w", ErrHalt, err)
		case errors.Is(err, nanoclient.ErrRejected):
			if rejections++; rejections >= maxRejections {
				return fmt.Errorf("%w: rejected %d times with no success between: %w; see drainme status", ErrHalt, rejections, err)
			}
			w.log.Warn("payout: node rejected our chain; will retry", "err", err)
		case !errors.Is(err, errIdle) && ctx.Err() == nil:
			w.log.Warn("payout: will retry", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.poll):
		case <-w.wake:
		}
	}
}

// crossCheck compares our head with the node's (D24). The one accepted difference is our own persisted send
// already being on chain. The node's numbers are never used for anything else.
func (w *Worker) crossCheck(ctx context.Context) error {
	frontier, bal, err := w.ledger.Head(ctx)
	if errors.Is(err, ledger.ErrNoHead) {
		return fmt.Errorf("%w: %w", ErrHalt, err)
	}
	if err != nil {
		return err
	}
	nodeFrontier, nodeBal, err := w.node.AccountInfo(ctx, w.pub)
	if err != nil {
		return err
	}
	if nodeFrontier == frontier && nodeBal.Cmp(bal) == 0 {
		return nil
	}
	s, err := w.ledger.InFlight(ctx)
	if err == nil && nodeFrontier == s.Hash && nodeBal.Cmp(new(big.Int).Sub(bal, s.Amount)) == 0 {
		return nil
	}
	if err != nil && !errors.Is(err, ledger.ErrNotInFlight) {
		return err
	}
	return fmt.Errorf("%w: node reports frontier %s balance %s, ledger has %s balance %s; see drainme status, then reconcile",
		ErrHalt, nodeFrontier.Hex(), nodeBal, frontier.Hex(), bal)
}

// step settles the send in flight if there is one, otherwise signs the next withdrawal. The chain is sequential,
// so nothing new is built while a send is unconfirmed.
func (w *Worker) step(ctx context.Context) error {
	if time.Since(w.floorAt) >= floorEvery {
		w.followFloor(ctx)
	}
	s, err := w.ledger.InFlight(ctx)
	if err == nil {
		return w.settle(ctx, s)
	}
	if !errors.Is(err, ledger.ErrNotInFlight) {
		return err
	}
	next, err := w.ledger.Next(ctx)
	if errors.Is(err, ledger.ErrQueueEmpty) {
		return w.sweepIdle(ctx)
	}
	if err != nil {
		return err
	}
	return w.sign(ctx, next)
}

// sweepIdle queues one idle balance when the queue is empty and a sweep is due. The next step pays it like any
// other withdrawal; a person who withdraws meanwhile waits behind at most this one.
func (w *Worker) sweepIdle(ctx context.Context) error {
	if w.idleAfter == 0 || time.Since(w.lastSweep) < w.sweep {
		return errIdle
	}
	w.lastSweep = time.Now()
	pub, amount, err := w.ledger.SweepIdle(ctx, w.lastSweep.Add(-w.idleAfter))
	if errors.Is(err, ledger.ErrNothingIdle) {
		return errIdle
	}
	if err != nil {
		return err
	}
	w.log.Info("payout: idle balance queued", "to", pub.Address(), "raw", amount.String())
	return nil
}

func (w *Worker) sign(ctx context.Context, wd ledger.Withdrawal) error {
	if w.started.IsZero() {
		w.started = time.Now()
	}
	frontier, bal, err := w.ledger.Head(ctx)
	if err != nil {
		return err
	}
	if bal.Cmp(wd.Amount) < 0 {
		return fmt.Errorf("%w: withdrawal %d of %s exceeds the vault's %s", ErrHalt, wd.ID, wd.Amount, bal)
	}
	work, err := w.workFor(ctx, frontier)
	if err != nil {
		return err
	}
	b := nanoclient.Block{StateBlock: nanocrypto.StateBlock{
		Account: w.pub, Previous: frontier, Representative: w.rep, Balance: new(big.Int).Sub(bal, wd.Amount), Link: wd.Address,
	}, Work: work}
	h, err := b.Hash()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrHalt, err)
	}
	b.Signature = w.key.Sign(h)
	w.hit("built")
	err = w.ledger.MarkSigned(ctx, wd, h, b.Encode())
	if errors.Is(err, ledger.ErrChanged) {
		return nil // a withdrawal merged into this row meanwhile; the next step rebuilds with the new amount
	}
	if err != nil {
		return err
	}
	// the next block builds on this one, whose hash is final now: its work is fetched while this one confirms
	w.prefetch(ctx, h)
	w.hit("signed")
	return nil
}

// settle broadcasts the persisted send and waits for confirmation. Rebroadcasting is safe: it is the same block.
func (w *Worker) settle(ctx context.Context, s ledger.Signed) error {
	if err := w.verify(ctx, s); err != nil {
		return err
	}
	if err := w.node.Process(ctx, s.Hash, s.Block, "send"); errors.Is(err, nanoclient.ErrFork) {
		return fmt.Errorf("%w: %w (send %s); see drainme status, then recover", ErrHalt, err, s.Hash.Hex())
	} else if err != nil {
		return err
	}
	w.hit("broadcast")
	for _, wait := range confirmWaits {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(int64(wait) * int64(w.poll) / int64(time.Second))):
		}
		ok, err := w.node.Confirmed(ctx, s.Hash)
		if err != nil {
			return err
		}
		if ok {
			if err := w.ledger.MarkConfirmed(ctx, s.ID); err != nil {
				return err
			}
			if !w.started.IsZero() { // zero for a send already in flight at boot, whose start is unknown
				w.timed(time.Since(w.started))
				w.started = time.Time{}
			}
			w.log.Info("payout confirmed", "id", s.ID, "to", s.Address.Address(), "raw", s.Amount.String(), "hash", s.Hash.Hex())
			if w.confirmed != nil {
				w.confirmed(s)
			}
			w.prefetch(ctx, s.Hash) // already running since the block was signed, unless the worker restarted since
			w.hit("confirmed")
			return nil
		}
	}
	return fmt.Errorf("payout: send %s not confirmed after %d checks; rebroadcasting", s.Hash.Hex(), len(confirmWaits))
}

// verify is R5, run on the bytes read back from the ledger, which are exactly the bytes about to be broadcast.
func (w *Worker) verify(ctx context.Context, s ledger.Signed) error {
	frontier, bal, err := w.ledger.Head(ctx)
	if err != nil {
		return err
	}
	b, err := nanoclient.DecodeBlock(s.Block)
	if err != nil {
		return fmt.Errorf("%w: persisted send %d: %w", ErrHalt, s.ID, err)
	}
	h, _ := b.Hash() // DecodeBlock already checked the balance range
	var problem string
	switch {
	case h != s.Hash:
		problem = "block does not hash to its recorded hash"
	case b.Account != w.pub:
		problem = "block is for another account"
	case !nanocrypto.Verify(w.pub, h, b.Signature):
		problem = "signature does not verify"
	case b.Previous != frontier:
		problem = "previous is not our frontier"
	case new(big.Int).Sub(bal, b.Balance).Cmp(s.Amount) != 0:
		problem = "block pays a different amount than was debited"
	case b.Link != s.Address:
		problem = "block pays a different address than the withdrawal"
	case nanocrypto.Difficulty(b.Previous, b.Work) < w.threshold:
		problem = "work is below the threshold"
	default:
		return nil
	}
	return fmt.Errorf("%w: persisted send %d: %s", ErrHalt, s.ID, problem)
}

// prefetch starts fetching work for root unless that fetch is already running or done. ctx is Run's, so the fetch
// outlives the step that started it.
func (w *Worker) prefetch(ctx context.Context, root nanocrypto.Hash) {
	if w.pow != nil && w.pow.root == root && w.pow.target >= w.target {
		return
	}
	p := &pow{root: root, target: w.target, done: make(chan struct{})}
	w.pow = p
	go func() {
		defer close(p.done)
		p.value, p.err = w.node.Work(ctx, root, p.target)
	}()
}

// workFor waits for the work for root, starting the fetch if nothing has. A failed fetch is forgotten, so the next
// call asks again.
func (w *Worker) workFor(ctx context.Context, root nanocrypto.Hash) (uint64, error) {
	w.prefetch(ctx, root)
	p := w.pow
	select {
	case <-p.done:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if p.err != nil {
		w.pow = nil
		return 0, p.err
	}
	return p.value, nil
}

func (w *Worker) hit(point string) {
	if w.checkpoint != nil {
		w.checkpoint(point)
	}
}
