package payout

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/fakenode"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanoclient"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

// low enough that the fake node finds work instantly, high enough that a random value usually fails it
const threshold = 0xff00000000000000

var (
	key     = nanocrypto.DeriveKey([32]byte{0x71}, 0)
	rep     = nanocrypto.DeriveKey([32]byte{0x72}, 0).Public()
	genesis = nanocrypto.Hash{0x6e, 0x02}
	vault   = new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil) // the real 1 XNO in raw
)

func addr(i int) nanocrypto.PublicKey {
	return nanocrypto.DeriveKey([32]byte{byte(i), 0x73}, 0).Public()
}

type env struct {
	t    *testing.T
	path string
	l    *ledger.Ledger
	node *fakenode.Node
	want map[nanocrypto.PublicKey]*big.Int // what each address withdrew, so what the node must have paid it
}

func newEnv(t *testing.T) *env {
	e := &env{t: t, path: filepath.Join(t.TempDir(), "ledger.db"), want: map[nanocrypto.PublicKey]*big.Int{}}
	e.open()
	t.Cleanup(func() { e.l.Close() })
	must(t, e.l.SetHead(context.Background(), genesis, vault))
	e.node = fakenode.New(t, key.Public(), genesis, vault, threshold)
	return e
}

func (e *env) open() {
	l, err := ledger.Open(e.path, 1000)
	must(e.t, err)
	e.l = l
}

// reopen is the restart half of a crash: nothing survives but the database file
func (e *env) reopen() {
	must(e.t, e.l.Close())
	e.open()
}

// take credits clicks to address i and withdraws them
func (e *env) take(i int, clicks uint64) {
	ctx := context.Background()
	must(e.t, e.l.Credit(ctx, map[nanocrypto.PublicKey]uint64{addr(i): clicks}))
	_, _, err := e.l.Withdraw(ctx, addr(i))
	must(e.t, err)
	if e.want[addr(i)] == nil {
		e.want[addr(i)] = new(big.Int)
	}
	e.want[addr(i)].Add(e.want[addr(i)], new(big.Int).SetUint64(clicks))
}

func (e *env) queue() {
	for i, n := range []uint64{1, 7, 1_000_003, 42} {
		e.take(i+1, n)
	}
}

func (e *env) worker() *Worker {
	urls := []*url.URL{e.node.URL}
	w := New(e.l, nanoclient.New(urls, urls, 5*time.Second), key, rep, threshold, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.poll = time.Millisecond
	return w
}

type crash struct{ point string }

// run drives the worker until it crashes at an injected checkpoint, returns on its own (a halt), or has settled
// the whole queue, in which case it is cancelled and returns context.Canceled.
func (e *env) run(w *Worker) (crashedAt string, err error) {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		crashedAt string
		err       error
	}
	done := make(chan result, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				c, ok := r.(crash)
				if !ok {
					panic(r)
				}
				done <- result{crashedAt: c.point}
			}
		}()
		done <- result{err: w.Run(ctx)}
	}()
	deadline := time.After(20 * time.Second)
	for {
		select {
		case r := <-done:
			return r.crashedAt, r.err
		case <-deadline:
			e.t.Fatal("worker neither settled nor stopped")
		case <-time.After(2 * time.Millisecond):
			if e.settled() {
				cancel()
				r := <-done
				return r.crashedAt, r.err
			}
		}
	}
}

func (e *env) settled() bool {
	ctx := context.Background()
	_, err := e.l.Next(ctx)
	_, err2 := e.l.InFlight(ctx)
	return errors.Is(err, ledger.ErrQueueEmpty) && errors.Is(err2, ledger.ErrNotInFlight)
}

// paidExactly is the claim at the end of every test: each address got exactly what it withdrew, once, and the
// node's chain and the ledger agree on where the vault stands.
func (e *env) paidExactly() {
	e.t.Helper()
	total := new(big.Int)
	for pub, want := range e.want {
		if got := e.node.Paid(pub); got.Cmp(want) != 0 {
			e.t.Errorf("%s paid %s, withdrew %s", pub.Address(), got, want)
		}
		total.Add(total, want)
	}
	frontier, bal := e.node.Head()
	lf, lb, err := e.l.Head(context.Background())
	must(e.t, err)
	if frontier != lf || bal.Cmp(lb) != 0 || new(big.Int).Sub(vault, bal).Cmp(total) != 0 {
		e.t.Errorf("node head %X %s, ledger head %X %s, total withdrawn %s", frontier, bal, lf, lb, total)
	}
	must(e.t, e.l.Audit(context.Background()))
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func wantHalt(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrHalt) {
		t.Fatalf("want a halt, got %v", err)
	}
}

func TestPaysTheQueue(t *testing.T) {
	e := newEnv(t)
	e.queue()
	w := e.worker()
	heard := map[nanocrypto.PublicKey]*big.Int{} // what the page's payout feed is told
	w.OnConfirmed(func(s ledger.Signed) { heard[s.Address] = s.Amount })
	if _, err := e.run(w); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	e.paidExactly()
	if e.node.Blocks() != 4 || len(heard) != 4 {
		t.Fatalf("%d blocks and %d confirmations for 4 withdrawals", e.node.Blocks(), len(heard))
	}
	for pub, amt := range heard {
		if amt.Cmp(e.want[pub]) != 0 {
			t.Errorf("feed told %s for %s, paid %s", amt, pub.Address(), e.want[pub])
		}
	}
}

var checkpoints = []string{"built", "signed", "broadcast", "confirmed"}

// D60: crash at each checkpoint of each payout in turn, restart on the same database file, and the end state
// must be the same as if nothing had happened: every address paid exactly once, nothing extra on chain.
func TestCrashAtEveryCheckpoint(t *testing.T) {
	var seen []string
	{
		e := newEnv(t)
		e.queue()
		w := e.worker()
		w.checkpoint = func(p string) { seen = append(seen, p) }
		e.run(w)
		e.paidExactly()
	}
	if len(seen) != 4*len(checkpoints) {
		t.Fatalf("a clean run passed %d checkpoints: %v", len(seen), seen)
	}
	for k := 1; k <= len(seen); k++ {
		t.Run(fmt.Sprintf("%d-%s", k, seen[k-1]), func(t *testing.T) {
			e := newEnv(t)
			e.queue()
			w, n := e.worker(), 0
			w.checkpoint = func(p string) {
				if n++; n == k {
					panic(crash{p})
				}
			}
			if at, err := e.run(w); at != seen[k-1] {
				t.Fatalf("crashed at %q (%v), want %q", at, err, seen[k-1])
			}
			e.reopen()
			if _, err := e.run(e.worker()); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			e.paidExactly()
			if e.node.Blocks() != 4 {
				t.Fatalf("%d blocks for 4 withdrawals", e.node.Blocks())
			}
		})
	}
}

// crash again and again, each restart crashing at its nth checkpoint, so restarts also follow restarts
func TestCrashRepeatedly(t *testing.T) {
	for _, nth := range []int{2, 3} {
		t.Run(fmt.Sprint(nth), func(t *testing.T) {
			e := newEnv(t)
			e.queue()
			for restarts := 0; ; restarts++ {
				if restarts > 50 {
					t.Fatal("no progress across restarts")
				}
				w, n := e.worker(), 0
				w.checkpoint = func(p string) {
					if n++; n == nth {
						panic(crash{p})
					}
				}
				at, err := e.run(w)
				e.reopen()
				if at == "" {
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
					break
				}
			}
			e.paidExactly()
			if e.node.Blocks() != 4 {
				t.Fatalf("%d blocks for 4 withdrawals", e.node.Blocks())
			}
		})
	}
}

// R5 against a hostile database: a send that is consistent, correctly hashed and validly signed by our own key,
// but pays more than was debited, must never leave. Only the pre-broadcast check stands in its way.
func TestTamperedPersistedBlockIsNeverBroadcast(t *testing.T) {
	e := newEnv(t)
	e.take(1, 10)
	w := e.worker()
	w.checkpoint = func(p string) {
		if p == "signed" {
			panic(crash{p})
		}
	}
	if at, _ := e.run(w); at != "signed" {
		t.Fatalf("crashed at %q", at)
	}
	e.l.Close()

	b := nanoclient.Block{StateBlock: nanocrypto.StateBlock{Account: key.Public(), Previous: genesis, Representative: rep,
		Balance: new(big.Int).Sub(vault, big.NewInt(1_000_000)), Link: addr(1)}}
	b.Work = mustWork(genesis)
	h, _ := b.Hash()
	b.Signature = key.Sign(h)
	db, err := sql.Open("sqlite", e.path)
	must(t, err)
	_, err = db.Exec(`DROP TRIGGER withdrawal_final_once_signed`)
	must(t, err)
	_, err = db.Exec(`UPDATE withdrawals SET block_hash = ?, block = ? WHERE state = 'signed'`, fmt.Sprintf("%X", h), string(b.Encode()))
	must(t, err)
	must(t, db.Close())

	e.open()
	_, err = e.run(e.worker())
	wantHalt(t, err)
	if e.node.Processed() != 0 {
		t.Fatalf("the node saw %d process calls", e.node.Processed())
	}
}

func mustWork(root nanocrypto.Hash) uint64 {
	for w := uint64(0); ; w++ {
		if nanocrypto.Difficulty(root, w) >= threshold {
			return w
		}
	}
}

// R5, case by case: each block differs from a correct one in exactly one way, and is signed by our key where
// that is possible, so only the named check can catch it.
func TestVerify(t *testing.T) {
	e := newEnv(t)
	w := e.worker()
	other := nanocrypto.DeriveKey([32]byte{0x74}, 0)
	amount := big.NewInt(25)
	good := func() nanoclient.Block {
		return nanoclient.Block{StateBlock: nanocrypto.StateBlock{Account: key.Public(), Previous: genesis, Representative: rep,
			Balance: new(big.Int).Sub(vault, amount), Link: addr(1)}, Work: mustWork(genesis)}
	}
	sign := func(b nanoclient.Block, k nanocrypto.PrivateKey) ledger.Signed {
		h, _ := b.Hash()
		b.Signature = k.Sign(h)
		return ledger.Signed{Withdrawal: ledger.Withdrawal{ID: 1, Address: addr(1), Amount: amount}, Hash: h, Block: b.Encode()}
	}
	must(t, w.verify(context.Background(), sign(good(), key)))

	cases := map[string]ledger.Signed{}
	b := good()
	b.Balance = new(big.Int).Sub(b.Balance, big.NewInt(1))
	cases["pays one raw more"] = sign(b, key)
	b = good()
	b.Balance = new(big.Int).Add(b.Balance, big.NewInt(1))
	cases["pays one raw less"] = sign(b, key)
	b = good()
	b.Link = addr(2)
	cases["pays someone else"] = sign(b, key)
	b = good()
	b.Previous = nanocrypto.Hash{0x99}
	b.Work = mustWork(b.Previous)
	cases["builds on another previous"] = sign(b, key)
	b = good()
	b.Account = other.Public()
	cases["another account"] = sign(b, key) // our key over another account's block: only the account check sees it
	cases["signed by another key"] = sign(good(), other)
	s := sign(good(), key)
	s.Hash[0] ^= 1
	cases["recorded hash differs"] = s
	b = good()
	for nanocrypto.Difficulty(genesis, b.Work) >= threshold {
		b.Work++
	}
	cases["work below threshold"] = sign(b, key)
	s = sign(good(), key)
	s.Block = append([]byte(" "), s.Block...)
	cases["not canonical"] = s

	for name, s := range cases {
		if err := w.verify(context.Background(), s); !errors.Is(err, ErrHalt) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// D24 and R6: the node is asked, never believed. Any disagreement at boot halts before anything is sent.
func TestHaltsWhenNodeDisagrees(t *testing.T) {
	for name, lie := range map[string]map[string]string{
		"lower balance":  {"frontier": fmt.Sprintf("%X", genesis), "balance": "1"},
		"higher balance": {"frontier": fmt.Sprintf("%X", genesis), "balance": new(big.Int).Add(vault, big.NewInt(1)).String()},
		"other frontier": {"frontier": fmt.Sprintf("%X", nanocrypto.Hash{0x42}), "balance": vault.String()},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.take(1, 10)
			e.node.Intercept = func(action string) any {
				if action == "account_info" {
					return lie
				}
				return nil
			}
			_, err := e.run(e.worker())
			wantHalt(t, err)
			if e.node.Processed() != 0 {
				t.Fatal("sent despite the disagreement")
			}
		})
	}
}

func TestHaltsBeforeInitVault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	l, err := ledger.Open(path, 10)
	must(t, err)
	defer l.Close()
	e := &env{t: t, l: l, node: fakenode.New(t, key.Public(), genesis, vault, threshold)}
	_, err = e.run(e.worker())
	if !errors.Is(err, ErrHalt) || !errors.Is(err, ledger.ErrNoHead) {
		t.Fatalf("want a halt on ErrNoHead, got %v", err)
	}
}

func TestHaltsWhenBooksDoNotBalance(t *testing.T) {
	e := newEnv(t)
	e.take(1, 10)
	e.l.Close()
	db, err := sql.Open("sqlite", e.path)
	must(t, err)
	_, err = db.Exec(`UPDATE vault SET available = '1'`)
	must(t, err)
	must(t, db.Close())
	e.open()
	_, err = e.run(e.worker())
	if !errors.Is(err, ErrHalt) || !errors.Is(err, ledger.ErrInvariant) {
		t.Fatalf("want a halt on the audit, got %v", err)
	}
}

// someone else's block on our frontier: halt, and leave the send in flight for the operator (D14)
func TestHaltsOnFork(t *testing.T) {
	e := newEnv(t)
	e.take(1, 10)
	e.node.Intercept = func(action string) any {
		if action == "process" {
			return map[string]string{"error": "Fork"}
		}
		return nil
	}
	_, err := e.run(e.worker())
	wantHalt(t, err)
	if _, err := e.l.InFlight(context.Background()); err != nil {
		t.Fatalf("the send should stay in flight: %v", err)
	}
	must(t, e.l.Audit(context.Background()))
}

// a slow network: the block is rebroadcast while unconfirmed, and still lands exactly once
func TestSlowConfirmationRebroadcasts(t *testing.T) {
	e := newEnv(t)
	e.take(1, 10)
	e.node.PendingPolls = 3*len(confirmWaits) + 1
	if _, err := e.run(e.worker()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	e.paidExactly()
	if e.node.Blocks() != 1 || e.node.Processed() < 4 {
		t.Fatalf("%d blocks from %d broadcasts", e.node.Blocks(), e.node.Processed())
	}
}

// every endpoint failing for a while is weather, not a halt: the worker waits it out
func TestRetriesThroughOutages(t *testing.T) {
	e := newEnv(t)
	e.queue()
	calls := 0
	e.node.Intercept = func(action string) any {
		// outages come in windows: seven failing calls, then seven answered, over and over
		if calls++; (calls/7)%2 == 0 {
			return map[string]string{"error": "Too many requests"}
		}
		return nil
	}
	if _, err := e.run(e.worker()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	e.paidExactly()
}

// a withdrawal merging into the row while its block is being built: the stale block is never persisted, and
// one block pays the merged total
func TestMergeWhileBuilding(t *testing.T) {
	e := newEnv(t)
	e.take(1, 10)
	w, merged := e.worker(), false
	w.checkpoint = func(p string) {
		if p == "built" && !merged {
			merged = true
			e.take(1, 5)
		}
	}
	if _, err := e.run(w); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	e.paidExactly()
	if e.node.Blocks() != 1 {
		t.Fatalf("%d blocks for one merged withdrawal", e.node.Blocks())
	}
}

// D7, D102: work for the next block is fetched from the moment this one is signed, and each block costs one work
// request
func TestPrecachesWork(t *testing.T) {
	e := newEnv(t)
	e.queue()
	var works atomic.Int32
	e.node.Intercept = func(action string) any {
		if action == "work_generate" {
			works.Add(1)
		}
		return nil
	}
	// stepped by hand: under run, cancelling once the queue settles can cut the final precache short
	w := e.worker()
	signed := 0
	w.checkpoint = func(point string) {
		if point != "signed" {
			return
		}
		signed++
		s, err := e.l.InFlight(context.Background())
		must(t, err)
		if w.pow == nil || w.pow.root != s.Hash {
			t.Errorf("send %d signed, but no work is being fetched for the block after it", signed)
		}
	}
	must(t, w.crossCheck(context.Background()))
	for {
		err := w.step(context.Background())
		if errors.Is(err, errIdle) {
			break
		}
		must(t, err)
	}
	e.paidExactly()
	<-w.pow.done // the last one runs on in the background
	if works.Load() != 5 || signed != 4 {
		t.Fatalf("%d work requests for %d blocks, want 5 for 4 (one per block plus the one after the last)", works.Load(), signed)
	}
}

// the two calls inside a settle failing on their own: the send is rebroadcast and still lands exactly once
func TestRetriesFailedBroadcastAndConfirm(t *testing.T) {
	e := newEnv(t)
	e.take(1, 10)
	failed := map[string]int{}
	e.node.Intercept = func(action string) any {
		if (action == "process" || action == "block_info") && failed[action] < 2 {
			failed[action]++
			return map[string]string{"error": "Too many requests"}
		}
		return nil
	}
	if _, err := e.run(e.worker()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	e.paidExactly()
	if e.node.Blocks() != 1 || failed["process"] != 2 || failed["block_info"] != 2 {
		t.Fatalf("%d blocks, failures injected %v", e.node.Blocks(), failed)
	}
}

// the node disputing our chain, every time, is a disagreement the operator must resolve (D24, R6): halt, after
// enough rounds that a lagging node cannot cause it
func TestHaltsWhenNodeKeepsRejecting(t *testing.T) {
	for action, reply := range map[string]string{"process": "Gap previous block", "account_info": "Account not found"} {
		t.Run(action, func(t *testing.T) {
			e := newEnv(t)
			e.take(1, 10)
			e.node.Intercept = func(a string) any {
				if a == action {
					return map[string]string{"error": reply}
				}
				return nil
			}
			_, err := e.run(e.worker())
			wantHalt(t, err)
			if !errors.Is(err, nanoclient.ErrRejected) {
				t.Fatalf("halt does not say why: %v", err)
			}
			if e.node.Blocks() != 0 {
				t.Fatal("a block was accepted")
			}
		})
	}
}

// a few rejections from a node that is catching up are waited out
func TestRidesOutBriefRejections(t *testing.T) {
	e := newEnv(t)
	e.take(1, 10)
	rejected := 0
	e.node.Intercept = func(a string) any {
		if a == "process" && rejected < maxRejections-1 {
			rejected++
			return map[string]string{"error": "Gap previous block"}
		}
		return nil
	}
	if _, err := e.run(e.worker()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	e.paidExactly()
}

// shutdown during boot is a cancellation, not a halt that sends the operator to reconcile
func TestCancelledBootIsNotAHalt(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.worker().Run(ctx); !errors.Is(err, context.Canceled) || errors.Is(err, ErrHalt) {
		t.Fatalf("want a plain cancellation, got %v", err)
	}
}

// with polling effectively off, only Wake can start the payout, and it starts at once
func TestWakeStartsAtOnce(t *testing.T) {
	e := newEnv(t)
	w := e.worker()
	w.poll = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond) // let it boot and go idle
	e.take(1, 10)
	w.Wake()
	for deadline := time.Now().Add(5 * time.Second); e.node.Paid(addr(1)).String() != "10"; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("a woken worker did not pay")
		}
	}
}

// D7: work for the first send after a restart is fetched at boot, before anyone withdraws
func TestPrecachesWorkAtBoot(t *testing.T) {
	e := newEnv(t)
	var works atomic.Int32
	e.node.Intercept = func(action string) any {
		if action == "work_generate" {
			works.Add(1)
		}
		return nil
	}
	w := e.worker()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() { cancel(); <-done }()
	for deadline := time.Now().Add(5 * time.Second); works.Load() != 1; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d work requests at boot, want 1", works.Load())
		}
	}
	e.take(1, 3)
	w.Wake()
	for deadline := time.Now().Add(5 * time.Second); e.node.Paid(addr(1)).String() != "3"; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("not paid")
		}
	}
	// the boot one is used for the send, then exactly one precache follows the confirm
	for deadline := time.Now().Add(5 * time.Second); works.Load() != 2; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d work requests, want 2", works.Load())
		}
	}
	time.Sleep(50 * time.Millisecond)
	if n := works.Load(); n != 2 {
		t.Fatalf("%d work requests, want 2: the send fetched its own instead of using the boot one", n)
	}
}

// credit owes address i clicks without withdrawing, as a visitor who never cashes out
func (e *env) credit(i int, clicks uint64) {
	must(e.t, e.l.Credit(context.Background(), map[nanocrypto.PublicKey]uint64{addr(i): clicks}))
	if e.want[addr(i)] == nil {
		e.want[addr(i)] = new(big.Int)
	}
	e.want[addr(i)].Add(e.want[addr(i)], new(big.Int).SetUint64(clicks))
}

// runFor runs the worker until the node holds blocks sends, then stops it
func (e *env) runFor(w *Worker, blocks int) {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() { cancel(); <-done }()
	for deadline := time.Now().Add(10 * time.Second); e.node.Blocks() < blocks || !e.settled(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			e.t.Fatalf("%d of %d blocks", e.node.Blocks(), blocks)
		}
	}
}

// idle balances are paid out by themselves, exactly, and a person's withdrawal always goes first (D96)
func TestSweepsIdleBalances(t *testing.T) {
	e := newEnv(t)
	e.credit(1, 3)
	e.credit(2, 5)
	e.take(3, 7)                        // a person who asked
	time.Sleep(1100 * time.Millisecond) // last_active is in whole seconds; idle means strictly earlier
	w := e.worker()
	w.SweepIdleAfter(time.Nanosecond)
	w.sweep = 10 * time.Millisecond
	e.runFor(w, 3)
	e.paidExactly()
	if order := e.node.Order(); order[0] != addr(3) {
		t.Fatalf("a sweep went before the person who asked: %v", order)
	}
	if n, _ := e.l.Addresses(context.Background()); n != 0 {
		t.Fatalf("%d addresses still hold slots", n)
	}
}

// at most one sweep per interval, so idle balances can never become a burst of blocks
func TestSweepPace(t *testing.T) {
	e := newEnv(t)
	e.credit(1, 3)
	e.credit(2, 5)
	time.Sleep(1100 * time.Millisecond)
	w := e.worker()
	w.SweepIdleAfter(time.Nanosecond)
	w.sweep = time.Hour
	w.lastSweep = time.Now().Add(-2 * time.Hour) // the first is due at once
	e.runFor(w, 1)
	time.Sleep(200 * time.Millisecond)
	if e.node.Blocks() != 1 {
		t.Fatalf("%d sweeps inside one interval", e.node.Blocks())
	}
}

func TestSweepOff(t *testing.T) {
	e := newEnv(t)
	e.credit(1, 3)
	time.Sleep(1100 * time.Millisecond)
	w := e.worker()
	w.sweep = time.Millisecond // due constantly, but idleAfter is 0
	e.take(2, 1)
	e.runFor(w, 1)
	time.Sleep(200 * time.Millisecond)
	if e.node.Blocks() != 1 || e.node.Paid(addr(1)).Sign() != 0 {
		t.Fatal("swept with sweeping off")
	}
}

// D104: the work target follows the network's minimum up, never down, never past the ceiling, and only when two
// nodes agree; persisted blocks are still checked against the base, so a raise never halts a send already signed
func TestThresholdFloor(t *testing.T) {
	const ceiling = 0xfff0000000000000
	cases := []struct {
		name            string
		network, expect uint64
		ceiling         uint64
	}{
		{"network above base", 0xff80000000000000, 0xff80000000000000, ceiling},
		{"network past the ceiling", 0xfff8000000000000, ceiling, ceiling},
		{"network below base", 0xf000000000000000, threshold, ceiling},
		{"no ceiling set", 0xff80000000000000, threshold, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.node.NetworkMinimum = tc.network
			urls := []*url.URL{e.node.URL, e.node.URL}
			w := New(e.l, nanoclient.New(urls, urls, 5*time.Second), key, rep, threshold, slog.New(slog.NewTextHandler(io.Discard, nil)))
			w.poll = time.Millisecond
			if tc.ceiling != 0 {
				w.SetSendCeiling(tc.ceiling)
			}
			w.followFloor(context.Background())
			if w.target != tc.expect {
				t.Fatalf("target %016x, want %016x", w.target, tc.expect)
			}
			e.take(1, 3)
			must(t, w.crossCheck(context.Background()))
			for {
				if err := w.step(context.Background()); errors.Is(err, errIdle) {
					break
				} else {
					must(t, err)
				}
			}
			e.paidExactly()
		})
	}

	// one node alone cannot raise it
	e := newEnv(t)
	e.node.NetworkMinimum = 0xff80000000000000
	w := e.worker()
	w.SetSendCeiling(ceiling)
	w.followFloor(context.Background())
	if w.target != threshold {
		t.Fatalf("a single node raised the target to %016x", w.target)
	}
}

// work fetched at an old target is not reused once the target rises
func TestRaisedTargetRefetchesWork(t *testing.T) {
	e := newEnv(t)
	w := e.worker()
	w.prefetch(context.Background(), genesis)
	<-w.pow.done
	old := w.pow
	w.target = 0xff80000000000000
	v, err := w.workFor(context.Background(), genesis)
	must(t, err)
	if w.pow == old || nanocrypto.Difficulty(genesis, v) < w.target {
		t.Fatal("work below the raised target was reused")
	}
}

// D104: a payout that confirms promptly costs one confirmation check, made after a short wait
func TestConfirmationChecksBackOff(t *testing.T) {
	e := newEnv(t)
	e.take(1, 3)
	var checks atomic.Int32
	e.node.Intercept = func(action string) any {
		if action == "block_info" {
			checks.Add(1)
		}
		return nil
	}
	w := e.worker()
	must(t, w.crossCheck(context.Background()))
	for {
		if err := w.step(context.Background()); errors.Is(err, errIdle) {
			break
		} else {
			must(t, err)
		}
	}
	e.paidExactly()
	if n := checks.Load(); n != 1 {
		t.Fatalf("%d confirmation checks for one prompt payout, want 1", n)
	}
}

// D104: the wait quoted for a place in line comes from how long sends have actually taken
func TestETA(t *testing.T) {
	e := newEnv(t)
	w := e.worker()
	if w.ETA(3) != 3*assumedSend {
		t.Fatalf("before any send: %s", w.ETA(3))
	}
	w.timed(2 * time.Second)
	if w.ETA(1) != 2*time.Second {
		t.Fatalf("after one: %s", w.ETA(1))
	}
	w.timed(10 * time.Second) // an eighth of the way towards it
	if w.ETA(1) != 3*time.Second {
		t.Fatalf("moving average: %s", w.ETA(1))
	}
	e.take(1, 3)
	w2 := e.worker()
	must(t, w2.crossCheck(context.Background()))
	for {
		if err := w2.step(context.Background()); errors.Is(err, errIdle) {
			break
		} else {
			must(t, err)
		}
	}
	if avg := time.Duration(w2.avgSend.Load()); avg <= 0 || avg > 5*time.Second {
		t.Fatalf("a real send was timed at %s", avg)
	}
}
