package ledger

import (
	"errors"
	"flag"
	"math/big"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

var simLong = flag.Bool("sim.long", false, "run the ledger simulator as a long soak")

// TestSimulator drives concurrent bots (clicks and withdrawals) and a payout worker against one ledger, auditing
// while it runs and checking exact totals at the end (README §6.1, D59). Bot sequences are seeded, so a failure
// can be replayed; the interleaving is real concurrency.
func TestSimulator(t *testing.T) {
	ops := 100
	if *simLong {
		ops = 30000
	}
	for _, tc := range []struct {
		name     string
		vault    int64
		queueMax int
	}{
		{"roomy vault", 1_000_000_000, 1000},
		{"vault runs dry", 20_000, 1000}, // credits must stop exactly at the vault, never past it
		{"queue saturated", 1_000_000_000, 3},
	} {
		t.Run(tc.name, func(t *testing.T) { simulate(t, tc.vault, tc.queueMax, 8, 30, ops) })
	}
}

func simulate(t *testing.T, vaultSize int64, queueMax, bots, addresses, ops int) {
	l, err := Open(filepath.Join(t.TempDir(), "ledger.db"), queueMax)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.SetHead(ctx, genesis, big.NewInt(vaultSize)); err != nil {
		t.Fatal(err)
	}
	pool := make([]nanocrypto.PublicKey, addresses)
	for i := range pool {
		pool[i] = key(i)
	}

	var credited, paid atomic.Int64
	var botsWG, workerWG sync.WaitGroup
	stop := make(chan struct{})

	workerWG.Add(2)
	go func() { // the payout worker: one send at a time, signed then confirmed
		defer workerWG.Done()
		for n := 1; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			if !payOne(t, l, n, &paid) {
				time.Sleep(time.Millisecond)
			}
		}
	}()
	go func() { // auditor: the invariants must hold at every commit, not just at the end
		defer workerWG.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
				if err := l.Audit(ctx); err != nil {
					t.Error(err)
					return
				}
				var queued int
				if err := l.db.QueryRow(`SELECT count(*) FROM withdrawals WHERE state = 'queued'`).Scan(&queued); err != nil || queued > queueMax {
					t.Errorf("queue holds %d, cap %d (%v)", queued, queueMax, err)
					return
				}
			}
		}
	}()

	for bot := range bots {
		botsWG.Add(1)
		go func() {
			defer botsWG.Done()
			rng := rand.New(rand.NewPCG(uint64(bot), 0xd7a1))
			for range ops {
				if rng.IntN(4) == 0 {
					_, _, err := l.Withdraw(ctx, pool[rng.IntN(len(pool))])
					if err != nil && !errors.Is(err, ErrNothingToWithdraw) && !errors.Is(err, ErrQueueFull) {
						t.Error(err)
					}
					continue
				}
				batch := map[nanocrypto.PublicKey]uint64{}
				for range 1 + rng.IntN(4) {
					batch[pool[rng.IntN(len(pool))]] += uint64(1 + rng.IntN(500))
				}
				switch err := l.Credit(ctx, batch); {
				case err == nil:
					for _, n := range batch {
						credited.Add(int64(n))
					}
				case !errors.Is(err, ErrVaultExhausted):
					t.Error(err)
				}
			}
		}()
	}
	botsWG.Wait()

	// drain: every address cashes out, the worker pays everything
	deadline := time.Now().Add(30 * time.Second)
	for _, pub := range pool {
		for {
			_, _, err := l.Withdraw(ctx, pub)
			if err == nil || errors.Is(err, ErrNothingToWithdraw) {
				break
			}
			if !errors.Is(err, ErrQueueFull) || time.Now().After(deadline) {
				t.Fatal(err)
			}
			time.Sleep(time.Millisecond)
		}
	}
	for {
		if _, err := l.Next(ctx); errors.Is(err, ErrQueueEmpty) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("queue did not drain")
		}
		time.Sleep(time.Millisecond)
	}
	close(stop)
	workerWG.Wait()

	if err := l.Audit(ctx); err != nil {
		t.Fatal(err)
	}
	v := readVaultForTest(t, l)
	if v.credited.Int64() != credited.Load() {
		t.Errorf("ledger credited %s, bots credited %d", v.credited, credited.Load())
	}
	if paid.Load() != credited.Load() {
		t.Errorf("paid %d, credited %d", paid.Load(), credited.Load())
	}
	if want := vaultSize - paid.Load(); v.chain.Int64() != want {
		t.Errorf("chain balance %s, want %d", v.chain, want)
	}
	if credited.Load() > vaultSize {
		t.Errorf("credited %d exceeds the vault %d", credited.Load(), vaultSize)
	}
	t.Logf("credited %d raw, paid %d raw, vault %d -> %s", credited.Load(), paid.Load(), vaultSize, v.chain)
}

// payOne signs and confirms the oldest queued withdrawal; a merge between Next and MarkSigned is retried.
func payOne(t *testing.T, l *Ledger, n int, paid *atomic.Int64) bool {
	w, err := l.Next(ctx)
	if errors.Is(err, ErrQueueEmpty) {
		return false
	}
	if err != nil {
		t.Error(err)
		return false
	}
	var h nanocrypto.Hash
	h[0], h[1], h[2] = byte(n), byte(n>>8), byte(n>>16)
	switch err := l.MarkSigned(ctx, w, h, []byte(`{}`)); {
	case errors.Is(err, ErrChanged):
		return true
	case err != nil:
		t.Error(err)
		return false
	}
	if err := l.MarkConfirmed(ctx, w.ID); err != nil {
		t.Error(err)
		return false
	}
	paid.Add(w.Amount.Int64())
	return true
}

func readVaultForTest(t *testing.T, l *Ledger) vault {
	t.Helper()
	tx, err := l.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	v, err := readVault(tx)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
