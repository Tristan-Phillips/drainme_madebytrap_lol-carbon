package ledger

import (
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

// a ledger with every kind of row: owed balances, a confirmed payout, a recredited one, one queued, one in flight
func busyLedger(t *testing.T) (*Ledger, string) {
	t.Helper()
	l, path := openTemp(t)
	must(t, l.SetHead(ctx, genesis, big.NewInt(1000)))
	a, b, c, d := key(1), key(2), key(3), key(4)
	must(t, l.Credit(ctx, map[nanocrypto.PublicKey]uint64{a: 10, b: 20, c: 30, d: 40}))
	sign := func(h byte) Withdrawal {
		w, err := l.Next(ctx)
		must(t, err)
		must(t, l.MarkSigned(ctx, w, nanocrypto.Hash{h}, []byte(`{}`)))
		return w
	}
	withdraw(t, l, a, "10", 1)
	must(t, l.MarkConfirmed(ctx, sign(0xA1).ID))
	withdraw(t, l, b, "20", 1)
	must(t, l.Recredit(ctx, sign(0xB2).ID))
	withdraw(t, l, c, "30", 1)
	sign(0xC3)
	withdraw(t, l, d, "40", 2)
	check(t, l)
	return l, path
}

func TestReport(t *testing.T) {
	_, path := busyLedger(t)
	r0, err := OpenReader(path)
	must(t, err)
	defer r0.Close()
	r, err := r0.Report(ctx)
	must(t, err)
	got := map[string]string{
		"chain": r.Chain.String(), "available": r.Available.String(), "credited": r.Credited.String(),
		"unpaid": r.Unpaid.String(), "queued": r.Queued.String(), "paid": r.Paid.String(),
	}
	want := map[string]string{"chain": "990", "available": "900", "credited": "100", "unpaid": "20", "queued": "40", "paid": "10"}
	for k := range want {
		if got[k] != want[k] {
			t.Errorf("%s = %s, want %s", k, got[k], want[k])
		}
	}
	if r.Addresses != 1 || r.QueuedCount != 1 || r.PaidCount != 1 || r.Recredited != 1 || r.Books != nil {
		t.Errorf("counts %d %d %d %d, books %v", r.Addresses, r.QueuedCount, r.PaidCount, r.Recredited, r.Books)
	}
	if r.InFlight == nil || r.InFlight.Address != key(3) || r.InFlight.Amount.String() != "30" || r.InFlight.Hash != (nanocrypto.Hash{0xC3}) {
		t.Errorf("in flight %+v", r.InFlight)
	}
	if r.Frontier == nil || *r.Frontier != (nanocrypto.Hash{0xA1}) {
		t.Errorf("frontier %v", r.Frontier)
	}
	rec, err := r0.Recredited(ctx)
	must(t, err)
	if len(rec) != 1 || rec[0] != (nanocrypto.Hash{0xB2}) {
		t.Errorf("recredited %v", rec)
	}
}

func TestReportSeesBrokenBooks(t *testing.T) {
	l, path := busyLedger(t)
	_, err := l.db.Exec(`UPDATE vault SET available = '901'`)
	must(t, err)
	r0, err := OpenReader(path)
	must(t, err)
	defer r0.Close()
	r, err := r0.Report(ctx)
	must(t, err) // a report still comes back: broken books are what the operator most needs to see
	if !errors.Is(r.Books, ErrInvariant) {
		t.Fatalf("books %v", r.Books)
	}
}

// the reader is for running beside serve: it does not create, migrate, write, or wait for the writer
func TestReaderIsReadOnly(t *testing.T) {
	if _, err := OpenReader(filepath.Join(t.TempDir(), "missing.db")); err == nil {
		t.Fatal("a missing ledger was opened (and so created)")
	}
	l, path := busyLedger(t)
	r, err := OpenReader(path)
	must(t, err)
	defer r.Close()
	if err := r.Credit(ctx, map[nanocrypto.PublicKey]uint64{key(9): 1}); err == nil || !strings.Contains(err.Error(), "readonly") {
		t.Fatalf("a write through the reader: %v", err)
	}
	// the writer holds its lock while the reader reports
	tx, err := l.db.BeginTx(ctx, nil)
	must(t, err)
	defer tx.Rollback()
	_, err = tx.Exec(`UPDATE vault SET addresses = addresses WHERE id = 1`)
	must(t, err)
	done := make(chan error, 1)
	go func() { _, err := r.Report(ctx); done <- err }()
	select {
	case err := <-done:
		must(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Report waited for the writer")
	}

	old := filepath.Join(t.TempDir(), "old.db")
	must(t, os.WriteFile(old, nil, 0o600)) // an empty file is schema version 0
	if _, err := OpenReader(old); err == nil {
		t.Fatal("an unmigrated ledger was opened")
	}
}

func TestBackup(t *testing.T) {
	l, path := busyLedger(t)
	r, err := OpenReader(path)
	must(t, err)
	defer r.Close()
	dest := filepath.Join(t.TempDir(), "copy.db")
	must(t, r.Backup(ctx, dest))
	if err := r.Backup(ctx, dest); err == nil {
		t.Fatal("a backup overwrote an existing file")
	}
	credit(t, l, key(9), 5) // the live ledger moves on; the copy does not

	c, err := Open(dest, testQueueMax)
	must(t, err)
	defer c.Close()
	check(t, c)
	if b, _ := c.Balance(ctx, key(9)); b.Sign() != 0 {
		t.Fatal("the backup changed after it was taken")
	}
	s, err := c.InFlight(ctx)
	if err != nil || s.Hash != (nanocrypto.Hash{0xC3}) {
		t.Fatalf("backup lost the send in flight: %v %v", s, err)
	}
}

func TestLeaderboard(t *testing.T) {
	l, _ := openTemp(t)
	must(t, l.SetHead(ctx, genesis, big.NewInt(1000)))
	pay := func(i int, n uint64, h byte) {
		credit(t, l, key(i), n)
		if _, _, err := l.Withdraw(ctx, key(i)); err != nil {
			t.Fatal(err)
		}
		w, err := l.Next(ctx)
		must(t, err)
		must(t, l.MarkSigned(ctx, w, nanocrypto.Hash{h}, []byte(`{}`)))
		must(t, l.MarkConfirmed(ctx, w.ID))
	}
	pay(1, 5, 1)
	pay(2, 9, 2)
	pay(1, 7, 3) // 12 in all
	pay(3, 1, 4)
	// queued and recredited sends are not paid
	credit(t, l, key(4), 50)
	if _, _, err := l.Withdraw(ctx, key(4)); err != nil {
		t.Fatal(err)
	}
	top, err := l.TopPaid(ctx, 2)
	must(t, err)
	if len(top) != 2 || top[0].Address != key(1) || top[0].Paid.String() != "12" || top[1].Address != key(2) || top[1].Paid.String() != "9" {
		t.Fatalf("top %+v", top)
	}
	if p, err := l.PaidTo(ctx, key(1)); err != nil || p.String() != "12" {
		t.Fatalf("paid to 1: %v %v", p, err)
	}
	if p, err := l.PaidTo(ctx, key(4)); err != nil || p.Sign() != 0 {
		t.Fatalf("queued counted as paid: %v %v", p, err)
	}
	// the leaderboard's queries use the index, not a table scan
	var plan string
	rows, err := l.read.Query(`EXPLAIN QUERY PLAN SELECT amount FROM withdrawals WHERE address = ? AND state = 'confirmed'`, key(1).Address())
	must(t, err)
	for rows.Next() {
		var a, b, c int
		var d string
		must(t, rows.Scan(&a, &b, &c, &d))
		plan += d
	}
	rows.Close()
	if !strings.Contains(plan, "withdrawals_paid_by_address") {
		t.Fatalf("plan: %s", plan)
	}
}
