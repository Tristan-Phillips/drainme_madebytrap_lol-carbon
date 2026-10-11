package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

var ctx = context.Background()

func openTemp(t *testing.T) (*Ledger, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledger.db")
	l, err := Open(path, testQueueMax)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l, path
}

const testQueueMax = 1000

// genesis stands in for the hot wallet's frontier at init-vault
var genesis = nanocrypto.Hash{0xEE, 0x01}

func key(i int) nanocrypto.PublicKey {
	return nanocrypto.DeriveKey([32]byte{byte(i), 0xd7}, 0).Public()
}

func num(s string) *big.Int {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic(s)
	}
	return n
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func credit(t *testing.T, l *Ledger, pub nanocrypto.PublicKey, n uint64) {
	t.Helper()
	must(t, l.Credit(ctx, map[nanocrypto.PublicKey]uint64{pub: n}))
}

func withdraw(t *testing.T, l *Ledger, pub nanocrypto.PublicKey, wantAmount string, wantPos int64) {
	t.Helper()
	amt, pos, err := l.Withdraw(ctx, pub)
	must(t, err)
	if amt.String() != wantAmount || pos != wantPos {
		t.Fatalf("withdraw: got %s at %d, want %s at %d", amt, pos, wantAmount, wantPos)
	}
}

func check(t *testing.T, l *Ledger) {
	t.Helper()
	must(t, l.Audit(ctx))
}

func mustBalance(t *testing.T, l *Ledger, pub nanocrypto.PublicKey, want string) {
	t.Helper()
	got, err := l.Balance(ctx, pub)
	must(t, err)
	if got.String() != want {
		t.Fatalf("balance %s, want %s", got, want)
	}
}

func wantErr(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("want %v, got %v", target, err)
	}
}

func TestFreshVaultPaysNothing(t *testing.T) {
	l, _ := openTemp(t)
	wantErr(t, l.Credit(ctx, map[nanocrypto.PublicKey]uint64{key(1): 1}), ErrVaultExhausted)
	check(t, l)
}

func TestFullFlow(t *testing.T) {
	l, _ := openTemp(t)
	a, b := key(1), key(2)
	must(t, l.SetHead(ctx, genesis, num("1000000000000000000000000000000")))
	must(t, l.Credit(ctx, map[nanocrypto.PublicKey]uint64{a: 5, b: 3, key(9): 0}))
	mustBalance(t, l, a, "5")
	mustBalance(t, l, key(9), "0")
	check(t, l)

	withdraw(t, l, a, "5", 1)
	mustBalance(t, l, a, "0")
	credit(t, l, a, 2)
	withdraw(t, l, a, "7", 1) // merges into a's queued row
	withdraw(t, l, b, "3", 2)
	check(t, l)

	w, err := l.Next(ctx)
	must(t, err)
	if w.Address != a || w.Amount.String() != "7" {
		t.Fatalf("next: %+v", w)
	}
	must(t, l.MarkSigned(ctx, w, nanocrypto.Hash{1}, []byte(`{"block":1}`)))

	credit(t, l, a, 1)
	withdraw(t, l, a, "1", 3) // a's send is in flight, so a fresh queued row starts behind b

	next, err := l.Next(ctx)
	must(t, err)
	if next.Address != b {
		t.Fatalf("FIFO broken: next is %v", next.Address)
	}
	wantErr(t, l.MarkSigned(ctx, next, nanocrypto.Hash{2}, []byte(`{}`)), ErrInFlight)
	mustHead(t, l, genesis, "1000000000000000000000000000000")
	must(t, l.MarkConfirmed(ctx, w.ID))
	mustHead(t, l, nanocrypto.Hash{1}, "999999999999999999999999999993") // the confirmed send is the new frontier
	wantErr(t, l.MarkConfirmed(ctx, w.ID), ErrNotInFlight)
	must(t, l.MarkSigned(ctx, next, nanocrypto.Hash{2}, []byte(`{}`)))
	check(t, l)
}

func mustAddresses(t *testing.T, l *Ledger, want int64) {
	t.Helper()
	n, err := l.Addresses(ctx)
	must(t, err)
	if n != want {
		t.Fatalf("%d owed addresses, want %d", n, want)
	}
}

// the database keeps the count itself: new rows add one, upserts of an existing row add nothing, withdrawals and
// recredits move it both ways
func TestAddressCount(t *testing.T) {
	l, _ := openTemp(t)
	must(t, l.SetHead(ctx, genesis, big.NewInt(100)))
	mustAddresses(t, l, 0)
	must(t, l.Credit(ctx, map[nanocrypto.PublicKey]uint64{key(1): 1, key(2): 1, key(3): 0}))
	credit(t, l, key(1), 4)
	mustAddresses(t, l, 2)
	withdraw(t, l, key(1), "5", 1)
	mustAddresses(t, l, 1)
	w, err := l.Next(ctx)
	must(t, err)
	must(t, l.MarkSigned(ctx, w, nanocrypto.Hash{3}, []byte(`{}`)))
	must(t, l.Recredit(ctx, w.ID))
	mustAddresses(t, l, 2)
	check(t, l)
}

func mustHead(t *testing.T, l *Ledger, frontier nanocrypto.Hash, bal string) {
	t.Helper()
	h, b, err := l.Head(ctx)
	must(t, err)
	if h != frontier || b.String() != bal {
		t.Fatalf("head %X %s, want %X %s", h, b, frontier, bal)
	}
}

func TestHeadNeedsInitVault(t *testing.T) {
	l, _ := openTemp(t)
	_, _, err := l.Head(ctx)
	wantErr(t, err, ErrNoHead)
	wantErr(t, l.SetHead(ctx, nanocrypto.Hash{}, big.NewInt(5)), ErrZeroFrontier)
	_, _, err = l.Head(ctx)
	wantErr(t, err, ErrNoHead)
	must(t, l.SetHead(ctx, genesis, big.NewInt(0)))
	mustHead(t, l, genesis, "0")
}

// Head runs on every sign and every rebroadcast, so it must not queue behind the click path's write lock
func TestHeadDoesNotTakeTheWriteLock(t *testing.T) {
	l, path := openTemp(t)
	must(t, l.SetHead(ctx, genesis, big.NewInt(5)))
	other, err := Open(path, testQueueMax)
	must(t, err)
	defer other.Close()
	writer, err := other.db.Begin() // _txlock=immediate: this holds the write lock until rolled back
	must(t, err)
	defer writer.Rollback()
	start := time.Now()
	mustHead(t, l, genesis, "5")
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Head waited %s for the write lock", d)
	}
}

func TestParseHashIsStrict(t *testing.T) {
	good := strings.Repeat("AB", 32)
	for _, s := range []string{"", strings.ToLower(good), good[:62], good + "AB", "G" + good[1:], " " + good[1:]} {
		if _, err := parseHash(s); !errors.Is(err, ErrInvariant) {
			t.Errorf("%q accepted", s)
		}
	}
	if h, err := parseHash(good); err != nil || h[0] != 0xAB || h[31] != 0xAB {
		t.Errorf("%q: %X %v", good, h, err)
	}
}

func TestMarkSignedRejectsStaleAmount(t *testing.T) {
	l, _ := openTemp(t)
	a := key(1)
	must(t, l.SetHead(ctx, genesis, big.NewInt(100)))
	credit(t, l, a, 5)
	withdraw(t, l, a, "5", 1)
	w, err := l.Next(ctx)
	must(t, err)
	// a merge lands after the worker read the row but before it persisted the block
	credit(t, l, a, 1)
	withdraw(t, l, a, "6", 1)
	wantErr(t, l.MarkSigned(ctx, w, nanocrypto.Hash{1}, []byte(`{}`)), ErrChanged)
	w, err = l.Next(ctx)
	must(t, err)
	if w.Amount.String() != "6" {
		t.Fatalf("re-read amount %s", w.Amount)
	}
	must(t, l.MarkSigned(ctx, w, nanocrypto.Hash{1}, []byte(`{}`)))
	check(t, l)
}

// a restarted worker gets back exactly the block it persisted, and settles it before anything else moves
func TestInFlightSettling(t *testing.T) {
	l, _ := openTemp(t)
	a := key(1)
	must(t, l.SetHead(ctx, genesis, big.NewInt(10)))
	credit(t, l, a, 8)
	withdraw(t, l, a, "8", 1)
	w, err := l.Next(ctx)
	must(t, err)
	block := []byte(`{"type":"state","signature":"AB"}`)
	must(t, l.MarkSigned(ctx, w, nanocrypto.Hash{0xAB, 0xCD}, block))

	s, err := l.InFlight(ctx)
	must(t, err)
	if s.ID != w.ID || s.Address != a || s.Amount.String() != "8" || s.Hash != (nanocrypto.Hash{0xAB, 0xCD}) || string(s.Block) != string(block) {
		t.Fatalf("InFlight returned %+v", s)
	}
	// the chain may or may not already show this send, so its balance cannot be trusted until it settles
	wantErr(t, l.SetHead(ctx, genesis, big.NewInt(2)), ErrInFlight)

	must(t, l.MarkConfirmed(ctx, w.ID))
	_, err = l.InFlight(ctx)
	wantErr(t, err, ErrNotInFlight)
	must(t, l.SetHead(ctx, genesis, big.NewInt(2))) // the chain after the send: no double count
	check(t, l)
}

func TestRecredit(t *testing.T) {
	l, _ := openTemp(t)
	a := key(1)
	must(t, l.SetHead(ctx, genesis, big.NewInt(10)))
	credit(t, l, a, 8)
	withdraw(t, l, a, "8", 1)
	w, err := l.Next(ctx)
	must(t, err)
	wantErr(t, l.Recredit(ctx, w.ID), ErrNotInFlight) // still queued, nothing to recover
	must(t, l.MarkSigned(ctx, w, nanocrypto.Hash{1}, []byte(`{}`)))
	credit(t, l, a, 1)
	must(t, l.Recredit(ctx, w.ID))
	mustBalance(t, l, a, "9")
	wantErr(t, l.Recredit(ctx, w.ID), ErrNotInFlight)
	wantErr(t, l.MarkConfirmed(ctx, w.ID), ErrNotInFlight)
	check(t, l)
}

// F1: a full queue refuses new addresses without debiting them, still lets a queued address merge, and frees a
// slot once the head is signed
func TestQueueCap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	l, err := Open(path, 2)
	must(t, err)
	defer l.Close()
	must(t, l.SetHead(ctx, genesis, big.NewInt(100)))
	for i := 1; i <= 3; i++ {
		credit(t, l, key(i), 5)
	}
	withdraw(t, l, key(1), "5", 1)
	withdraw(t, l, key(2), "5", 2)
	_, _, err = l.Withdraw(ctx, key(3))
	wantErr(t, err, ErrQueueFull)
	mustBalance(t, l, key(3), "5") // the refused withdrawal debited nothing

	credit(t, l, key(1), 1)
	withdraw(t, l, key(1), "6", 1) // merging needs no new slot

	w, err := l.Next(ctx)
	must(t, err)
	must(t, l.MarkSigned(ctx, w, nanocrypto.Hash{1}, []byte(`{}`)))
	withdraw(t, l, key(3), "5", 3) // the signed head no longer counts against the cap
	check(t, l)

	if _, err := Open(filepath.Join(t.TempDir(), "x.db"), 0); err == nil {
		t.Fatal("zero queue cap accepted")
	}
}

func TestEmptyCases(t *testing.T) {
	l, _ := openTemp(t)
	_, _, err := l.Withdraw(ctx, key(1))
	wantErr(t, err, ErrNothingToWithdraw)
	_, err = l.Next(ctx)
	wantErr(t, err, ErrQueueEmpty)
	_, err = l.InFlight(ctx)
	wantErr(t, err, ErrNotInFlight)
	wantErr(t, l.MarkConfirmed(ctx, 42), ErrNotInFlight)
	wantErr(t, l.MarkSigned(ctx, Withdrawal{ID: 42, Amount: big.NewInt(1)}, nanocrypto.Hash{}, []byte(`{}`)), ErrChanged)
}

func TestCreditIsAllOrNothing(t *testing.T) {
	l, _ := openTemp(t)
	must(t, l.SetHead(ctx, genesis, big.NewInt(10)))
	wantErr(t, l.Credit(ctx, map[nanocrypto.PublicKey]uint64{key(1): 6, key(2): 6}), ErrVaultExhausted)
	mustBalance(t, l, key(1), "0")
	mustBalance(t, l, key(2), "0")
	check(t, l)
	credit(t, l, key(1), 10) // exactly the remaining amount is creditable
	check(t, l)
}

func TestSetHeadOvercommitted(t *testing.T) {
	l, _ := openTemp(t)
	must(t, l.SetHead(ctx, genesis, big.NewInt(10)))
	credit(t, l, key(1), 8)
	wantErr(t, l.SetHead(ctx, genesis, big.NewInt(7)), ErrOvercommitted)
	must(t, l.SetHead(ctx, genesis, big.NewInt(8)))
	check(t, l)
}

// README §6.4: values around 2^64, 2^127 and 10^30 must survive exactly
func TestLargeValues(t *testing.T) {
	pow2 := func(n uint) *big.Int { return new(big.Int).Lsh(big.NewInt(1), n) }
	max128 := new(big.Int).Sub(pow2(128), big.NewInt(1))

	t.Run("past 2^64 through Credit", func(t *testing.T) {
		l, _ := openTemp(t)
		must(t, l.SetHead(ctx, genesis, max128))
		a := key(1)
		for range 3 {
			credit(t, l, a, ^uint64(0))
		}
		want := new(big.Int).Mul(new(big.Int).SetUint64(^uint64(0)), big.NewInt(3))
		withdraw(t, l, a, want.String(), 1)
		check(t, l)
		wantErr(t, l.SetHead(ctx, genesis, pow2(128)), ErrInvariant)
	})

	// Credit takes uint64 counts, so a balance near 2^127 is seeded directly, keeping the books consistent,
	// and then driven through Withdraw, MarkSigned and MarkConfirmed
	for _, amt := range []*big.Int{
		new(big.Int).Sub(pow2(127), big.NewInt(1)),
		pow2(127),
		new(big.Int).Add(pow2(127), big.NewInt(1)),
		num("1000000000000000000000000000000"),
	} {
		t.Run("payout of "+amt.String(), func(t *testing.T) {
			l, _ := openTemp(t)
			a := key(1)
			chain := new(big.Int).Add(amt, big.NewInt(5))
			must(t, l.SetHead(ctx, genesis, chain))
			_, err := l.db.Exec(`UPDATE vault SET available = '5', credited = ?`, amt.String())
			must(t, err)
			_, err = l.db.Exec(`INSERT INTO balances (address, unpaid) VALUES (?, ?)`, a.Address(), amt.String())
			must(t, err)
			check(t, l)

			withdraw(t, l, a, amt.String(), 1)
			w, err := l.Next(ctx)
			must(t, err)
			must(t, l.MarkSigned(ctx, w, nanocrypto.Hash{7}, []byte(`{}`)))
			must(t, l.MarkConfirmed(ctx, w.ID))
			check(t, l)
			v := readVaultForTest(t, l)
			if v.chain.String() != "5" {
				t.Fatalf("chain after payout %s, want 5", v.chain)
			}
		})
	}

	t.Run("10^30 vault", func(t *testing.T) {
		l, _ := openTemp(t)
		must(t, l.SetHead(ctx, genesis, num("1000000000000000000000000000000")))
		credit(t, l, key(1), 1)
		v := readVaultForTest(t, l)
		if v.available.String() != "999999999999999999999999999999" {
			t.Fatalf("available %s", v.available)
		}
		check(t, l)
	})
}

func TestReopenKeepsState(t *testing.T) {
	l, path := openTemp(t)
	must(t, l.SetHead(ctx, genesis, big.NewInt(50)))
	credit(t, l, key(1), 7)
	must(t, l.Close())

	l2, err := Open(path, testQueueMax)
	must(t, err)
	mustBalance(t, l2, key(1), "7")
	check(t, l2)
	// 1 is the step-1 layout without vault.frontier: refused by name, never half-opened
	for _, v := range []int{1, 99} {
		_, err = l2.db.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, v))
		must(t, err)
		if l3, err := Open(path, testQueueMax); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("unknown schema version %d", v)) {
			if l3 != nil {
				l3.Close()
			}
			t.Fatalf("schema version %d: %v", v, err)
		}
	}
	must(t, l2.Close())
}

// the attack that matters (README §6.2): many concurrent withdrawals for one address pay it exactly once
func TestConcurrentWithdrawSameAddress(t *testing.T) {
	l, _ := openTemp(t)
	a := key(1)
	must(t, l.SetHead(ctx, genesis, big.NewInt(1_000_000)))
	credit(t, l, a, 1000)

	var wg sync.WaitGroup
	var mu sync.Mutex
	total := new(big.Int)
	wins := 0
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			amt, _, err := l.Withdraw(ctx, a)
			if errors.Is(err, ErrNothingToWithdraw) {
				return
			}
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			total.Add(total, amt)
			wins++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if wins != 1 || total.String() != "1000" {
		t.Fatalf("%d withdrawals succeeded totalling %s, want 1 of 1000", wins, total)
	}
	check(t, l)
}

// The schema refuses these on its own, with no Go code in the way. Each case runs against a fresh fixture that
// holds only rows reached through legal transitions, so a case can only pass because of the rule it names.
func TestSchemaBackstops(t *testing.T) {
	maxAmt, overAmt := maxAmount.String(), new(big.Int).Add(maxAmount, big.NewInt(1)).String()
	a, b := key(1).Address(), key(2).Address()
	hexHash := func(c string) string { return strings.Repeat(c, 64) }
	insert := func(addr, amt string) string {
		return `INSERT INTO withdrawals (address, amount, state, created) VALUES ('` + addr + `', '` + amt + `', 'queued', 'x')`
	}
	insertAs := func(state string) string {
		return `INSERT INTO withdrawals (address, amount, state, block_hash, block, created) VALUES ('` + b + `', '1', '` + state + `', '` + hexHash("C") + `', '{}', 'x')`
	}
	// base: a queued (id 1); b has one confirmed send (id 2). inFlight adds b's signed send (id 3).
	base := []string{
		`UPDATE vault SET frontier = '` + hexHash("F") + `'`,
		`INSERT INTO balances (address, unpaid) VALUES ('` + a + `', '5')`,
		insert(a, "5"),
		insert(b, "2"),
		`UPDATE withdrawals SET state = 'signed', block_hash = '` + hexHash("B") + `', block = '{}' WHERE id = 2`,
		`UPDATE withdrawals SET state = 'confirmed' WHERE id = 2`,
	}
	inFlight := append(append([]string{}, base...),
		insert(b, "3"),
		`UPDATE withdrawals SET state = 'signed', block_hash = '`+hexHash("A")+`', block = '{}' WHERE id = 3`,
	)

	refused := []struct {
		name, setupName string
		setup           []string
		query           string
	}{
		{"negative balance", "base", base, `UPDATE balances SET unpaid = '-1'`},
		{"leading zero", "base", base, `UPDATE balances SET unpaid = '05'`},
		{"empty amount", "base", base, `UPDATE balances SET unpaid = ''`},
		{"non-digit amount", "base", base, `UPDATE balances SET unpaid = '1e9'`},
		{"amount 2^128", "base", base, `UPDATE balances SET unpaid = '` + overAmt + `'`},
		{"39 nines", "base", base, `UPDATE balances SET unpaid = '` + strings.Repeat("9", 39) + `'`},
		{"vault over 2^128-1", "base", base, `UPDATE vault SET chain_balance = '` + overAmt + `'`},
		{"negative vault", "base", base, `UPDATE vault SET available = '-5'`},
		{"balance without frontier", "base", base, `UPDATE vault SET chain_balance = '5', frontier = NULL`},
		{"lowercase frontier", "base", base, `UPDATE vault SET frontier = '` + hexHash("f") + `'`},
		{"short frontier", "base", base, `UPDATE vault SET frontier = 'ABCD'`},
		{"zero withdrawal", "base", base, insert(b, "0")},
		{"withdrawal 2^128", "base", base, insert(b, overAmt)},
		{"second queued for a", "base", base, insert(a, "1")},
		{"insert as signed", "base", base, insertAs("signed")},
		{"insert as confirmed", "base", base, insertAs("confirmed")},
		{"insert as recredited", "base", base, insertAs("recredited")},
		{"signed without block", "base", base, `UPDATE withdrawals SET state = 'signed' WHERE state = 'queued'`},
		{"block without hash", "base", base, `UPDATE withdrawals SET block = '{}' WHERE state = 'queued'`},
		{"unconfirm", "base", base, `UPDATE withdrawals SET state = 'signed' WHERE state = 'confirmed'`},
		{"skip signing", "base", base, `UPDATE withdrawals SET state = 'confirmed' WHERE state = 'queued'`},
		{"recredit confirmed", "base", base, `UPDATE withdrawals SET state = 'recredited' WHERE state = 'confirmed'`},
		{"unknown state", "base", base, `UPDATE withdrawals SET state = 'paid' WHERE state = 'queued'`},
		{"edit confirmed amount", "base", base, `UPDATE withdrawals SET amount = '999' WHERE state = 'confirmed'`},
		{"duplicate block hash", "base", base, `UPDATE withdrawals SET state = 'signed', block_hash = '` + hexHash("B") + `', block = '{}' WHERE state = 'queued'`},
		{"lowercase block hash", "base", base, `UPDATE withdrawals SET state = 'signed', block_hash = '` + hexHash("e") + `', block = '{}' WHERE state = 'queued'`},
		{"delete withdrawal", "base", base, `DELETE FROM withdrawals`},
		{"zero balance row", "base", base, `UPDATE balances SET unpaid = '0'`},
		{"negative last_active", "base", base, `UPDATE balances SET last_active = -1`},
		{"negative address count", "base", base, `UPDATE vault SET addresses = -1`},
		{"delete vault", "base", base, `DELETE FROM vault`},
		{"second vault row", "base", base, `INSERT INTO vault (id, chain_balance, available, credited) VALUES (2, '0', '0', '0')`},
		{"malformed address", "base", base, `INSERT INTO balances (address, unpaid) VALUES ('xrb_short', '1')`},
		{"second in flight", "inFlight", inFlight, `UPDATE withdrawals SET state = 'signed', block_hash = '` + hexHash("D") + `', block = '{}' WHERE state = 'queued'`},
		{"edit signed amount", "inFlight", inFlight, `UPDATE withdrawals SET amount = '999' WHERE state = 'signed'`},
		{"edit signed block", "inFlight", inFlight, `UPDATE withdrawals SET block = '{"evil":1}' WHERE state = 'signed'`},
		{"redirect signed send", "inFlight", inFlight, `UPDATE withdrawals SET address = '` + a + `' WHERE state = 'signed'`},
	}
	accepted := []struct{ name, query string }{
		{"max amount", `UPDATE balances SET unpaid = '` + maxAmt + `'`},
		// Withdraw deletes the row. Deleting an owed row is no more power than the zeroing it replaced, and Audit's
		// first equation catches either at the next boot (TestAuditDetectsCorruption).
		{"delete owed row", `DELETE FROM balances`},
		{"funded vault with frontier", `UPDATE vault SET chain_balance = '5', available = '5'`},
	}

	fixture := func(t *testing.T, setup []string) *Ledger {
		t.Helper()
		l, _ := openTemp(t)
		for _, q := range setup {
			if _, err := l.db.Exec(q); err != nil {
				t.Fatalf("setup %q: %v", q, err)
			}
		}
		return l
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			l := fixture(t, tc.setup)
			if _, err := l.db.Exec(tc.query); err == nil {
				t.Errorf("schema accepted %q on the %s fixture", tc.query, tc.setupName)
			}
		})
	}
	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			l := fixture(t, base)
			if _, err := l.db.Exec(tc.query); err != nil {
				t.Errorf("schema refused %q: %v", tc.query, err)
			}
		})
	}
}

// Audit must notice books that do not balance, for each of its two equations.
func TestAuditDetectsCorruption(t *testing.T) {
	for name, tamper := range map[string]string{
		"money appears":          `UPDATE vault SET available = '11'`,
		"unpaid inflated":        `UPDATE balances SET unpaid = '9'`,
		"credited understated":   `UPDATE vault SET credited = '1'`,
		"chain balance inflated": `UPDATE vault SET chain_balance = '1000'`,
		"owed row deleted":       `DELETE FROM balances`,
		"address count drifted":  `UPDATE vault SET addresses = 5`,
	} {
		t.Run(name, func(t *testing.T) {
			l, _ := openTemp(t)
			must(t, l.SetHead(ctx, genesis, big.NewInt(100)))
			credit(t, l, key(1), 4)
			check(t, l)
			_, err := l.db.Exec(tamper)
			must(t, err)
			wantErr(t, l.Audit(ctx), ErrInvariant)
		})
	}
}

func TestParseAmountIsStrict(t *testing.T) {
	over := new(big.Int).Lsh(big.NewInt(1), 128).String()
	for _, s := range []string{"", "-1", "01", "+1", "1e3", " 1", "0x10", "1.0", over} {
		if _, err := parseAmount(s); !errors.Is(err, ErrInvariant) {
			t.Errorf("%q accepted", s)
		}
	}
	for _, s := range []string{"0", "1", maxAmount.String()} {
		if _, err := parseAmount(s); err != nil {
			t.Errorf("%q rejected: %v", s, err)
		}
	}
}

func setActive(t *testing.T, l *Ledger, pub nanocrypto.PublicKey, unix int64) {
	t.Helper()
	if _, err := l.db.Exec(`UPDATE balances SET last_active = ? WHERE address = ?`, unix, pub.Address()); err != nil {
		t.Fatal(err)
	}
}

// D96: the longest-idle balance goes first, recent ones stay, and a sweep is an ordinary queued withdrawal
func TestSweepIdle(t *testing.T) {
	l, _ := openTemp(t)
	must(t, l.SetHead(ctx, genesis, big.NewInt(100)))
	credit(t, l, key(1), 3)
	credit(t, l, key(2), 5)
	credit(t, l, key(3), 7)
	setActive(t, l, key(1), 200)
	setActive(t, l, key(2), 100)
	// key(3) was credited just now

	_, _, err := l.SweepIdle(ctx, time.Unix(100, 0))
	wantErr(t, err, ErrNothingIdle) // idle means strictly before
	pub, amt, err := l.SweepIdle(ctx, time.Unix(250, 0))
	must(t, err)
	if pub != key(2) || amt.String() != "5" {
		t.Fatalf("swept %s %s, want the longest idle, key(2) with 5", pub.Address(), amt)
	}
	pub, amt, err = l.SweepIdle(ctx, time.Unix(250, 0))
	must(t, err)
	if pub != key(1) || amt.String() != "3" {
		t.Fatalf("swept %s %s, want key(1) with 3", pub.Address(), amt)
	}
	_, _, err = l.SweepIdle(ctx, time.Unix(250, 0))
	wantErr(t, err, ErrNothingIdle)
	mustBalance(t, l, key(3), "7")
	mustAddresses(t, l, 1)

	// a click resets the idle clock
	setActive(t, l, key(3), 100)
	credit(t, l, key(3), 1)
	_, _, err = l.SweepIdle(ctx, time.Unix(250, 0))
	wantErr(t, err, ErrNothingIdle)

	w, err := l.Next(ctx)
	must(t, err)
	if w.Address != key(2) || w.Amount.String() != "5" {
		t.Fatalf("queue head %+v", w)
	}
	check(t, l)
}

// the dust test ran on schema v2: opening it migrates in place, keeps every row, and its balances count as long idle
func TestMigrateFromV2(t *testing.T) { testMigrate(t, 2) }
func TestMigrateFromV3(t *testing.T) { testMigrate(t, 3) }

func testMigrate(t *testing.T, from int) {
	path := filepath.Join(t.TempDir(), "old.db")
	v2, err := os.ReadFile(fmt.Sprintf("testdata/schema_v%d.sql", from))
	must(t, err)
	db, err := sql.Open("sqlite", path)
	must(t, err)
	for _, q := range []string{string(v2),
		`UPDATE vault SET chain_balance = '100', available = '90', credited = '10', frontier = '` + strings.Repeat("A", 64) + `'`,
		`INSERT INTO balances (address, unpaid) VALUES ('` + key(1).Address() + `', '6')`,
		`INSERT INTO withdrawals (address, amount, state, created) VALUES ('` + key(2).Address() + `', '4', 'queued', 'x')`,
		fmt.Sprintf(`PRAGMA user_version = %d`, from),
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%.60s: %v", q, err)
		}
	}
	must(t, db.Close())

	l, err := Open(path, testQueueMax)
	must(t, err)
	defer l.Close()
	var v int
	must(t, l.db.QueryRow(`PRAGMA user_version`).Scan(&v))
	if v != schemaVersion {
		t.Fatalf("schema version %d after migrating", v)
	}
	check(t, l)
	mustBalance(t, l, key(1), "6")
	pub, amt, err := l.SweepIdle(ctx, time.Unix(1, 0))
	must(t, err)
	if pub != key(1) || amt.String() != "6" {
		t.Fatalf("migrated balance not idle: %s %s", pub.Address(), amt)
	}
	var idx int
	must(t, l.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name = 'withdrawals_paid_by_address'`).Scan(&idx))
	if idx != 1 {
		t.Fatal("the leaderboard index was not added")
	}
}

// plain reads go to their own pool, so a long write transaction (a big flush) never holds them up
func TestReadsDoNotWaitForTheWriter(t *testing.T) {
	l, _ := openTemp(t)
	must(t, l.SetHead(ctx, genesis, big.NewInt(100)))
	credit(t, l, key(1), 5)
	withdraw(t, l, key(1), "5", 1)
	credit(t, l, key(2), 3)

	inTx, release := make(chan struct{}), make(chan struct{})
	go l.tx(ctx, func(*sql.Tx) error { close(inTx); <-release; return nil })
	<-inTx
	defer close(release)
	reads := map[string]func() error{
		"Head":        func() error { _, _, err := l.Head(ctx); return err },
		"Totals":      func() error { _, _, _, err := l.Totals(ctx); return err },
		"Balance":     func() error { _, err := l.Balance(ctx, key(2)); return err },
		"Next":        func() error { _, err := l.Next(ctx); return err },
		"InFlight":    func() error { _, err := l.InFlight(ctx); wantErr(t, err, ErrNotInFlight); return nil },
		"QueueLength": func() error { _, err := l.QueueLength(ctx); return err },
		"Addresses":   func() error { _, err := l.Addresses(ctx); return err },
	}
	for name, read := range reads {
		done := make(chan error, 1)
		go func() { done <- read() }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Errorf("%s waited for the writer", name)
		}
	}
}

func TestReadPoolCannotWrite(t *testing.T) {
	l, _ := openTemp(t)
	if _, err := l.read.Exec(`UPDATE vault SET credited = '5'`); err == nil {
		t.Fatal("the read pool wrote to the ledger")
	}
}
