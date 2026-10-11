package main

import (
	"bytes"
	"context"
	"database/sql"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/config"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/fakenode"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanoclient"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

// opsEnv is an opened vault, its ledger, and two "nodes" that are the same fake node, so they always agree
type opsEnv struct {
	cfg  *config.Config
	node *fakenode.Node
	l    *ledger.Ledger
	eps  []endpoint
	key  nanocrypto.PrivateKey
	out  bytes.Buffer
}

func newOpsEnv(t *testing.T) *opsEnv {
	t.Helper()
	cfg, node := stack(t)
	l, err := ledger.Open(filepath.Join(cfg.DataDir, "ledger.db"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	seed, _ := config.ReadSeed(cfg.SeedFile)
	c := nanoclient.New(cfg.RPCURLs, nil, time.Second)
	return &opsEnv{cfg: cfg, node: node, l: l, eps: []endpoint{{"one", c}, {"two", c}}, key: nanocrypto.DeriveKey(seed, 0)}
}

var payee = nanocrypto.DeriveKey([32]byte{0xd7, 0x0e}, 0).Public()

// sign leaves a withdrawal of amount to payee signed and persisted, as the worker does just before broadcast;
// publish also hands it to the node
func (e *opsEnv) sign(t *testing.T, amount uint64, publish bool) ledger.Signed {
	t.Helper()
	ctx := context.Background()
	must(t, e.l.Credit(ctx, map[nanocrypto.PublicKey]uint64{payee: amount}))
	if _, _, err := e.l.Withdraw(ctx, payee); err != nil {
		t.Fatal(err)
	}
	w, err := e.l.Next(ctx)
	must(t, err)
	frontier, bal, err := e.l.Head(ctx)
	must(t, err)
	node := nanoclient.New(e.cfg.RPCURLs, e.cfg.RPCURLs, time.Second)
	work, err := node.Work(ctx, frontier, e.cfg.SendThreshold)
	must(t, err)
	rep, _ := nanocrypto.ParseAddress(e.cfg.Representative)
	b := nanoclient.Block{StateBlock: nanocrypto.StateBlock{Account: e.key.Public(), Previous: frontier, Representative: rep,
		Balance: new(big.Int).Sub(bal, w.Amount), Link: payee}, Work: work}
	h, _ := b.Hash()
	b.Signature = e.key.Sign(h)
	must(t, e.l.MarkSigned(ctx, w, h, b.Encode()))
	if publish {
		must(t, node.Process(ctx, h, b.Encode(), "send"))
	}
	s, err := e.l.InFlight(ctx)
	must(t, err)
	return s
}

func (e *opsEnv) recover(recredit bool) error {
	return recoverSend(context.Background(), e.l, e.eps, e.key.Public(), recredit, &e.out, quiet())
}

func (e *opsEnv) reconcile(adopt *nanocrypto.Hash) error {
	return reconcile(context.Background(), e.l, e.eps, e.key.Public(), adopt, &e.out, quiet())
}

func (e *opsEnv) said(t *testing.T, want string) {
	t.Helper()
	if !strings.Contains(e.out.String(), want) {
		t.Fatalf("output lacks %q:\n%s", want, e.out.String())
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (e *opsEnv) head(t *testing.T) (nanocrypto.Hash, string) {
	t.Helper()
	f, b, err := e.l.Head(context.Background())
	must(t, err)
	return f, b.String()
}

func TestRecoverNothingInFlight(t *testing.T) {
	e := newOpsEnv(t)
	must(t, e.recover(true))
	e.said(t, "nothing to recover")
}

// a send the network confirmed while the service was down is recorded as paid, from the nodes' word alone
func TestRecoverConfirmed(t *testing.T) {
	e := newOpsEnv(t)
	s := e.sign(t, 7, true)
	must(t, e.recover(false))
	e.said(t, "recorded as paid")
	if f, bal := e.head(t); f != s.Hash || bal != new(big.Int).Sub(vault, big.NewInt(7)).String() {
		t.Fatalf("head %s %s", f.Hex(), bal)
	}
	must(t, e.l.Audit(context.Background()))
}

// every case in which the block could still land changes nothing, recredit or not
func TestRecoverRefusesWhileTheBlockCouldLand(t *testing.T) {
	e := newOpsEnv(t)
	e.node.PendingPolls = 1000
	e.sign(t, 7, true)
	wantRefused(t, e.recover(true), "not confirmed it yet")

	e = newOpsEnv(t)
	e.sign(t, 7, false) // signed, never broadcast, and the chain has not moved
	wantRefused(t, e.recover(true), "could still accept it")
	if _, err := e.l.InFlight(context.Background()); err != nil {
		t.Fatal("a refusal changed the send in flight")
	}
	if b, _ := e.l.Balance(context.Background(), payee); b.Sign() != 0 {
		t.Fatal("a refusal recredited")
	}
}

// someone spent from the vault with the seed while our send was signed: ours can never confirm, so recover puts it
// back, when told to, and reconcile then catches the ledger up with the chain
func TestRecoverThenReconcile(t *testing.T) {
	e := newOpsEnv(t)
	e.sign(t, 7, false)
	theirs := nanocrypto.Hash{0xbe, 0xef}
	after := new(big.Int).Sub(vault, big.NewInt(1000))
	e.node.Foreign(theirs, after)

	must(t, e.recover(false))
	e.said(t, "can never confirm")
	e.said(t, "drainme recover --recredit")
	if _, err := e.l.InFlight(context.Background()); err != nil {
		t.Fatal("recover without --recredit changed something")
	}
	must(t, e.recover(true))
	if b, _ := e.l.Balance(context.Background(), payee); b.String() != "7" {
		t.Fatalf("recredited balance %s", b)
	}
	if f, _ := e.head(t); f != genesis {
		t.Fatal("recredit moved the head")
	}

	e.out.Reset()
	must(t, e.reconcile(nil))
	e.said(t, "-1000 raw")
	e.said(t, "reconcile --adopt "+theirs.Hex())
	if f, _ := e.head(t); f != genesis {
		t.Fatal("reconcile without --adopt changed the head")
	}
	wrong := nanocrypto.Hash{0x01}
	wantRefused(t, e.reconcile(&wrong), "not the")
	must(t, e.reconcile(&theirs))
	if f, bal := e.head(t); f != theirs || bal != after.String() {
		t.Fatalf("head after adopt %s %s", f.Hex(), bal)
	}
	must(t, e.l.Audit(context.Background()))
}

// a donation received in a wallet tops the vault up, once adopted
func TestReconcileTopUp(t *testing.T) {
	e := newOpsEnv(t)
	must(t, e.reconcile(nil))
	e.said(t, "agree")
	gift := nanocrypto.Hash{0x91, 0xf7}
	e.node.Foreign(gift, new(big.Int).Add(vault, big.NewInt(500)))
	must(t, e.reconcile(&gift))
	_, available, _, err := e.l.Totals(context.Background())
	must(t, err)
	if available.Cmp(new(big.Int).Add(vault, big.NewInt(500))) != 0 {
		t.Fatalf("available %s", available)
	}
}

func TestReconcileRefuses(t *testing.T) {
	e := newOpsEnv(t)
	e.sign(t, 7, false)
	wantRefused(t, e.reconcile(nil), "run drainme recover first")

	// the chain now holds less than is owed
	e = newOpsEnv(t)
	must(t, e.l.Credit(context.Background(), map[nanocrypto.PublicKey]uint64{payee: 50}))
	spent := nanocrypto.Hash{0x5e}
	e.node.Foreign(spent, big.NewInt(49))
	wantRefused(t, e.reconcile(&spent), "less than is already owed")
	if f, _ := e.head(t); f != genesis {
		t.Fatal("refusal changed the head")
	}

	// a node that disagrees with the others stops everything
	e = newOpsEnv(t)
	other := fakenode.New(t, e.key.Public(), nanocrypto.Hash{0x0d}, vault, 0)
	e.eps = append(e.eps, endpoint{"other", nanoclient.New([]*url.URL{other.URL}, nil, time.Second)})
	e.node.Foreign(nanocrypto.Hash{0x0e}, vault)
	wantRefused(t, e.reconcile(&nanocrypto.Hash{0x0e}), "disagree")
}

// a send recover put back that turns up on chain after all means its address was paid twice over: reconcile
// stops rather than adopt a chain that proves it
func TestReconcileRefusesARecreditedSendOnChain(t *testing.T) {
	e := newOpsEnv(t)
	s := e.sign(t, 7, false)
	e.node.Foreign(nanocrypto.Hash{0xbe, 0xef}, new(big.Int).Sub(vault, big.NewInt(1)))
	must(t, e.recover(true))
	e.node.Foreign(s.Hash, new(big.Int).Sub(vault, big.NewInt(8)))
	frontier := s.Hash
	wantRefused(t, e.reconcile(&frontier), "on chain after all")
}

func TestStatus(t *testing.T) {
	e := newOpsEnv(t)
	e.node.Fund(nanocrypto.Hash{0x99}, big.NewInt(42))
	r, err := ledger.OpenReader(filepath.Join(e.cfg.DataDir, "ledger.db"))
	must(t, err)
	defer r.Close()
	failover := nanoclient.New(e.cfg.RPCURLs, nil, time.Second)
	must(t, status(context.Background(), r, e.eps, failover, e.key.Public(), e.cfg.SendThreshold, &e.out))
	e.said(t, "matches the ledger")
	e.said(t, "books      balance")
	e.said(t, "42 raw in 1 sends")
	e.said(t, "in flight  nothing")
	e.said(t, "two nodes must agree") // the stack has one node, so the network minimum is unknown, and says why

	s := e.sign(t, 7, true)
	e.out.Reset()
	must(t, status(context.Background(), r, e.eps, failover, e.key.Public(), e.cfg.SendThreshold, &e.out))
	e.said(t, "already published")
	e.said(t, s.Hash.Hex())

	e.node.Foreign(nanocrypto.Hash{0x0f}, big.NewInt(1))
	e.out.Reset()
	must(t, status(context.Background(), r, e.eps, failover, e.key.Public(), e.cfg.SendThreshold, &e.out))
	e.said(t, "DIFFERS")
}

func TestBackupCommand(t *testing.T) {
	e := newOpsEnv(t)
	e.sign(t, 7, false)
	r, err := ledger.OpenReader(filepath.Join(e.cfg.DataDir, "ledger.db"))
	must(t, err)
	defer r.Close()
	dir := filepath.Join(e.cfg.DataDir, "backups")
	at := time.Date(2026, 10, 3, 21, 0, 0, 0, time.UTC)
	dest, err := backup(context.Background(), r, dir, 0, at, &e.out)
	must(t, err)
	if filepath.Base(dest) != "ledger-20261003-210000.db" {
		t.Fatalf("named %s", dest)
	}
	e.said(t, "opens and balances")
	if _, err := backup(context.Background(), r, dir, 0, at, &e.out); err == nil {
		t.Fatal("a second backup in the same second overwrote the first")
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("backup dir %v %v", fi, err)
	}
}

// the commands that write the ledger exclude each other and serve
func TestDataLock(t *testing.T) {
	dir := t.TempDir()
	release, err := lockData(dir)
	must(t, err)
	if _, err := lockData(dir); err == nil {
		t.Fatal("the lock was taken twice")
	}
	release()
	again, err := lockData(dir)
	must(t, err)
	again()

	cfg, _ := stack(t)
	held, err := lockData(cfg.DataDir)
	must(t, err)
	defer held()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	wantRefused(t, serve(context.Background(), cfg, ln, quiet()), "stop it first")
}

// the chain-side guards: each refuses and changes nothing
func TestReconcileGuards(t *testing.T) {
	ctx := context.Background()
	t.Run("too few nodes answer", func(t *testing.T) {
		e := newOpsEnv(t)
		dead, _ := url.Parse("http://127.0.0.1:1")
		e.eps = []endpoint{e.eps[0], {"dead", nanoclient.New([]*url.URL{dead}, nil, time.Second)}}
		e.node.Foreign(nanocrypto.Hash{0x0e}, vault)
		wantRefused(t, e.reconcile(&nanocrypto.Hash{0x0e}), "1 of 2 nodes answered")
	})
	t.Run("chain still settling", func(t *testing.T) {
		e := newOpsEnv(t)
		e.node.PendingPolls = 1000
		h := e.spend(t, 3) // someone's send from the vault, published but not yet cemented
		wantRefused(t, e.reconcile(&h), "still settling")
	})
	t.Run("node behind the ledger", func(t *testing.T) {
		e := newOpsEnv(t)
		ahead := nanocrypto.Hash{0xa4}
		e.node.Foreign(ahead, vault)
		must(t, e.l.SetHead(ctx, ahead, vault)) // the ledger has seen block 2; the node now reports block 1
		e.node.Intercept = func(action string) any {
			if action == "account_info" {
				return map[string]string{"frontier": genesis.Hex(), "balance": vault.String()}
			}
			return nil
		}
		wantRefused(t, e.reconcile(&genesis), "not ahead")
	})
	t.Run("ledger frontier unknown to the nodes", func(t *testing.T) {
		e := newOpsEnv(t)
		must(t, e.l.SetHead(ctx, nanocrypto.Hash{0x77}, vault))
		e.node.Foreign(nanocrypto.Hash{0x0e}, vault)
		wantRefused(t, e.reconcile(&nanocrypto.Hash{0x0e}), "no node knows")
	})
}

// spend publishes a send from the vault that drainme did not make, as a wallet holding the seed would
func (e *opsEnv) spend(t *testing.T, amount int64) nanocrypto.Hash {
	t.Helper()
	ctx := context.Background()
	frontier, bal := e.node.Head()
	node := nanoclient.New(e.cfg.RPCURLs, e.cfg.RPCURLs, time.Second)
	work, err := node.Work(ctx, frontier, e.cfg.SendThreshold)
	must(t, err)
	rep, _ := nanocrypto.ParseAddress(e.cfg.Representative)
	b := nanoclient.Block{StateBlock: nanocrypto.StateBlock{Account: e.key.Public(), Previous: frontier, Representative: rep,
		Balance: new(big.Int).Sub(bal, big.NewInt(amount)), Link: payee}, Work: work}
	h, _ := b.Hash()
	b.Signature = e.key.Sign(h)
	must(t, node.Process(ctx, h, b.Encode(), "send"))
	return h
}

// each check on what the nodes say stands on its own
func TestNodeAgreementChecks(t *testing.T) {
	t.Run("same blocks, different balance", func(t *testing.T) {
		e := newOpsEnv(t)
		other := fakenode.New(t, e.key.Public(), genesis, vault, 0)
		moved := nanocrypto.Hash{0x0e}
		e.node.Foreign(moved, vault)
		other.Foreign(moved, new(big.Int).Add(vault, big.NewInt(1)))
		e.eps = append(e.eps, endpoint{"other", nanoclient.New([]*url.URL{other.URL}, nil, time.Second)})
		wantRefused(t, e.reconcile(&moved), "disagree about the account")
	})
	t.Run("same account, different block", func(t *testing.T) {
		e := newOpsEnv(t)
		other := fakenode.New(t, e.key.Public(), genesis, vault, 0)
		moved := nanocrypto.Hash{0x0e}
		e.node.Foreign(moved, vault)
		other.Foreign(moved, vault)
		other.Intercept = func(action string) any {
			if action == "block_info" {
				return map[string]string{"confirmed": "false", "height": "2"}
			}
			return nil
		}
		e.eps = append(e.eps, endpoint{"other", nanoclient.New([]*url.URL{other.URL}, nil, time.Second)})
		wantRefused(t, e.reconcile(&moved), "disagree about block")
	})
	t.Run("a different block at the same height", func(t *testing.T) {
		e := newOpsEnv(t)
		moved := nanocrypto.Hash{0x0e}
		e.node.Foreign(moved, vault)
		e.node.Intercept = func(action string) any {
			if action == "block_info" {
				return map[string]string{"confirmed": "true", "height": "1"}
			}
			return nil
		}
		wantRefused(t, e.reconcile(&moved), "not ahead")
	})
}

func TestBackupRefusesBrokenBooks(t *testing.T) {
	e := newOpsEnv(t)
	db, err := sql.Open("sqlite", filepath.Join(e.cfg.DataDir, "ledger.db"))
	must(t, err)
	_, err = db.Exec(`UPDATE vault SET available = '1'`)
	db.Close()
	must(t, err)
	r, err := ledger.OpenReader(filepath.Join(e.cfg.DataDir, "ledger.db"))
	must(t, err)
	defer r.Close()
	_, err = backup(context.Background(), r, t.TempDir(), 0, time.Now(), &e.out)
	wantRefused(t, err, "does not balance")
}

// D104: hourly backups keep the newest copies and delete only backup files
func TestBackupKeeps(t *testing.T) {
	e := newOpsEnv(t)
	r, err := ledger.OpenReader(filepath.Join(e.cfg.DataDir, "ledger.db"))
	must(t, err)
	defer r.Close()
	dir := t.TempDir()
	must(t, os.WriteFile(filepath.Join(dir, "notes.txt"), nil, 0o600))
	must(t, os.WriteFile(filepath.Join(dir, "ledger-latest.db"), nil, 0o600))
	at := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	for h := 0; h < 5; h++ {
		if _, err := backup(context.Background(), r, dir, 3, at.Add(time.Duration(h)*time.Hour), &e.out); err != nil {
			t.Fatal(err)
		}
	}
	entries, _ := os.ReadDir(dir)
	var names []string
	for _, x := range entries {
		names = append(names, x.Name())
	}
	want := "ledger-20261004-020000.db ledger-20261004-030000.db ledger-20261004-040000.db ledger-latest.db notes.txt"
	if strings.Join(names, " ") != want {
		t.Fatalf("left %v", names)
	}
}

func TestParseBackupArgs(t *testing.T) {
	for _, c := range []struct {
		args []string
		dir  string
		keep int
		ok   bool
	}{
		{nil, "def", 0, true},
		{[]string{"/x"}, "/x", 0, true},
		{[]string{"--keep", "48"}, "def", 48, true},
		{[]string{"/x", "--keep", "2"}, "/x", 2, true},
		{[]string{"--keep", "0"}, "", 0, false},
		{[]string{"--keep"}, "", 0, false},
		{[]string{"--keep", "x"}, "", 0, false},
		{[]string{"/x", "/y"}, "", 0, false},
		{[]string{"--delete"}, "", 0, false},
	} {
		dir, keep, err := parseBackupArgs(c.args, "def")
		if (err == nil) != c.ok || (c.ok && (dir != c.dir || keep != c.keep)) {
			t.Errorf("%v: %s %d %v", c.args, dir, keep, err)
		}
	}
}
