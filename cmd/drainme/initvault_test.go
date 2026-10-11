package main

import (
	"bytes"
	"context"
	"math/big"
	"net"
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

var funding = nanocrypto.Hash{0xf0, 0x0d} // the operator's send from their own wallet

type vaultEnv struct {
	cfg  *config.Config
	node *fakenode.Node
	key  nanocrypto.PrivateKey
	rep  nanocrypto.PublicKey
	out  bytes.Buffer
}

func newVaultEnv(t *testing.T) *vaultEnv {
	cfg, node := stackAt(t, false)
	seed, err := config.ReadSeed(cfg.SeedFile)
	if err != nil {
		t.Fatal(err)
	}
	rep, _ := nanocrypto.ParseAddress(cfg.Representative)
	return &vaultEnv{cfg: cfg, node: node, key: nanocrypto.DeriveKey(seed, 0), rep: rep}
}

// run is init-vault against ledger file name in the stack's data dir
func (e *vaultEnv) run(t *testing.T, name, amount string, resume *nanocrypto.Hash) error {
	t.Helper()
	l, err := ledger.Open(filepath.Join(e.cfg.DataDir, name), 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	want, _ := new(big.Int).SetString(amount, 10)
	node := nanoclient.New(e.cfg.RPCURLs, e.cfg.WorkURLs, time.Second)
	return initVault(context.Background(), l, node, e.key, e.rep, e.cfg.ReceiveThreshold, want, resume, &e.out, time.Millisecond)
}

func (e *vaultEnv) head(t *testing.T, name string) (nanocrypto.Hash, *big.Int, error) {
	t.Helper()
	l, err := ledger.Open(filepath.Join(e.cfg.DataDir, name), 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Head(context.Background())
}

func wantRefused(t *testing.T, err error, says string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), says) {
		t.Fatalf("want a refusal saying %q, got %v", says, err)
	}
}

// every refusal leaves the chain and the ledger exactly as they were
func TestInitVaultRefuses(t *testing.T) {
	e := newVaultEnv(t)
	wantRefused(t, e.run(t, "ledger.db", "1000", nil), "found 0")
	e.node.Fund(funding, big.NewInt(1000))
	wantRefused(t, e.run(t, "ledger.db", "999", nil), "not the 999 given")
	e.node.Fund(nanocrypto.Hash{0xd5}, big.NewInt(1)) // someone's dust arrives too
	wantRefused(t, e.run(t, "ledger.db", "1000", nil), "found 2")
	if e.node.Blocks() != 0 || e.node.Processed() != 0 {
		t.Fatal("a refusal reached the chain")
	}
	if _, _, err := e.head(t, "ledger.db"); err != ledger.ErrNoHead {
		t.Fatalf("a refusal touched the ledger: %v", err)
	}
}

func TestInitVaultOpensOnce(t *testing.T) {
	e := newVaultEnv(t)
	e.node.Fund(funding, vault)
	if err := e.run(t, "ledger.db", vault.String(), nil); err != nil {
		t.Fatal(err)
	}
	frontier, bal := e.node.Head()
	h, b, err := e.head(t, "ledger.db")
	if err != nil || h != frontier || b.Cmp(vault) != 0 || bal.Cmp(vault) != 0 || e.node.Blocks() != 1 {
		t.Fatalf("ledger %X %s %v, node %X %s, %d blocks", h, b, err, frontier, bal, e.node.Blocks())
	}
	if !strings.Contains(e.out.String(), "--resume "+h.Hex()) {
		t.Fatalf("the resume hint was not printed before broadcast:\n%s", e.out.String())
	}
	wantRefused(t, e.run(t, "ledger.db", vault.String(), nil), "runs once")
	// a different, empty ledger against the opened account: told to resume, not to open again
	wantRefused(t, e.run(t, "other.db", vault.String(), nil), "already open on chain")
}

// interrupted after the block was confirmed but before the ledger recorded it: --resume finishes with only our
// own hash and amount, and refuses anything that does not match the chain
func TestInitVaultResume(t *testing.T) {
	e := newVaultEnv(t)
	e.node.Fund(funding, vault)
	if err := e.run(t, "first.db", vault.String(), nil); err != nil {
		t.Fatal(err)
	}
	h, _, _ := e.head(t, "first.db") // "crashed.db" plays the ledger that never got this far
	wantRefused(t, e.run(t, "crashed.db", vault.String(), &nanocrypto.Hash{0x0b}), "nothing was recorded")
	wantRefused(t, e.run(t, "crashed.db", "999", &h), "nothing was recorded")
	if _, _, err := e.head(t, "crashed.db"); err != ledger.ErrNoHead {
		t.Fatalf("a refused resume touched the ledger: %v", err)
	}
	if err := e.run(t, "crashed.db", vault.String(), &h); err != nil {
		t.Fatal(err)
	}
	if got, bal, err := e.head(t, "crashed.db"); err != nil || got != h || bal.Cmp(vault) != 0 {
		t.Fatalf("resumed head %X %s %v", got, bal, err)
	}
	if e.node.Blocks() != 1 {
		t.Fatalf("%d blocks: resume sent something", e.node.Blocks())
	}
}

// the launch path: an unopened account is funded, init-vault opens it, and serve pays from it
func TestInitVaultThenServe(t *testing.T) {
	e := newVaultEnv(t)
	e.node.Fund(funding, vault)
	if err := e.run(t, "ledger.db", vault.String(), nil); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, e.cfg, ln, quiet()) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	if err := healthcheck(base + "/healthz"); err != nil {
		t.Fatal(err)
	}
	me := nanocrypto.DeriveKey([32]byte{0xd7, 0x03}, 0).Public()
	for i := 0; i < 3; i++ {
		post(t, base+"/click/"+me.Address(), 204)
	}
	post(t, base+"/withdraw/"+me.Address(), 200)
	for deadline := time.Now().Add(30 * time.Second); e.node.Paid(me).String() != "3"; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("not paid: node shows %s", e.node.Paid(me))
		}
	}
}

func TestParseInitArgs(t *testing.T) {
	h := strings.Repeat("AB", 32)
	if want, resume, err := parseInitArgs([]string{"1000"}); err != nil || want.String() != "1000" || resume != nil {
		t.Fatalf("plain: %v %v %v", want, resume, err)
	}
	if _, resume, err := parseInitArgs([]string{"1000", "--resume", h}); err != nil || resume.Hex() != h {
		t.Fatalf("resume: %v %v", resume, err)
	}
	over := new(big.Int).Lsh(big.NewInt(1), 128).String()
	for _, args := range [][]string{{"0"}, {"-5"}, {"01"}, {"1e30"}, {"1.5"}, {over}, {"5", "--resume"}, {"5", "--resume", "ab"},
		{"5", "--resume", strings.ToLower(h)}, {"5", "--force", h}, {"5", "6"}} {
		if _, _, err := parseInitArgs(args); err == nil {
			t.Errorf("%q accepted", args)
		}
	}
}
