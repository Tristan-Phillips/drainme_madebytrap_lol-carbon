package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/config"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/fakenode"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

var local = flag.String("local", "", "serve the whole stack against the fake node on this address until interrupted, e.g. -local 127.0.0.1:8080")

const (
	threshold = "ff00000000000000" // low enough that the fake node finds work at once
	testSeed  = "00000000000000000000000000000000000000000000000000000000000d7a1e"
)

var (
	genesis = nanocrypto.Hash{0xd7, 0x01}
	vault   = new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)
)

// stack is a data dir with a funded vault (as init-vault leaves it), a fake node holding the same chain, and
// the configuration serve reads, with the test seed in a file as compose would mount it
func stack(t *testing.T) (*config.Config, *fakenode.Node) { return stackAt(t, true) }

// stackAt with opened false is the state before init-vault: an empty ledger and an account never opened
func stackAt(t *testing.T, opened bool) (*config.Config, *fakenode.Node) {
	t.Helper()
	dir := t.TempDir()
	seedFile := filepath.Join(dir, "seed")
	if err := os.WriteFile(seedFile, []byte(testSeed+"\n"), 0o400); err != nil {
		t.Fatal(err)
	}
	seed, err := config.ReadSeed(seedFile)
	if err != nil {
		t.Fatal(err)
	}
	account := nanocrypto.DeriveKey(seed, 0).Public()
	var want uint64
	fmt.Sscanf(threshold, "%x", &want)
	node := fakenode.New(t, account, nanocrypto.Hash{}, new(big.Int), want)
	if opened {
		l, err := ledger.Open(filepath.Join(dir, "ledger.db"), 1000)
		if err != nil {
			t.Fatal(err)
		}
		if err := l.SetHead(context.Background(), genesis, vault); err != nil {
			t.Fatal(err)
		}
		l.Close()
		node = fakenode.New(t, account, genesis, vault, want)
	}
	env := map[string]string{
		"DRAINME_DATA_DIR":          dir,
		"DRAINME_SEED_FILE":         seedFile,
		"DRAINME_RPC_URLS":          node.URL.String(),
		"DRAINME_SEND_THRESHOLD":    threshold,
		"DRAINME_RECEIVE_THRESHOLD": threshold,
		"DRAINME_REPRESENTATIVE":    nanocrypto.DeriveKey([32]byte{0xd7}, 0).Public().Address(),
	}
	cfg, err := config.Load(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	return cfg, node
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestServeEndToEnd(t *testing.T) {
	cfg, node := stack(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg, ln, quiet()) }()
	if err := healthcheck(base + "/healthz"); err != nil {
		t.Fatal(err)
	}

	// the page: compressed once, revalidated by ETag, locked down by CSP
	req, _ := http.NewRequest("GET", base+"/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := http.DefaultTransport.RoundTrip(req) // the transport, so gzip is not undone for us
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	html, _ := io.ReadAll(zr)
	resp.Body.Close()
	if !strings.Contains(string(html), "cannot be drained by clicking") || !strings.Contains(resp.Header.Get("Content-Security-Policy"), "default-src 'none'") {
		t.Fatalf("page: %d %v", resp.StatusCode, resp.Header)
	}
	req.Header.Set("If-None-Match", resp.Header.Get("ETag"))
	if again, err := http.DefaultTransport.RoundTrip(req); err != nil || again.StatusCode != http.StatusNotModified {
		t.Fatalf("revalidation: %v %v", again, err)
	}

	// D104: viewers past the stream cap poll the snapshot; it serves once the first tick has run
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		resp, err := http.Get(base + "/snapshot")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			if !strings.Contains(string(body), `"remaining":"`) || resp.Header.Get("Cache-Control") != "public, max-age=10" {
				t.Fatalf("snapshot: %s %v", body, resp.Header)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot never served: %d", resp.StatusCode)
		}
	}

	events, err := http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer events.Body.Close()
	payouts := make(chan string, 4)
	go func() {
		r := bufio.NewReader(events.Body)
		var name string
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				close(payouts)
				return
			}
			if strings.HasPrefix(line, "event: ") {
				name = strings.TrimSpace(line[7:])
			} else if name == "payout" && strings.HasPrefix(line, "data: ") {
				payouts <- line[6:]
			}
		}
	}()

	me := nanocrypto.DeriveKey([32]byte{0xd7, 0x02}, 0).Public()
	for i := 0; i < 5; i++ {
		post(t, base+"/click/"+me.Address(), http.StatusNoContent)
	}
	if body := post(t, base+"/withdraw/"+me.Address(), http.StatusOK); !strings.Contains(body, `"raw":"5"`) {
		t.Fatalf("withdraw: %s", body)
	}

	// the worker pays it on the (fake) chain, and the stream announces it with the exact amount and block
	select {
	case data := <-payouts:
		var p struct{ A, Raw, Hash string }
		if err := json.Unmarshal([]byte(data), &p); err != nil || p.A != me.Address() || p.Raw != "5" || len(p.Hash) != 64 {
			t.Fatalf("payout event %s: %v", data, err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("no payout on the stream")
	}
	if node.Paid(me).String() != "5" || node.Blocks() != 1 {
		t.Fatalf("node paid %s in %d blocks", node.Paid(me), node.Blocks())
	}

	// clicks still in memory at shutdown are flushed, and the open stream does not hold shutdown up
	post(t, base+"/click/"+me.Address(), http.StatusNoContent)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second): // under the 15 s shutdown limit: an open stream must not hold it up
		t.Fatal("serve did not stop")
	}
	l, err := ledger.Open(filepath.Join(cfg.DataDir, "ledger.db"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if b, _ := l.Balance(context.Background(), me); b.String() != "1" {
		t.Fatalf("click before shutdown: balance %s, want 1", b)
	}
	if err := l.Audit(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func post(t *testing.T, url string, want int) string {
	t.Helper()
	resp, err := http.Post(url, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		t.Fatalf("POST %s: %d %s", url, resp.StatusCode, body)
	}
	return string(body)
}

// serve refuses to start on books that do not balance (D19) or without a valid representative
func TestServeRefusesToStart(t *testing.T) {
	cfg, _ := stack(t)
	bad := *cfg
	bad.Representative = "nano_nope"
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	if err := serve(context.Background(), &bad, ln, quiet()); err == nil || !strings.Contains(err.Error(), "REPRESENTATIVE") {
		t.Fatalf("bad representative: %v", err)
	}
	// money appearing from nowhere, written past the ledger as only a bug or an intruder could
	db, err := sql.Open("sqlite", filepath.Join(cfg.DataDir, "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE vault SET available = '1'`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	ln, _ = net.Listen("tcp", "127.0.0.1:0")
	if err := serve(context.Background(), cfg, ln, quiet()); err == nil || !strings.Contains(err.Error(), "do not balance") {
		t.Fatalf("unbalanced books: %v", err)
	}
}

// TestLocal runs the whole stack for a person to click around in, with no real network or funds:
//
//	go test ./cmd/drainme -run TestLocal -local 127.0.0.1:8080
func TestLocal(t *testing.T) {
	if *local == "" {
		t.Skip("pass -local host:port to run the stack by hand")
	}
	cfg, _ := stack(t)
	ln, err := net.Listen("tcp", *local)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	fmt.Printf("drainme on http://%s against the fake node, data in %s; Ctrl-C to stop\n", ln.Addr(), cfg.DataDir)
	if err := serve(ctx, cfg, ln, slog.New(slog.NewTextHandler(os.Stdout, nil))); err != nil {
		t.Fatal(err)
	}
}
