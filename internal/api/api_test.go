package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ingest"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/stats"
)

var ctx = context.Background()

func pub(i byte) nanocrypto.PublicKey { return nanocrypto.DeriveKey([32]byte{i, 0xa9}, 0).Public() }

// the vault every test server runs with
var vaultKey = nanocrypto.DeriveKey([32]byte{0xfe}, 0).Public()

type env struct {
	l     *ledger.Ledger
	c     *ingest.Counter
	st    *stats.Stats
	srv   *httptest.Server
	woken atomic.Int32 // how often the payout worker was woken
}

func setup(t *testing.T, vault int64, queueMax, addressMax int) *env {
	t.Helper()
	l, err := ledger.Open(filepath.Join(t.TempDir(), "ledger.db"), queueMax)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := l.SetHead(ctx, nanocrypto.Hash{1}, big.NewInt(vault)); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c, err := ingest.New(ctx, l, addressMax, log)
	if err != nil {
		t.Fatal(err)
	}
	st := stats.New(l, c, 10, log)
	e := &env{l: l, c: c, st: st}
	e.srv = httptest.NewServer(New(l, c, st, func() { e.woken.Add(1) }, func(p int64) time.Duration { return time.Duration(p) * 2500 * time.Millisecond }, vaultKey, log))
	t.Cleanup(e.srv.Close)
	return e
}

type reply struct {
	status int
	body   string
	header http.Header
}

func (e *env) do(t *testing.T, method, path string, body string) reply {
	t.Helper()
	req, err := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return reply{resp.StatusCode, string(b), resp.Header}
}

func (e *env) balance(t *testing.T, p nanocrypto.PublicKey) string {
	t.Helper()
	if err := e.c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := e.l.Balance(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func want(t *testing.T, r reply, status int) {
	t.Helper()
	if r.status != status {
		t.Fatalf("status %d, want %d: %s", r.status, status, r.body)
	}
	if r.header.Get("Cache-Control") != "no-store" || r.header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("missing headers: %v", r.header)
	}
}

// R1: one accepted request is exactly one raw, whatever else the request carries
func TestClickIsExactlyOneRaw(t *testing.T) {
	e := setup(t, 1000, 10, 10)
	a := pub(1).Address()
	want(t, e.do(t, "POST", "/click/"+a, ""), http.StatusNoContent)
	want(t, e.do(t, "POST", "/click/"+a+"?count=1000&amount=1000&n=1000", `{"count":1000,"amount":"1000"}`), http.StatusNoContent)
	want(t, e.do(t, "POST", "/click/"+a, "1000"), http.StatusNoContent)
	if b := e.balance(t, pub(1)); b != "3" {
		t.Fatalf("three requests credited %s raw", b)
	}
}

func TestBadAddresses(t *testing.T) {
	e := setup(t, 1000, 10, 10)
	good := pub(1).Address()
	last := "1"
	if good[64] == '1' {
		last = "3"
	}
	for name, addr := range map[string]string{
		"xrb prefix":     "xrb_" + good[5:],
		"bad checksum":   good[:64] + last,
		"short":          good[:64],
		"uppercase":      strings.ToUpper(good),
		"script":         "%3Cscript%3Ealert(1)%3C%2Fscript%3E",
		"percent-padded": good + "%20",
	} {
		for _, route := range []string{"/click/", "/withdraw/"} {
			r := e.do(t, "POST", route+addr, "")
			want(t, r, http.StatusBadRequest)
			if strings.Contains(r.body, "script") || strings.Contains(r.body, addr) {
				t.Errorf("%s %s: reply echoes the input: %q", name, route, r.body)
			}
		}
	}
	if n, _ := e.l.Addresses(ctx); n != 0 {
		t.Fatal("a bad address was credited")
	}
}

// paying the vault to itself only parks raw out of reach (it never receives), so its address is refused
func TestVaultAddressRefused(t *testing.T) {
	e := setup(t, 1000, 10, 10)
	for _, route := range []string{"/click/", "/withdraw/"} {
		r := e.do(t, "POST", route+vaultKey.Address(), "")
		want(t, r, http.StatusBadRequest)
		if r.body != msgVault+"\n" {
			t.Errorf("%s: %q, want the vault message", route, r.body)
		}
	}
	if n, _ := e.l.Addresses(ctx); n != 0 {
		t.Fatal("the vault's address was credited")
	}
	if n := e.woken.Load(); n != 0 {
		t.Fatal("the payout worker was woken")
	}
	want(t, e.do(t, "POST", "/click/"+pub(1).Address(), ""), http.StatusNoContent) // everyone else still counts
}

// R8 and D31: only the two routes exist, POST only, and none of them takes a destination
func TestRoutes(t *testing.T) {
	e := setup(t, 1000, 10, 10)
	a, b := pub(1).Address(), pub(2).Address()
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/click/" + a, http.StatusMethodNotAllowed},
		{"PUT", "/withdraw/" + a, http.StatusMethodNotAllowed},
		{"POST", "/withdraw/" + a + "/" + b, http.StatusNotFound},
		{"POST", "/withdraw?to=" + b, http.StatusNotFound},
		{"POST", "/click/", http.StatusNotFound},
		{"POST", "/send/" + a, http.StatusNotFound},
	} {
		if r := e.do(t, tc.method, tc.path, ""); r.status != tc.status {
			t.Errorf("%s %s: %d, want %d", tc.method, tc.path, r.status, tc.status)
		}
	}
}

func TestWithdraw(t *testing.T) {
	e := setup(t, 1000, 10, 10)
	a := pub(1).Address()
	want(t, e.do(t, "POST", "/withdraw/"+a, ""), http.StatusConflict)
	for i := 0; i < 3; i++ {
		e.do(t, "POST", "/click/"+a, "")
	}
	// no flush in between: the withdrawal must still include clicks that are only in memory
	r := e.do(t, "POST", "/withdraw/"+a+"?raw=999", "")
	want(t, r, http.StatusOK)
	var got struct {
		Raw      string `json:"raw"`
		Position int64  `json:"position"`
		ETA      int64  `json:"eta_s"`
	}
	// position 1 at 2.5 s a payout, rounded up to whole seconds
	if err := json.Unmarshal([]byte(r.body), &got); err != nil || got.Raw != "3" || got.Position != 1 || got.ETA != 3 {
		t.Fatalf("reply %q: %+v %v", r.body, got, err)
	}
	if r.header.Get("Content-Type") != "application/json" {
		t.Fatalf("content type %q", r.header.Get("Content-Type"))
	}
	want(t, e.do(t, "POST", "/withdraw/"+a, ""), http.StatusConflict)
	if n := e.woken.Load(); n != 1 {
		t.Fatalf("worker woken %d times for one queued withdrawal and two refusals", n)
	}
	if err := e.l.Audit(ctx); err != nil {
		t.Fatal(err)
	}
}

// F1 and D89: full means 503 with a retry hint, and nothing is taken from anyone
func TestFullAnswers503(t *testing.T) {
	e := setup(t, 1000, 1, 1)
	a, b := pub(1).Address(), pub(2).Address()
	want(t, e.do(t, "POST", "/click/"+a, ""), http.StatusNoContent)
	r := e.do(t, "POST", "/click/"+b, "")
	want(t, r, http.StatusServiceUnavailable)
	if r.header.Get("Retry-After") == "" {
		t.Fatal("no Retry-After on 503")
	}
	want(t, e.do(t, "POST", "/withdraw/"+a, ""), http.StatusOK) // frees a's address slot and takes the one queue place
	if err := e.c.Flush(ctx); err != nil {                      // slots are recounted at each flush, every 5 s in service
		t.Fatal(err)
	}
	want(t, e.do(t, "POST", "/click/"+b, ""), http.StatusNoContent)
	r = e.do(t, "POST", "/withdraw/"+b, "")
	want(t, r, http.StatusServiceUnavailable)
	if !strings.Contains(r.body, "line is full") {
		t.Fatalf("queue-full reply: %q", r.body)
	}
	if bal := e.balance(t, pub(2)); bal != "1" {
		t.Fatalf("b's balance after a refused withdrawal: %s", bal)
	}
}

// D20: once the vault cannot pay, clicks are refused and say why
func TestHaltedAnswers503(t *testing.T) {
	e := setup(t, 2, 10, 10)
	a := pub(1).Address()
	for i := 0; i < 3; i++ {
		want(t, e.do(t, "POST", "/click/"+a, ""), http.StatusNoContent)
	}
	e.c.Flush(ctx) // three clicks against a vault of two: refused whole, and the counter halts
	r := e.do(t, "POST", "/click/"+a, "")
	want(t, r, http.StatusServiceUnavailable)
	if !strings.Contains(r.body, "halted") {
		t.Fatalf("halt reply: %q", r.body)
	}
}

func TestShutdownAndInternalErrors(t *testing.T) {
	e := setup(t, 1000, 10, 10)
	a := pub(1).Address()
	runCtx, cancel := context.WithCancel(ctx)
	cancel()
	e.c.Run(runCtx, time.Hour) // returns at once: final flush, then clicks are refused
	r := e.do(t, "POST", "/click/"+a, "")
	want(t, r, http.StatusServiceUnavailable)
	if !strings.Contains(r.body, "Shutting down") {
		t.Fatalf("shutdown reply: %q", r.body)
	}
	e.l.Close() // the database going away is an internal error, said plainly and without detail
	r = e.do(t, "POST", "/withdraw/"+a, "")
	want(t, r, http.StatusInternalServerError)
	if !strings.Contains(r.body, "safe") || strings.Contains(r.body, "sql") {
		t.Fatalf("internal reply: %q", r.body)
	}
}

// an accepted click reaches the live feed; a refused one does not
func TestClicksReachTheFeed(t *testing.T) {
	e := setup(t, 1000, 10, 10)
	feed := httptest.NewServer(e.st)
	defer feed.Close()
	resp, err := http.Get(feed.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	for deadline := time.Now().Add(5 * time.Second); e.st.Viewers() != 1; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("viewer never subscribed")
		}
	}
	e.do(t, "POST", "/click/xrb_refused", "")
	want(t, e.do(t, "POST", "/click/"+pub(1).Address(), ""), http.StatusNoContent)
	if err := e.st.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(resp.Body)
	var got []string
	for len(got) < 2 {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "event: ") {
			got = append(got, strings.TrimSpace(line[7:]))
		}
	}
	if got[0] != "click" || got[1] != "snapshot" {
		t.Fatalf("feed events %v, want one click then the snapshot", got)
	}
}
