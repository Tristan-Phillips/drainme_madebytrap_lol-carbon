package stats

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ingest"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/sysinfo"
)

var ctx = context.Background()

func pub(i byte) nanocrypto.PublicKey { return nanocrypto.PublicKey{i, 0x5e} }

func setup(t *testing.T, vault *big.Int, viewers int) (*Stats, *ledger.Ledger, *ingest.Counter, *time.Time) {
	t.Helper()
	l, err := ledger.Open(filepath.Join(t.TempDir(), "ledger.db"), 10)
	must(t, err)
	t.Cleanup(func() { l.Close() })
	must(t, l.SetHead(ctx, nanocrypto.Hash{1}, vault))
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c, err := ingest.New(ctx, l, 100, log)
	must(t, err)
	s := New(l, c, viewers, log)
	clock := time.Unix(1_800_000_000, 0)
	s.now = func() time.Time { return clock }
	return s, l, c, &clock
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

type snap struct {
	Remaining, Chain, Unpaid, Clicks string
	Rate                             json.Number
	DrainYears                       *string `json:"drain_years"`
	UniverseX                        *string `json:"universe_x"`
	Halted                           bool
}

// events splits SSE bytes into (name, data) pairs, failing on anything malformed
func events(t *testing.T, b []byte) [][2]string {
	t.Helper()
	var out [][2]string
	for _, block := range strings.Split(strings.TrimSuffix(string(b), "\n\n"), "\n\n") {
		lines := strings.Split(block, "\n")
		if len(lines) != 2 || !strings.HasPrefix(lines[0], "event: ") || !strings.HasPrefix(lines[1], "data: ") {
			t.Fatalf("malformed event %q", block)
		}
		out = append(out, [2]string{lines[0][7:], lines[1][6:]})
	}
	return out
}

func readSnap(t *testing.T, s *Stats) snap {
	t.Helper()
	b, err := s.snapshot(ctx)
	must(t, err)
	var got snap
	must(t, json.Unmarshal(b, &got))
	return got
}

// the figures the verification brief checked: 1 XNO at 1 click/s lasts 3.17e22 years, at 100,000/s 3.17e17
func TestSci(t *testing.T) {
	xno := new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)
	for _, tc := range []struct {
		num, den *big.Int
		want     string
	}{
		{xno, secondsPerYear, "3.16e22"}, // 3.1688e22, truncated
		{xno, new(big.Int).Mul(big.NewInt(100_000), secondsPerYear), "3.16e17"},
		{big.NewInt(689), big.NewInt(10), "6.89e1"},
		{big.NewInt(999_999), big.NewInt(1), "9.99e5"},
		{big.NewInt(1), big.NewInt(3), "3.33e-1"},
		{big.NewInt(5), big.NewInt(1), "5.00e0"},
		{big.NewInt(0), big.NewInt(7), "0"},
		{big.NewInt(1), new(big.Int).Exp(big.NewInt(10), big.NewInt(40), nil), "0"},
	} {
		if got := sci(tc.num, tc.den); got != tc.want {
			t.Errorf("%s/%s = %s, want %s", tc.num, tc.den, got, tc.want)
		}
	}
}

func TestSnapshot(t *testing.T) {
	s, l, c, clock := setup(t, big.NewInt(1000), 10)
	got := readSnap(t, s)
	if got.Remaining != "1000" || got.Chain != "1000" || got.Unpaid != "0" || got.Clicks != "0" || got.Rate != "0.0" || got.DrainYears != nil || got.UniverseX != nil {
		t.Fatalf("fresh vault: %+v", got)
	}

	// 100 clicks credited and 25 still in memory: the page counts both
	for i := 0; i < 125; i++ {
		must(t, c.Click(ctx, pub(1)))
		s.Click(pub(1))
		if i == 99 {
			must(t, c.Flush(ctx))
		}
	}
	got = readSnap(t, s)
	if got.Remaining != "875" || got.Chain != "1000" || got.Unpaid != "125" || got.Clicks != "125" {
		t.Fatalf("after 125 clicks: %+v", got)
	}
	// 125 clicks in the 60 s window is 2.083/s, shown as 2.0
	if got.Rate != "2.0" || got.DrainYears == nil {
		t.Fatalf("rate %s, drain %v", got.Rate, got.DrainYears)
	}
	// years = 875 × 60 / (125 × 31,557,600) = 1.33e-5; universe = that / 1.38e10 = 9.64e-16
	if *got.DrainYears != "1.33e-5" || *got.UniverseX != "9.64e-16" {
		t.Fatalf("drain %s, universe %s", *got.DrainYears, *got.UniverseX)
	}
	if _, _, err := l.Withdraw(ctx, pub(1)); err != nil {
		t.Fatal(err)
	}
	if got = readSnap(t, s); got.Unpaid != "125" || got.Remaining != "875" {
		t.Fatalf("a queued withdrawal is still owed: %+v", got)
	}

	// the window slides: clicks older than 60 s stop counting, and with no rate there is no drain time
	*clock = clock.Add(61 * time.Second)
	if got = readSnap(t, s); got.Rate != "0.0" || got.DrainYears != nil {
		t.Fatalf("after the window: rate %s, drain %v", got.Rate, got.DrainYears)
	}
	s.SetHalted(true)
	if got = readSnap(t, s); !got.Halted {
		t.Fatal("halt not shown")
	}
}

// D35: at most 10 click events a second, each with a truncated address only
func TestClickSampling(t *testing.T) {
	s, _, _, clock := setup(t, big.NewInt(1000), 10)
	for i := 0; i < 15; i++ {
		s.Click(pub(2))
	}
	*clock = clock.Add(time.Second)
	s.Click(pub(2))
	ev := events(t, s.samples)
	if len(ev) != 11 {
		t.Fatalf("%d click events from 16 clicks over two seconds, want 11", len(ev))
	}
	full := pub(2).Address()
	var c struct {
		A string
		T int64
	}
	must(t, json.Unmarshal([]byte(ev[0][1]), &c))
	if c.A != full[:10]+"…"+full[59:] || c.T != 1_800_000_000_000 || strings.Contains(string(s.samples), full) {
		t.Fatalf("click event %+v exposes or mangles the address", c)
	}
}

func TestPaidEvent(t *testing.T) {
	s, _, _, _ := setup(t, big.NewInt(1000), 10)
	s.Paid(ledger.Signed{Withdrawal: ledger.Withdrawal{Address: pub(3), Amount: big.NewInt(42)}, Hash: nanocrypto.Hash{0xAB}})
	ev := events(t, s.payouts)
	var p struct{ A, Raw, Hash string }
	must(t, json.Unmarshal([]byte(ev[0][1]), &p))
	if ev[0][0] != "payout" || p.A != pub(3).Address() || p.Raw != "42" || p.Hash != (nanocrypto.Hash{0xAB}).Hex() {
		t.Fatalf("payout event %v", ev)
	}
}

// D33: a viewer that stops reading is dropped, never waited for
func TestSlowViewerDropped(t *testing.T) {
	s, _, _, _ := setup(t, big.NewInt(1000), 10)
	slow := make(chan []byte, viewerBuffer)
	s.viewers[slow] = struct{}{}
	for i := 0; i <= viewerBuffer; i++ {
		must(t, s.Tick(ctx))
	}
	if s.Viewers() != 0 {
		t.Fatal("slow viewer still subscribed")
	}
	for range slow { // drains the buffered updates, then ends because the channel was closed
	}
}

func stream(t *testing.T, url string) (*bufio.Reader, func()) {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := http.DefaultClient.Do(req)
	must(t, err)
	if resp.Header.Get("Content-Type") != "text/event-stream" || resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("headers %v", resp.Header)
	}
	return bufio.NewReader(resp.Body), func() { resp.Body.Close() }
}

func next(t *testing.T, r *bufio.Reader) [2]string {
	t.Helper()
	var block strings.Builder
	for {
		line, err := r.ReadString('\n')
		must(t, err)
		if line == "\n" {
			return events(t, []byte(block.String()+"\n"))[0]
		}
		block.WriteString(line)
	}
}

// over real HTTP: a viewer gets the last snapshot at once, then each tick's events; past the cap, "full"
func TestStream(t *testing.T) {
	s, _, _, _ := setup(t, big.NewInt(1000), 1)
	srv := httptest.NewServer(s)
	defer srv.Close()
	must(t, s.Tick(ctx))

	r, done := stream(t, srv.URL)
	defer done()
	if ev := next(t, r); ev[0] != "snapshot" {
		t.Fatalf("first event %v", ev)
	}
	for deadline := time.Now().Add(5 * time.Second); s.Viewers() != 1; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("viewer never subscribed")
		}
	}

	full, doneFull := stream(t, srv.URL)
	defer doneFull()
	if ev := next(t, full); ev[0] != "full" {
		t.Fatalf("over the cap: %v", ev)
	}

	s.Click(pub(4))
	s.Paid(ledger.Signed{Withdrawal: ledger.Withdrawal{Address: pub(4), Amount: big.NewInt(1)}, Hash: nanocrypto.Hash{1}})
	must(t, s.Tick(ctx))
	for _, want := range []string{"click", "payout", "snapshot"} {
		if ev := next(t, r); ev[0] != want {
			t.Fatalf("got %v, want %s", ev, want)
		}
	}
}

// clicks accepted in memory beyond what the vault holds (the batch that will halt it, D20) show as empty, not
// as a negative number the page would reject
func TestRemainingNeverNegative(t *testing.T) {
	s, _, c, _ := setup(t, big.NewInt(5), 10)
	for i := 0; i < 8; i++ {
		must(t, c.Click(ctx, pub(5)))
		s.Click(pub(5))
	}
	// and an empty reserve has no drain estimate, however fast the clicking
	if got := readSnap(t, s); got.Remaining != "0" || got.Clicks != "8" || got.DrainYears != nil || got.UniverseX != nil {
		t.Fatalf("%+v", got)
	}
}

func TestRateTenths(t *testing.T) {
	s, _, _, _ := setup(t, big.NewInt(1000), 10)
	for i := 0; i < 129; i++ { // 2.15 clicks/s
		s.Click(pub(6))
	}
	if got := readSnap(t, s); got.Rate != "2.1" {
		t.Fatalf("rate %s, want 2.1", got.Rate)
	}
}

// the real loop: an update reaches a viewer within a tick, and Run ends with its context
func TestRun(t *testing.T) {
	s, _, _, _ := setup(t, big.NewInt(1000), 10)
	ch := make(chan []byte, viewerBuffer)
	s.viewers[ch] = struct{}{}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- s.Run(runCtx) }()
	select {
	case b := <-ch:
		if ev := events(t, b); ev[len(ev)-1][0] != "snapshot" {
			t.Fatalf("tick sent %v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no update within a tick")
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatal(err)
	}
}

// shutdown ends open streams instead of waiting on them, and turns new viewers away
func TestClose(t *testing.T) {
	s, _, _, _ := setup(t, big.NewInt(1000), 10)
	srv := httptest.NewServer(s)
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	must(t, err)
	defer resp.Body.Close()
	for deadline := time.Now().Add(5 * time.Second); s.Viewers() != 1; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("viewer never subscribed")
		}
	}
	s.Close()
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatalf("stream did not end cleanly: %v", err)
	}
	again, err := http.Get(srv.URL)
	must(t, err)
	again.Body.Close()
	if again.StatusCode != http.StatusServiceUnavailable || s.Viewers() != 0 {
		t.Fatalf("after close: %d, %d viewers", again.StatusCode, s.Viewers())
	}
}

// D97: the budget rides on every tenth update only, carries the allocation, use and queue, and a new viewer
// gets the latest one straight away
func TestBudgetEvent(t *testing.T) {
	s, l, c, _ := setup(t, big.NewInt(1000), 10)
	dir := t.TempDir()
	for name, content := range map[string]string{"cpu.max": "50000 100000", "cpu.stat": "usage_usec 0", "memory.current": "33554432", "memory.max": "268435456"} {
		must(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
	}
	s.Budget(sysinfo.New(dir))
	must(t, c.Click(ctx, pub(7)))
	must(t, c.Flush(ctx))
	if _, _, err := l.Withdraw(ctx, pub(7)); err != nil {
		t.Fatal(err)
	}
	ch := make(chan []byte, 32)
	s.viewers[ch] = struct{}{}
	var withBudget []int
	for tick := 1; tick <= 21; tick++ {
		must(t, s.Tick(ctx))
		for _, ev := range events(t, <-ch) {
			if ev[0] == "budget" {
				withBudget = append(withBudget, tick)
				var b struct {
					CPUMilli int64 `json:"cpu_milli"`
					MemMB    int64 `json:"mem_mb"`
					MemMaxMB int64 `json:"mem_max_mb"`
					Queue    int64
				}
				must(t, json.Unmarshal([]byte(ev[1]), &b))
				if b.CPUMilli != 500 || b.MemMB != 32 || b.MemMaxMB != 256 || b.Queue != 1 {
					t.Fatalf("budget %s", ev[1])
				}
			}
		}
	}
	if fmt.Sprint(withBudget) != "[1 11 21]" {
		t.Fatalf("budget sent on ticks %v, want every tenth from the first", withBudget)
	}

	srv := httptest.NewServer(s)
	defer srv.Close()
	r, done := stream(t, srv.URL)
	defer done()
	if first, second := next(t, r), next(t, r); first[0] != "snapshot" || second[0] != "budget" {
		t.Fatalf("on connect: %v then %v", first, second)
	}
}

// D104: a viewer is told the vault's address and when the last payout was, once, on connecting; the snapshot
// counts the people watching, and carries the bot notice only while its file exists
func TestVaultHereAndNotice(t *testing.T) {
	s, _, _, clock := setup(t, big.NewInt(1000), 5)
	s.SetVault("nano_vaultaddress")
	notice := filepath.Join(t.TempDir(), "under-attack")
	s.NoticeFile(notice)
	s.Paid(ledger.Signed{Withdrawal: ledger.Withdrawal{Address: pub(4), Amount: big.NewInt(1)}, Hash: nanocrypto.Hash{1}})
	srv := httptest.NewServer(s)
	defer srv.Close()
	must(t, s.Tick(ctx))

	r, done := stream(t, srv.URL)
	defer done()
	if ev := next(t, r); ev[0] != "snapshot" {
		t.Fatalf("first %v", ev)
	}
	ev := until(t, r, "vault")
	var v struct {
		A        string
		LastPaid int64 `json:"last_paid"`
	}
	must(t, json.Unmarshal([]byte(ev[1]), &v))
	if ev[0] != "vault" || v.A != "nano_vaultaddress" || v.LastPaid != clock.UnixMilli() {
		t.Fatalf("vault event %v", ev)
	}
	for deadline := time.Now().Add(5 * time.Second); s.Viewers() != 1; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("viewer never subscribed")
		}
	}

	var got struct {
		Here   int
		Notice *bool
	}
	b, err := s.snapshot(ctx)
	must(t, err)
	must(t, json.Unmarshal(b, &got))
	if got.Here != 1 || got.Notice != nil {
		t.Fatalf("snapshot %s", b)
	}
	must(t, os.WriteFile(notice, nil, 0o600))
	b, _ = s.snapshot(ctx)
	got.Notice = nil
	must(t, json.Unmarshal(b, &got))
	if got.Notice == nil || !*got.Notice {
		t.Fatalf("notice file present, snapshot %s", b)
	}
}

// D104: past the stream cap, the page polls the same snapshot as plain JSON
func TestServeSnapshot(t *testing.T) {
	s, _, _, _ := setup(t, big.NewInt(1000), 5)
	w := httptest.NewRecorder()
	s.ServeSnapshot(w, httptest.NewRequest("GET", "/snapshot", nil))
	if w.Code != 503 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("before the first tick: %d %v", w.Code, w.Header())
	}
	must(t, s.Tick(ctx))
	w = httptest.NewRecorder()
	s.ServeSnapshot(w, httptest.NewRequest("GET", "/snapshot", nil))
	var got snap
	must(t, json.Unmarshal(w.Body.Bytes(), &got))
	if w.Code != 200 || got.Remaining != "1000" || w.Header().Get("Cache-Control") != "public, max-age=10" ||
		w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("%d %v %s", w.Code, w.Header(), w.Body)
	}
}

// D104: the board holds the ten most-paid addresses, exactly, truncated; it updates when a payout moves it and
// a new viewer receives it on connecting
func TestLeaders(t *testing.T) {
	s, l, _, _ := setup(t, big.NewInt(1_000_000), 5)
	payTo := func(i byte, n uint64, h byte) {
		must(t, l.Credit(ctx, map[nanocrypto.PublicKey]uint64{pub(i): n}))
		if _, _, err := l.Withdraw(ctx, pub(i)); err != nil {
			t.Fatal(err)
		}
		w, err := l.Next(ctx)
		must(t, err)
		must(t, l.MarkSigned(ctx, w, nanocrypto.Hash{h, i}, []byte(`{}`)))
		must(t, l.MarkConfirmed(ctx, w.ID))
		s.Paid(ledger.Signed{Withdrawal: ledger.Withdrawal{ID: w.ID, Address: pub(i), Amount: new(big.Int).SetUint64(n)}})
	}
	board := func() []struct{ A, Raw string } {
		t.Helper()
		ev := events(t, s.leaders)
		var rows []struct{ A, Raw string }
		must(t, json.Unmarshal([]byte(ev[0][1]), &rows))
		return rows
	}
	// before the service starts, two addresses were already paid
	payTo(1, 50, 1)
	payTo(2, 70, 2)
	s.paidTo = nil
	must(t, s.LoadLeaders(ctx))
	if b := board(); len(b) != 2 || b[0].Raw != "70" || b[1].Raw != "50" || !strings.Contains(b[0].A, "…") {
		t.Fatalf("loaded board %v", b)
	}
	// twelve more addresses, and address 1 paid again to overtake: the board keeps the top ten in order
	for i := byte(3); i <= 14; i++ {
		payTo(i, uint64(i), 3)
	}
	payTo(1, 40, 4) // 90 in all
	must(t, s.Tick(ctx))
	b := board()
	if len(b) != leaderPlaces || b[0].Raw != "90" || b[1].Raw != "70" || b[9].Raw != "7" {
		t.Fatalf("board after payouts %v", b)
	}
	a1 := pub(1).Address()
	if b[0].A != a1[:10]+"…"+a1[len(a1)-6:] {
		t.Fatalf("first place %s, want address 1 truncated", b[0].A)
	}
	// a tick with no payout sends no leaders event
	srv := httptest.NewServer(s)
	defer srv.Close()
	r, done := stream(t, srv.URL)
	defer done()
	if ev := next(t, r); ev[0] != "snapshot" {
		t.Fatalf("first %v", ev)
	}
	until(t, r, "leaders")
	until(t, r, "recent") // the connect-time events end with the recent payouts (no vault set here)
	for deadline := time.Now().Add(5 * time.Second); s.Viewers() != 1; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("viewer never subscribed")
		}
	}
	must(t, s.Tick(ctx))
	if ev := next(t, r); ev[0] != "snapshot" {
		t.Fatalf("quiet tick sent %v", ev)
	}
}

// a new viewer gets the latest payouts at once, at most eight, oldest first, as their own event
func TestRecentPayouts(t *testing.T) {
	s, _, _, _ := setup(t, big.NewInt(1000), 5)
	for i := byte(1); i <= 10; i++ {
		s.Paid(ledger.Signed{Withdrawal: ledger.Withdrawal{Address: pub(i), Amount: big.NewInt(int64(i))}, Hash: nanocrypto.Hash{i}})
	}
	srv := httptest.NewServer(s)
	defer srv.Close()
	must(t, s.Tick(ctx))
	r, done := stream(t, srv.URL)
	defer done()
	next(t, r) // snapshot
	ev := next(t, r)
	var rows []struct{ A, Raw string }
	must(t, json.Unmarshal([]byte(ev[1]), &rows))
	if ev[0] != "recent" || len(rows) != 8 || rows[0].Raw != "3" || rows[7].Raw != "10" || rows[7].A != pub(10).Address() {
		t.Fatalf("%s %v", ev[0], rows)
	}
}

// until reads events up to and including the first named one
func until(t *testing.T, r *bufio.Reader, name string) [2]string {
	t.Helper()
	for i := 0; i < 10; i++ {
		if ev := next(t, r); ev[0] == name {
			return ev
		}
	}
	t.Fatalf("no %s event", name)
	return [2]string{}
}
