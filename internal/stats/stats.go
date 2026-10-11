// Package stats turns the ledger and the click stream into what the page shows (D21, D35–D37) and serves it as
// one shared Server-Sent Events stream (D33, D34). Every number is an exact integer; the drain estimate is
// integer division formatted as scientific notation, never a float (D86 R3).
package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ingest"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/sysinfo"
)

const (
	window        = 60 // seconds in the rate window (D36)
	samplesPerSec = 10 // click events on the wire per second at most (D35)
	// each viewer may fall this many seconds behind before it is dropped (D33); every second is one write
	viewerBuffer = 8
	// a write that cannot finish in this long means a stuck client, which is dropped
	writeTimeout = 10 * time.Second
	// the budget readout changes slowly, so it rides along on every tenth update rather than every one
	budgetEvery = 10
	// places on the leaderboard (D104)
	leaderPlaces = 10
	// payouts a new viewer sees at once, so "what's left me" is never empty while payouts flow; the page lists 8
	recentPayouts = 8
)

var (
	secondsPerYear = big.NewInt(31_557_600)     // Julian year, as the page's formula states
	universeYears  = big.NewInt(13_800_000_000) // age of the universe, for the comparison
)

type Stats struct {
	ledger    *ledger.Ledger
	counter   *ingest.Counter
	maxViewer int
	log       *slog.Logger
	now       func() time.Time

	clickMu sync.Mutex
	buckets [window]struct {
		sec int64
		n   uint64
	}
	samples   []byte // click events waiting for the next tick
	sampled   int    // samples taken in sampleSec
	sampleSec int64
	payouts   []byte // payout events waiting for the next tick
	halted    bool
	lastPaid  int64                             // unix ms of the latest confirmed payout, 0 for none since start
	paidTo    map[nanocrypto.PublicKey]struct{} // addresses paid since the last tick, whose totals may have moved
	recent    []payoutRow                       // the latest payouts, oldest first, for new viewers

	viewMu     sync.Mutex
	viewers    map[chan []byte]struct{}
	last       []byte // the latest snapshot event, sent first to a new viewer
	lastJSON   []byte // the same snapshot as plain JSON, for GET /snapshot
	lastBudget []byte // the latest budget event, likewise
	leaders    []byte // the leaderboard event, sent to each new viewer and again whenever it changes
	closed     bool

	vault      string // the hot wallet's address, sent once per viewer so the page can link the chain as proof
	noticeFile string // while this file exists, the page explains that Cloudflare is challenging bots (D104)

	budget *sysinfo.Reader // nil: no budget readout
	ticks  int
	top    []ledger.Taker // the leaderboard, most paid first; only Run's goroutine touches it
}

// SetVault names the hot wallet, which each viewer is told once on connecting (D104). Set it before Run.
func (s *Stats) SetVault(address string) { s.vault = address }

// NoticeFile is a path whose existence turns on the page's "bots are being challenged" notice, for while the
// operator has Cloudflare's Under Attack mode on (D104). Set it before Run.
func (s *Stats) NoticeFile(path string) { s.noticeFile = path }

// Budget turns on the container budget readout (D97). Set it before Run.
func (s *Stats) Budget(r *sysinfo.Reader) { s.budget = r }

// Close ends every open stream and refuses new ones. A stream never finishes on its own, so without this a
// graceful shutdown would wait out every viewer.
func (s *Stats) Close() {
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	s.closed = true
	for ch := range s.viewers {
		delete(s.viewers, ch)
		close(ch)
	}
}

func New(l *ledger.Ledger, c *ingest.Counter, maxViewers int, log *slog.Logger) *Stats {
	return &Stats{ledger: l, counter: c, maxViewer: maxViewers, log: log, now: time.Now, viewers: map[chan []byte]struct{}{}}
}

// Click records one accepted click for the rate and, up to the sampling limit, for the wire.
func (s *Stats) Click(pub nanocrypto.PublicKey) {
	now := s.now()
	sec := now.Unix()
	s.clickMu.Lock()
	defer s.clickMu.Unlock()
	b := &s.buckets[sec%window]
	if b.sec != sec {
		b.sec, b.n = sec, 0
	}
	b.n++
	if s.sampleSec != sec {
		s.sampleSec, s.sampled = sec, 0
	}
	if s.sampled < samplesPerSec {
		s.sampled++
		// the wire shows only a truncated address (D35), the same cut the page uses (D66)
		a := pub.Address()
		s.samples = event(s.samples, "click", struct {
			A string `json:"a"`
			T int64  `json:"t"`
		}{a[:10] + "…" + a[len(a)-6:], now.UnixMilli()})
	}
}

type payoutRow struct {
	A    string `json:"a"`
	Raw  string `json:"raw"`
	Hash string `json:"hash"`
	T    int64  `json:"t"`
}

// Paid records a confirmed send for the payout feed. Payouts are public on chain, so the address is shown whole.
func (s *Stats) Paid(sent ledger.Signed) {
	s.clickMu.Lock()
	defer s.clickMu.Unlock()
	row := payoutRow{sent.Address.Address(), sent.Amount.String(), sent.Hash.Hex(), s.now().UnixMilli()}
	s.payouts = event(s.payouts, "payout", row)
	s.recent = append(s.recent, row)
	if len(s.recent) > recentPayouts {
		s.recent = s.recent[len(s.recent)-recentPayouts:]
	}
	s.lastPaid = s.now().UnixMilli()
	if s.paidTo == nil {
		s.paidTo = map[nanocrypto.PublicKey]struct{}{}
	}
	s.paidTo[sent.Address] = struct{}{}
}

// LoadLeaders reads the leaderboard from the ledger once; after that each payout updates it (D104).
func (s *Stats) LoadLeaders(ctx context.Context) error {
	top, err := s.ledger.TopPaid(ctx, leaderPlaces)
	if err != nil {
		return err
	}
	s.top = top
	ev := s.leadersEvent()
	s.viewMu.Lock()
	s.leaders = ev
	s.viewMu.Unlock()
	return nil
}

// rank re-reads what each just-paid address has been paid in all and reports whether the board changed. One
// index walk per paid address, never a table scan.
func (s *Stats) rank(ctx context.Context, paid map[nanocrypto.PublicKey]struct{}) (bool, error) {
	changed := false
	for pub := range paid {
		total, err := s.ledger.PaidTo(ctx, pub)
		if err != nil {
			return changed, err
		}
		i := slices.IndexFunc(s.top, func(t ledger.Taker) bool { return t.Address == pub })
		if i >= 0 && s.top[i].Paid.Cmp(total) == 0 {
			continue
		}
		if i >= 0 {
			s.top = slices.Delete(s.top, i, i+1)
			changed = true
		}
		if total.Sign() == 0 {
			continue // nothing paid after all: never on the board
		}
		changed = true
		s.top = append(s.top, ledger.Taker{Address: pub, Paid: total})
		slices.SortStableFunc(s.top, func(a, b ledger.Taker) int {
			if c := b.Paid.Cmp(a.Paid); c != 0 {
				return c
			}
			return cmpAddr(a.Address, b.Address)
		})
		if len(s.top) > leaderPlaces {
			// an address that falls off is still ranked: it rejoins when it is next paid
			s.top = s.top[:leaderPlaces]
		}
	}
	return changed, nil
}

func cmpAddr(a, b nanocrypto.PublicKey) int {
	switch x, y := a.Address(), b.Address(); {
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}

// leadersEvent shows truncated addresses, as the wire does (D35), with exact totals
func (s *Stats) leadersEvent() []byte {
	type row struct {
		A   string `json:"a"`
		Raw string `json:"raw"`
	}
	rows := make([]row, 0, len(s.top))
	for _, t := range s.top {
		a := t.Address.Address()
		rows = append(rows, row{a[:10] + "…" + a[len(a)-6:], t.Paid.String()})
	}
	return event(nil, "leaders", rows)
}

// SetHalted shows or clears the "payouts paused, funds intact" banner (D64).
func (s *Stats) SetHalted(halted bool) {
	s.clickMu.Lock()
	defer s.clickMu.Unlock()
	s.halted = halted
}

// Run sends one shared update a second until ctx ends.
func (s *Stats) Run(ctx context.Context) error {
	if err := s.LoadLeaders(ctx); err != nil {
		s.log.Warn("stats: leaderboard starts empty", "err", err)
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
		if err := s.Tick(ctx); err != nil {
			s.log.Warn("stats: snapshot skipped", "err", err)
		}
	}
}

// Tick builds this second's update, every event in one write, and sends the same bytes to every viewer.
func (s *Stats) Tick(ctx context.Context) error {
	data, err := s.snapshot(ctx)
	if err != nil {
		return err
	}
	snap := fmt.Appendf(nil, "event: snapshot\ndata: %s\n\n", data)
	var budget []byte
	if s.ticks++; s.budget != nil && s.ticks%budgetEvery == 1 {
		if budget, err = s.budgetEvent(ctx); err != nil {
			return err
		}
	}
	s.clickMu.Lock()
	out := append(append(append(s.samples, s.payouts...), snap...), budget...)
	s.samples, s.payouts = nil, nil
	paid := s.paidTo
	s.paidTo = nil
	s.clickMu.Unlock()
	var leaders []byte
	if len(paid) > 0 {
		changed, err := s.rank(ctx, paid)
		if err != nil {
			s.log.Warn("stats: leaderboard not updated", "err", err)
		} else if changed {
			leaders = s.leadersEvent()
			out = append(out, leaders...)
		}
	}

	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	s.last, s.lastJSON = snap, data
	if leaders != nil {
		s.leaders = leaders
	}
	if budget != nil {
		s.lastBudget = budget
	}
	for ch := range s.viewers {
		select {
		case ch <- out:
		default: // too far behind (D33): drop it, and the page reconnects
			delete(s.viewers, ch)
			close(ch)
		}
	}
	return nil
}

func (s *Stats) snapshot(ctx context.Context) ([]byte, error) {
	chain, available, credited, err := s.ledger.Totals(ctx)
	if err != nil {
		return nil, err
	}
	// clicks still waiting for the next flush already count, so the number moves every second
	pending := new(big.Int).SetUint64(s.counter.Pending())
	remaining := new(big.Int).Sub(available, pending)
	if remaining.Sign() < 0 {
		remaining.SetInt64(0)
	}
	owed := new(big.Int).Sub(chain, available)

	sec := s.now().Unix()
	s.clickMu.Lock()
	var inWindow uint64
	for _, b := range s.buckets {
		if sec-b.sec < window {
			inWindow += b.n
		}
	}
	halted := s.halted
	s.clickMu.Unlock()
	notice := false
	if s.noticeFile != "" {
		_, err := os.Stat(s.noticeFile)
		notice = err == nil
	}

	snap := struct {
		Remaining  string      `json:"remaining"`
		Chain      string      `json:"chain"`
		Unpaid     string      `json:"unpaid"`
		Clicks     string      `json:"clicks"`
		Rate       json.Number `json:"rate"`
		DrainYears *string     `json:"drain_years"`
		UniverseX  *string     `json:"universe_x"`
		Halted     bool        `json:"halted"`
		Here       int         `json:"here"`             // open streams: people watching now, counted, never identified
		Notice     bool        `json:"notice,omitempty"` // absent unless on, so it costs nothing normally
	}{
		Remaining: remaining.String(),
		Chain:     chain.String(),
		Unpaid:    owed.Add(owed, pending).String(),
		Clicks:    credited.Add(credited, pending).String(),
		// clicks per second to one decimal place, from integers: tenths = clicks in window × 10 / window
		Rate:   json.Number(fmt.Sprintf("%d.%d", inWindow/window, inWindow*10/window%10)),
		Halted: halted,
		Here:   s.Viewers(),
		Notice: notice,
	}
	if inWindow > 0 && remaining.Sign() > 0 {
		// years = remaining ÷ (clicks/s) ÷ seconds per year = remaining × window ÷ (clicks in window × seconds per year)
		num := new(big.Int).Mul(remaining, big.NewInt(window))
		den := new(big.Int).Mul(new(big.Int).SetUint64(inWindow), secondsPerYear)
		years := sci(num, den)
		universe := sci(num, den.Mul(den, universeYears))
		snap.DrainYears, snap.UniverseX = &years, &universe
	}
	return json.Marshal(snap)
}

// budgetEvent is what the page shows to separate the VPS budget from Nano: the container's allocation and use,
// and how many payouts are in line. Integers only; -1 is unknown, and a limit of 0 is unlimited.
func (s *Stats) budgetEvent(ctx context.Context) ([]byte, error) {
	b, ok := s.budget.Read()
	if !ok {
		return nil, nil
	}
	queue, err := s.ledger.QueueLength(ctx)
	if err != nil {
		return nil, err
	}
	mb := func(n int64) int64 {
		if n <= 0 {
			return n
		}
		return n >> 20
	}
	return event(nil, "budget", struct {
		CPUMilli int64 `json:"cpu_milli"`
		CPUPct   int64 `json:"cpu_pct"`
		MemMB    int64 `json:"mem_mb"`
		MemMaxMB int64 `json:"mem_max_mb"`
		Queue    int64 `json:"queue"`
	}{b.CPUMilli, b.CPUPct, mb(b.MemBytes), mb(b.MemMax), queue}), nil
}

// sci writes num/den as "6.89e19": three significant digits, truncated, with a signed exponent.
func sci(num, den *big.Int) string {
	const extra = 30 // digits of headroom, so quotients far below 1 still have three significant digits
	q := new(big.Int).Mul(num, new(big.Int).Exp(big.NewInt(10), big.NewInt(extra), nil))
	d := q.Quo(q, den).String()
	if d == "0" {
		return "0"
	}
	return fmt.Sprintf("%s.%se%d", d[:1], (d + "00")[1:3], len(d)-1-extra)
}

// event appends one SSE event. JSON from encoding/json holds no raw newline, so it is always one data line.
func event(buf []byte, name string, v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err) // only fixed structs of strings and integers are passed
	}
	return fmt.Appendf(buf, "event: %s\ndata: %s\n\n", name, data)
}

// ServeHTTP is GET /events. Past the viewer cap (D34) it sends "full" and closes, and the page retries slowly.
func (s *Stats) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Accel-Buffering", "no") // proxies must pass each event through, not hold the stream

	ch := make(chan []byte, viewerBuffer)
	s.viewMu.Lock()
	closed, full := s.closed, len(s.viewers) >= s.maxViewer
	if !closed && !full {
		s.viewers[ch] = struct{}{}
	}
	first := append(append(append([]byte(nil), s.last...), s.lastBudget...), s.leaders...)
	s.viewMu.Unlock()
	s.clickMu.Lock()
	if len(s.recent) > 0 {
		// a separate event from payout: the page lists these but announces nothing, and a past payout must not
		// be taken for the one a visitor is waiting on
		first = event(first, "recent", s.recent)
	}
	s.clickMu.Unlock()
	if s.vault != "" {
		s.clickMu.Lock()
		paid := s.lastPaid
		s.clickMu.Unlock()
		first = event(first, "vault", struct {
			A        string `json:"a"`
			LastPaid int64  `json:"last_paid,omitempty"`
		}{s.vault, paid})
	}
	if closed {
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	if full {
		w.Write([]byte("event: full\ndata: {}\n\n"))
		return
	}
	defer func() {
		s.viewMu.Lock()
		if _, ok := s.viewers[ch]; ok {
			delete(s.viewers, ch)
			close(ch)
		}
		s.viewMu.Unlock()
	}()

	rc := http.NewResponseController(w)
	send := func(b []byte) bool {
		rc.SetWriteDeadline(time.Now().Add(writeTimeout)) // the server's WriteTimeout must not apply to a stream
		_, err := w.Write(b)
		return err == nil && rc.Flush() == nil
	}
	if !send(first) {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case b, ok := <-ch:
			if !ok || !send(b) {
				return
			}
		}
	}
}

// ServeSnapshot is GET /snapshot: the latest snapshot as plain JSON, for viewers past the stream cap, who poll it
// every 30 s instead (D104). Every poller gets the same bytes, and Cloudflare may cache them briefly.
func (s *Stats) ServeSnapshot(w http.ResponseWriter, _ *http.Request) {
	s.viewMu.Lock()
	data := s.lastJSON
	s.viewMu.Unlock()
	h := w.Header()
	h.Set("X-Content-Type-Options", "nosniff")
	if data == nil {
		h.Set("Cache-Control", "no-store")
		http.Error(w, "warming up", http.StatusServiceUnavailable)
		return
	}
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "public, max-age=10")
	w.Write(data)
}

// Viewers is how many streams are open.
func (s *Stats) Viewers() int {
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	return len(s.viewers)
}
