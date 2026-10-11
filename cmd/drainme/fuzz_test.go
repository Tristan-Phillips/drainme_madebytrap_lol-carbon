package main

import (
	"context"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/api"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ingest"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/site"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/stats"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/web"
)

// the answers the server is allowed to give; anything else, or a panic, is a finding
var allowed = map[int]bool{200: true, 204: true, 301: true, 304: true, 307: true, 308: true, 400: true, 404: true, 405: true, 409: true, 503: true}

// FuzzRoutes throws arbitrary methods, paths, queries and headers at the exact route table serve uses.
func FuzzRoutes(f *testing.F) {
	l, err := ledger.Open(filepath.Join(f.TempDir(), "ledger.db"), 10)
	if err != nil {
		f.Fatal(err)
	}
	defer l.Close()
	ctx := context.Background()
	l.SetHead(ctx, nanocrypto.Hash{1}, big.NewInt(1_000_000))
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c, err := ingest.New(ctx, l, 1000, log)
	if err != nil {
		f.Fatal(err)
	}
	st := stats.New(l, c, 10, log)
	page, err := site.New(web.Files)
	if err != nil {
		f.Fatal(err)
	}
	h := routes(page, api.New(l, c, st, func() {}, func(int64) time.Duration { return 0 }, nanocrypto.PublicKey{}, log), st, st.ServeSnapshot)
	good := nanocrypto.DeriveKey([32]byte{0xf2}, 0).Public().Address()

	for _, seed := range []struct{ method, target, header string }{
		{"POST", "/click/" + good, ""}, {"POST", "/withdraw/" + good + "?to=x", ""}, {"GET", "/", "If-None-Match"},
		{"GET", "/js/main.js", "Accept-Encoding"}, {"GET", "/healthz", ""}, {"DELETE", "/click/" + good, ""},
		{"POST", "/click/%3Cscript%3E", ""}, {"GET", "/../../etc/passwd", ""}, {"HEAD", "/css/style.css", "Range"},
	} {
		f.Add(seed.method, seed.target, seed.header, "x")
	}
	f.Fuzz(func(t *testing.T, method, target, header, value string) {
		if !strings.HasPrefix(target, "/") || method == "" || strings.ContainsAny(method, " \r\n\t/") {
			return // not a request line any server would parse; net/http rejects these before routing
		}
		if strings.HasPrefix(target, "/events") {
			return // a stream never ends on its own; it has its own tests
		}
		req, err := http.NewRequest(method, "http://drainme.test"+target, strings.NewReader(value))
		if err != nil {
			return
		}
		if header != "" && !strings.ContainsAny(header, " :\r\n\x00") {
			req.Header.Set(header, value)
		}
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req.WithContext(ctx))
		if !allowed[w.Code] {
			t.Fatalf("%s %q answered %d", method, target, w.Code)
		}
		// nothing from the request is ever reflected into an error reply
		if w.Code >= 400 && len(target) > 12 && strings.Contains(w.Body.String(), target) {
			t.Fatalf("%s %q reflected the path: %q", method, target, w.Body.String())
		}
	})
}
