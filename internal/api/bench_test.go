package api

import (
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ingest"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/stats"
)

// the hot path: one click, as the server sees it, without the network
func BenchmarkClick(b *testing.B) {
	l, _ := ledger.Open(filepath.Join(b.TempDir(), "l.db"), 10)
	defer l.Close()
	l.SetHead(ctx, nanocrypto.Hash{1}, new(big.Int).Lsh(big.NewInt(1), 100))
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c, _ := ingest.New(ctx, l, 100, log)
	h := New(l, c, stats.New(l, c, 10, log), func() {}, func(int64) time.Duration { return 0 }, nanocrypto.PublicKey{}, log)
	path := "/click/" + pub(1).Address()
	b.ReportAllocs()
	for b.Loop() {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, nil))
		if w.Code != http.StatusNoContent {
			b.Fatal(w.Code)
		}
	}
}

func BenchmarkParseAddress(b *testing.B) {
	a := pub(1).Address()
	b.ReportAllocs()
	for b.Loop() {
		nanocrypto.ParseAddress(a)
	}
}
