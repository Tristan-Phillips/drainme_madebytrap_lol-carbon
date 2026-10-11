// Package api is drainme's two money routes (D31, D87). Each reads exactly one input, the address in the path:
// no body, no query, no amount (D86 R1) and no destination other than that address (R8).
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ingest"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/stats"
)

// Messages are fixed strings: nothing from the request is ever echoed back (D83 plain reasons, no injection).
const (
	msgBadAddress = "That is not a nano_ address: it needs nano_ plus 60 characters with a valid checksum."
	msgVault      = "That is the vault's own address. Use your own."
	msgNothing    = "Nothing to send yet. Take something first."
	msgQueueFull  = "The payout line is full. Your balance stays credited; try again in a bit."
	msgFull       = "No room for a new address right now. Addresses already owed keep counting."
	msgHalted     = "The vault cannot pay any more clicks, so the service has halted."
	msgStopped    = "Shutting down. Try again in a moment."
	msgInternal   = "Something broke on my side. Your balance and the vault are safe."
)

// queued is called after each successful withdrawal, to wake the payout worker; it must not block. eta estimates
// the wait for a place in line (D104). vault is refused as a click or withdrawal address: paying the vault to
// itself would only park raw out of reach, since the service never receives.
func New(l *ledger.Ledger, c *ingest.Counter, st *stats.Stats, queued func(), eta func(position int64) time.Duration, vault nanocrypto.PublicKey, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /click/{address}", func(w http.ResponseWriter, r *http.Request) {
		pub, ok := address(w, r, vault)
		if !ok {
			return
		}
		if err := c.Click(r.Context(), pub); err != nil {
			fail(w, log, err)
			return
		}
		st.Click(pub)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /withdraw/{address}", func(w http.ResponseWriter, r *http.Request) {
		pub, ok := address(w, r, vault)
		if !ok {
			return
		}
		// clicks still waiting for the next flush belong in this withdrawal
		if err := c.FlushAddress(r.Context(), pub); err != nil {
			fail(w, log, err)
			return
		}
		amount, position, err := l.Withdraw(r.Context(), pub)
		if err != nil {
			fail(w, log, err)
			return
		}
		queued()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(struct {
			Raw      string `json:"raw"`
			Position int64  `json:"position"`
			ETA      int64  `json:"eta_s"` // whole seconds, rounded up: an estimate, never a promise
		}{amount.String(), position, int64((eta(position) + time.Second - 1) / time.Second)})
	})
	return headers(mux)
}

func address(w http.ResponseWriter, r *http.Request, vault nanocrypto.PublicKey) (nanocrypto.PublicKey, bool) {
	pub, err := nanocrypto.ParseAddress(r.PathValue("address"))
	if err != nil {
		http.Error(w, msgBadAddress, http.StatusBadRequest)
		return pub, false
	}
	// compared as keys, not as strings
	if pub == vault {
		http.Error(w, msgVault, http.StatusBadRequest)
		return pub, false
	}
	return pub, true
}

func fail(w http.ResponseWriter, log *slog.Logger, err error) {
	status, msg := http.StatusServiceUnavailable, ""
	switch {
	case errors.Is(err, ledger.ErrNothingToWithdraw):
		status, msg = http.StatusConflict, msgNothing
	case errors.Is(err, ledger.ErrQueueFull):
		msg = msgQueueFull
	case errors.Is(err, ingest.ErrFull):
		msg = msgFull
	case errors.Is(err, ingest.ErrHalted), errors.Is(err, ledger.ErrVaultExhausted):
		msg = msgHalted
	case errors.Is(err, ingest.ErrStopped):
		msg = msgStopped
	default:
		log.Error("api: request failed", "err", err)
		status, msg = http.StatusInternalServerError, msgInternal
	}
	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "5")
	}
	http.Error(w, msg, status)
}

// answers are per-visitor and change every click: never cached by Cloudflare or a browser, never sniffed
func headers(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}
