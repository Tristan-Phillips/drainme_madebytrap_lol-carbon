package pow

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

// Handler is the local work server (D104): the one RPC action work_generate, answered the way a node answers it,
// so nanoclient treats it as the last work endpoint. It runs one search at a time on threads cores, and a search
// stops the moment its caller gives up. It sits on the internal network only.
func Handler(threads int, log *slog.Logger) http.Handler {
	var busy sync.Mutex
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("POST /{$}", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Action, Hash, Difficulty string }
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&req); err != nil {
			reply(w, http.StatusBadRequest, map[string]string{"error": "Unable to parse JSON"})
			return
		}
		if req.Action != "work_generate" {
			reply(w, http.StatusBadRequest, map[string]string{"error": "Unsupported RPC action"})
			return
		}
		root, err := nanocrypto.ParseHash(req.Hash)
		threshold, err2 := strconv.ParseUint(req.Difficulty, 16, 64)
		if err != nil || err2 != nil || len(req.Difficulty) != 16 || threshold == 0 {
			reply(w, http.StatusBadRequest, map[string]string{"error": "Bad hash or difficulty"})
			return
		}
		if !busy.TryLock() {
			reply(w, http.StatusServiceUnavailable, map[string]string{"error": "Busy"})
			return
		}
		defer busy.Unlock()
		start := time.Now()
		work, err := Solve(r.Context(), root, threshold, threads)
		if err != nil {
			log.Info("work: search abandoned", "root", root.Hex(), "after", time.Since(start).Round(time.Millisecond).String())
			return // the caller has gone; nobody reads a reply
		}
		log.Info("work: found", "root", root.Hex(), "took", time.Since(start).Round(time.Millisecond).String())
		reply(w, http.StatusOK, map[string]string{"work": fmt.Sprintf("%016x", work), "difficulty": req.Difficulty, "hash": root.Hex()})
	})
	return mux
}

func reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}
