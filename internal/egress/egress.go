// Package egress is drainme's only way out (D57, D93): an HTTP CONNECT proxy that opens tunnels to the configured
// RPC and work endpoints and refuses every other destination. serve runs on a network with no route to the
// internet, so the allow-list holds even if serve itself were subverted.
package egress

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	dialTimeout = 10 * time.Second
	// a tunnel lives at most this long; the RPC client simply opens a new one
	tunnelLife = 10 * time.Minute
)

// Handler tunnels CONNECT requests to the exact host:port pairs in targets. GET /healthz answers 204 for the
// container probe.
func Handler(targets []string, log *slog.Logger) http.Handler {
	allowed := map[string]bool{}
	for _, t := range targets {
		allowed[strings.ToLower(t)] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodConnect {
			http.Error(w, "CONNECT only", http.StatusMethodNotAllowed)
			return
		}
		target := strings.ToLower(r.Host)
		if !allowed[target] {
			if len(target) > 100 {
				target = target[:100]
			}
			log.Warn("egress: refused", "target", target)
			http.Error(w, "destination not allowed", http.StatusForbidden)
			return
		}
		up, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(r.Context(), "tcp", target)
		if err != nil {
			http.Error(w, "upstream unreachable", http.StatusBadGateway)
			return
		}
		defer up.Close()
		conn, buf, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		// the server's read and write deadlines would otherwise cut a healthy tunnel short
		deadline := time.Now().Add(tunnelLife)
		conn.SetDeadline(deadline)
		up.SetDeadline(deadline)
		if _, err := conn.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n")); err != nil {
			return
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			io.Copy(up, buf) // buf holds anything the client sent right after its CONNECT line
			if tcp, ok := up.(*net.TCPConn); ok {
				tcp.CloseWrite()
			}
		}()
		io.Copy(conn, up)
		conn.Close()
		up.Close()
		<-done
	})
}
