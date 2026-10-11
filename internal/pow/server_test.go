package pow

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanoclient"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// the server is a work endpoint nanoclient accepts as is, and its answers verify
func TestServerWithClient(t *testing.T) {
	srv := httptest.NewServer(Handler(2, quiet()))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	c := nanoclient.New(nil, []*url.URL{u}, 10*time.Second)
	root := nanocrypto.Hash{0xd7, 0x0f}
	w, err := c.Work(context.Background(), root, 0xfff0000000000000)
	if err != nil || nanocrypto.Difficulty(root, w) < 0xfff0000000000000 {
		t.Fatalf("work %016x: %v", w, err)
	}
}

func TestServerRefuses(t *testing.T) {
	srv := httptest.NewServer(Handler(1, quiet()))
	defer srv.Close()
	hash := strings.Repeat("AB", 32)
	for name, body := range map[string]string{
		"other action":     `{"action":"account_info","account":"x"}`,
		"lowercase hash":   `{"action":"work_generate","hash":"` + strings.ToLower(hash) + `","difficulty":"fff0000000000000"}`,
		"short difficulty": `{"action":"work_generate","hash":"` + hash + `","difficulty":"fff"}`,
		"zero difficulty":  `{"action":"work_generate","hash":"` + hash + `","difficulty":"0000000000000000"}`,
		"not json":         `work please`,
		"oversized":        `{"action":"work_generate","pad":"` + strings.Repeat("a", 2<<10) + `"}`,
	} {
		resp, err := http.Post(srv.URL+"/", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: %d", name, resp.StatusCode)
		}
	}
	if resp, _ := http.Get(srv.URL + "/"); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET /: %d", resp.StatusCode)
	}
	if resp, _ := http.Get(srv.URL + "/healthz"); resp.StatusCode != http.StatusNoContent {
		t.Errorf("healthz: %d", resp.StatusCode)
	}
}

// one search at a time; a second caller is told busy at once, and a caller that gives up frees the server
func TestServerOneAtATime(t *testing.T) {
	srv := httptest.NewServer(Handler(1, quiet()))
	defer srv.Close()
	impossible := `{"action":"work_generate","hash":"` + strings.Repeat("CD", 32) + `","difficulty":"ffffffffffffffff"}`
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		req, _ := http.NewRequestWithContext(ctx, "POST", srv.URL+"/", strings.NewReader(impossible))
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	time.Sleep(100 * time.Millisecond)
	resp, err := http.Post(srv.URL+"/", "application/json", strings.NewReader(impossible))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second caller: %d, want 503", resp.StatusCode)
	}
	cancel()
	wg.Wait()
	easy := `{"action":"work_generate","hash":"` + strings.Repeat("CD", 32) + `","difficulty":"f000000000000000"}`
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		resp, err := http.Post(srv.URL+"/", "application/json", strings.NewReader(easy))
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the abandoned search never let go")
		}
	}
}
