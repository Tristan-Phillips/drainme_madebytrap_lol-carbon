// Command stress loads a drainme server the way a crowd, or someone trying to break it, would, and reports what
// came back. Point it at a local stack (go test ./cmd/drainme -run TestLocal -local ...). Against the real site
// it refuses unless told -i-own-this, and then runs gently (D104): at most 50 requests a second for at most a
// minute, page loads, streams, or clicks for an address you own (-addr). It never floods new addresses there:
// each would be owed a raw that the idle sweep later pays to an address nobody holds.
//
//	go run ./tools/stress -url http://127.0.0.1:8740 -mode click -c 200 -d 20s
//	go run ./tools/stress -url https://drainme.madebytrap.lol -i-own-this -mode click -addr nano_... -rate 20 -d 30s
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

func main() {
	base := flag.String("url", "http://127.0.0.1:8740", "server to load; never production")
	mode := flag.String("mode", "click", "click | clickuniq | withdraw | page | sse | abuse")
	conc := flag.Int("c", 100, "concurrent clients (sse: open streams)")
	dur := flag.Duration("d", 15*time.Second, "how long")
	own := flag.Bool("i-own-this", false, "allow the live site, gently")
	rate := flag.Int("rate", 0, "requests per second in all, 0 for as fast as possible (live site: at most 50)")
	mine := flag.String("addr", "", "the address clicks are for (required on the live site)")
	flag.Parse()
	if strings.Contains(*base, "madebytrap.lol") {
		if err := gentle(*own, *mode, mine, rate, dur, conc); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "live site: %s for %s at %d requests/s in all\n", *mode, *dur, *rate)
	} else if *mine != "" {
		if _, err := nanocrypto.ParseAddress(*mine); err != nil {
			fmt.Fprintln(os.Stderr, "-addr is not a valid nano_ address")
			os.Exit(2)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()
	tr := &http.Transport{MaxIdleConns: *conc, MaxIdleConnsPerHost: *conc, MaxConnsPerHost: *conc, DisableCompression: true}
	client := &http.Client{Transport: tr, Timeout: 30 * time.Second}
	r := &results{status: map[string]int64{}}
	start := time.Now()
	var pace <-chan time.Time // nil: no limit
	if *rate > 0 {
		t := time.NewTicker(time.Second / time.Duration(*rate))
		defer t.Stop()
		pace = t.C
	}

	switch *mode {
	case "sse":
		streams(ctx, *base, *conc, r)
	case "abuse":
		abuse(ctx, *base, *conc, r)
	default:
		var wg sync.WaitGroup
		for i := 0; i < *conc; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				fixed := addr()
				if *mine != "" {
					fixed = *mine
				}
				for ctx.Err() == nil {
					if pace != nil {
						select {
						case <-pace:
						case <-ctx.Done():
							return
						}
					}
					var req *http.Request
					switch *mode {
					case "click":
						req, _ = http.NewRequest("POST", *base+"/click/"+fixed, nil)
					case "clickuniq": // every request a brand-new address: the disk and memory path
						req, _ = http.NewRequest("POST", *base+"/click/"+addr(), nil)
					case "withdraw": // click then cash out, over and over: the payout queue path
						a := addr()
						r.do(client, mustReq("POST", *base+"/click/"+a))
						req, _ = http.NewRequest("POST", *base+"/withdraw/"+a, nil)
					case "page":
						req, _ = http.NewRequest("GET", *base+[]string{"/", "/css/style.css", "/js/main.js"}[i%3], nil)
						req.Header.Set("Accept-Encoding", "gzip")
					default:
						fmt.Fprintln(os.Stderr, "unknown mode")
						os.Exit(2)
					}
					r.do(client, req)
				}
			}(i)
		}
		wg.Wait()
	}
	r.report(time.Since(start))
}

// gentle is what the live site allows: only with own set, only read-only or own-address modes, and capped
func gentle(own bool, mode string, mine *string, rate *int, dur *time.Duration, conc *int) error {
	switch {
	case !own:
		return errors.New("refusing to load the live site without -i-own-this; run a local stack")
	case mode != "click" && mode != "page" && mode != "sse":
		return fmt.Errorf("mode %q is local only: on the live site it would create debts to addresses nobody holds, or attack the tunnel", mode)
	case mode == "click" && *mine == "":
		return errors.New("clicks on the live site need -addr, an address you own")
	}
	if mode == "click" {
		if _, err := nanocrypto.ParseAddress(*mine); err != nil {
			return errors.New("-addr is not a valid nano_ address")
		}
	}
	if *rate <= 0 || *rate > 50 {
		*rate = 50
	}
	*dur = min(*dur, time.Minute)
	*conc = min(*conc, 20)
	return nil
}

func mustReq(method, url string) *http.Request {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		panic(err)
	}
	return req
}

func addr() string {
	var p nanocrypto.PublicKey
	rand.Read(p[:])
	return p.Address()
}

type results struct {
	mu      sync.Mutex
	status  map[string]int64
	latency []time.Duration
	events  atomic.Int64
}

func (r *results) do(c *http.Client, req *http.Request) {
	t := time.Now()
	resp, err := c.Do(req)
	d := time.Since(t)
	key := "error"
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		key = fmt.Sprint(resp.StatusCode)
	}
	r.mu.Lock()
	r.status[key]++
	if len(r.latency) < 5_000_000 {
		r.latency = append(r.latency, d)
	}
	r.mu.Unlock()
}

func (r *results) report(elapsed time.Duration) {
	var total int64
	keys := make([]string, 0, len(r.status))
	for k, n := range r.status {
		total += n
		keys = append(keys, k)
	}
	slices.Sort(keys)
	fmt.Printf("requests %d in %s = %d/s\n", total, elapsed.Round(time.Millisecond), total*int64(time.Second)/int64(elapsed))
	for _, k := range keys {
		fmt.Printf("  %s: %d\n", k, r.status[k])
	}
	if len(r.latency) > 0 {
		slices.Sort(r.latency)
		at := func(per int) time.Duration { return r.latency[len(r.latency)*per/1000] }
		fmt.Printf("latency p50 %s  p90 %s  p99 %s  p99.9 %s  max %s\n", at(500), at(900), at(990), at(999), r.latency[len(r.latency)-1])
	}
	if n := r.events.Load(); n > 0 {
		fmt.Printf("stream events received %d\n", n)
	}
}

// streams opens n live streams and holds them, counting events, until the context ends
func streams(ctx context.Context, base string, n int, r *results) {
	var open, full, failed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(ctx, "GET", base+"/events", nil)
			resp, err := (&http.Transport{DisableCompression: true}).RoundTrip(req)
			if err != nil {
				failed.Add(1)
				return
			}
			defer resp.Body.Close()
			open.Add(1)
			defer open.Add(-1)
			sc := bufio.NewScanner(resp.Body)
			for sc.Scan() {
				if line := sc.Text(); strings.HasPrefix(line, "event: ") {
					if line == "event: full" {
						full.Add(1)
					}
					r.events.Add(1)
				}
			}
		}()
		if i%100 == 99 {
			time.Sleep(20 * time.Millisecond) // a crowd arrives fast, but not in one instant
		}
	}
	<-ctx.Done()
	fmt.Printf("streams: %d open at the end, %d told full, %d failed to connect\n", open.Load(), full.Load(), failed.Load())
	wg.Wait()
}

// abuse sends what someone trying to break a server sends, and counts how each kind was answered
func abuse(ctx context.Context, base string, n int, r *results) {
	host := strings.TrimPrefix(base, "http://")
	raw := func(payload string, slow bool) string {
		conn, err := net.DialTimeout("tcp", host, 5*time.Second)
		if err != nil {
			return "dial error"
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(20 * time.Second))
		if slow { // slowloris: a byte at a time, never finishing the headers
			for _, b := range []byte(payload) {
				if _, err := conn.Write([]byte{b}); err != nil {
					return "closed by server while trickling"
				}
				time.Sleep(500 * time.Millisecond)
			}
		} else {
			conn.Write([]byte(payload))
		}
		line, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			return "no reply (" + strings.SplitN(err.Error(), ":", 2)[0] + ")"
		}
		return strings.TrimSpace(line)
	}
	a := addr()
	cases := map[string]func() string{
		"huge header": func() string {
			return raw("GET / HTTP/1.1\r\nHost: x\r\nX-Pad: "+strings.Repeat("a", 64<<10)+"\r\n\r\n", false)
		},
		"slowloris": func() string {
			return raw("GET / HTTP/1.1\r\nHost: x\r\nX-A: "+strings.Repeat("b", 60)+"\r\n\r\n", true)
		},
		"big body on click": func() string {
			return raw("POST /click/"+a+" HTTP/1.1\r\nHost: x\r\nContent-Length: 10485760\r\n\r\n"+strings.Repeat("c", 1<<20), false)
		},
		"path traversal":    func() string { return raw("GET /../../etc/passwd HTTP/1.1\r\nHost: x\r\n\r\n", false) },
		"encoded traversal": func() string { return raw("GET /%2e%2e/%2e%2e/etc/passwd HTTP/1.1\r\nHost: x\r\n\r\n", false) },
		"nul in path":       func() string { return raw("GET /click/%00 HTTP/1.1\r\nHost: x\r\n\r\n", false) },
		"script in address": func() string {
			return raw("POST /click/%3Cscript%3E HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n", false)
		},
		"semicolon query": func() string {
			return raw("POST /click/"+a+"?a=1;b=2 HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n", false)
		},
		"TRACE":    func() string { return raw("TRACE / HTTP/1.1\r\nHost: x\r\n\r\n", false) },
		"CONNECT":  func() string { return raw("CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n", false) },
		"garbage":  func() string { return raw("\x16\x03\x01\x02\x00\x01\x00\x01\xfc\x03\x03 not http\r\n\r\n", false) },
		"HTTP/0.9": func() string { return raw("GET /\r\n\r\n", false) },
		"request smuggling": func() string {
			return raw("POST /click/"+a+" HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", false)
		},
		"withdraw by GET": func() string { return raw("GET /withdraw/"+a+" HTTP/1.1\r\nHost: x\r\n\r\n", false) },
		"destination param": func() string {
			return raw("POST /withdraw/"+a+"?to="+addr()+" HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n", false)
		},
	}
	names := make([]string, 0, len(cases))
	for name := range cases {
		names = append(names, name)
	}
	slices.Sort(names)
	var wg sync.WaitGroup
	answers := make([]string, len(names))
	for i, name := range names {
		wg.Add(1)
		go func() { defer wg.Done(); answers[i] = cases[name]() }()
	}
	wg.Wait()
	for i, name := range names {
		fmt.Printf("  %-18s %s\n", name, answers[i])
	}
	// and a connection flood: n sockets opened and left idle
	var idle []net.Conn
	for i := 0; i < n && ctx.Err() == nil; i++ {
		if c, err := net.DialTimeout("tcp", host, 2*time.Second); err == nil {
			idle = append(idle, c)
		}
	}
	fmt.Printf("  idle sockets held: %d\n", len(idle))
	for _, c := range idle {
		c.Close()
	}
}
