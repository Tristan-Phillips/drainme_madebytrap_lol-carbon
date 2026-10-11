package egress

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func upstream(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
	t.Cleanup(srv.Close)
	return srv
}

// client goes through the proxy and trusts the test upstreams' certificate
func client(t *testing.T, proxy *httptest.Server, trust *httptest.Server) *http.Client {
	t.Helper()
	p, _ := url.Parse(proxy.URL)
	tlsConf := trust.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	return &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(p), TLSClientConfig: tlsConf}}
}

func proxy(t *testing.T, targets ...string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(Handler(targets, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(srv.Close)
	return srv
}

func TestTunnelsOnlyToTargets(t *testing.T) {
	allowed, other := upstream(t, "rpc says hi"), upstream(t, "should never be reached")
	host := strings.TrimPrefix(allowed.URL, "https://")
	p := proxy(t, strings.ToUpper(host)) // configured case does not matter
	c := client(t, p, allowed)

	resp, err := c.Get(allowed.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "rpc says hi" {
		t.Fatalf("through the tunnel: %q", body)
	}

	// every httptest TLS server shares one certificate, so only the proxy can be what refuses this
	if resp, err := c.Get(other.URL); err == nil {
		resp.Body.Close()
		t.Fatal("tunnel opened to a destination not on the list")
	}
}

// the same host on another port is another destination
func TestPortMatters(t *testing.T) {
	up := upstream(t, "x")
	hostport := strings.TrimPrefix(up.URL, "https://")
	host := hostport[:strings.LastIndexByte(hostport, ':')]
	p := proxy(t, host+":443")
	if resp, err := client(t, p, up).Get(up.URL); err == nil {
		resp.Body.Close()
		t.Fatal("tunnel opened to an unlisted port")
	}
}

func TestRefusesPlainRequestsAndAnswersHealth(t *testing.T) {
	p := proxy(t, "a.example:443")
	pu, _ := url.Parse(p.URL)
	plain := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(pu)}}
	resp, err := plain.Get("http://a.example/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("forwarded plain request: %d", resp.StatusCode)
	}
	resp, err = http.Get(p.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("healthz: %d", resp.StatusCode)
	}
}

// hostnames are case-insensitive: an upper-case CONNECT for a listed host passes the allow-list (and then fails
// to dial the closed port, 502) instead of being refused as unlisted (403)
func TestHostCaseIgnored(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	port := closed.URL[strings.LastIndexByte(closed.URL, ':')+1:]
	closed.Close()
	p := proxy(t, "localhost:"+port)
	conn, err := net.Dial("tcp", strings.TrimPrefix(p.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "CONNECT LOCALHOST:%s HTTP/1.1\r\nHost: LOCALHOST:%s\r\n\r\n", port, port)
	status, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil || !strings.Contains(status, " 502 ") {
		t.Fatalf("status %q, %v", status, err)
	}
}
