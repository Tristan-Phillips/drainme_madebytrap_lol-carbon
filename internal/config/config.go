// Package config loads drainme's settings from the environment and validates them fail-fast.
// Every problem is reported at once so a bad deploy is fixed in one pass, not one key at a time.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	keyDataDir      = "DRAINME_DATA_DIR"
	keySeedFile     = "DRAINME_SEED_FILE"
	keyRPCURLs      = "DRAINME_RPC_URLS"
	keyWorkURLs     = "DRAINME_WORK_URLS"
	keySendThresh   = "DRAINME_SEND_THRESHOLD"
	keyRecvThresh   = "DRAINME_RECEIVE_THRESHOLD"
	keyRepresentive = "DRAINME_REPRESENTATIVE"
	keyRPCTimeout   = "DRAINME_RPC_TIMEOUT"
	keyQueueMax     = "DRAINME_QUEUE_MAX"
	keyAddressMax   = "DRAINME_ADDRESS_MAX"
	keyViewerMax    = "DRAINME_SSE_MAX"
	keyIdleAfter    = "DRAINME_IDLE_AFTER"
	keyWorkTimeout  = "DRAINME_WORK_TIMEOUT"
	keySendCeiling  = "DRAINME_SEND_THRESHOLD_MAX"
	keyWorkThreads  = "DRAINME_WORK_THREADS"

	defaultRPCTimeout = 10 * time.Second
	maxRPCTimeout     = 2 * time.Minute
	// local work averages about a minute on two cores and can take three times that (D104)
	defaultWorkTimeout = 3 * time.Minute
	maxWorkTimeout     = 10 * time.Minute
	// four times the send threshold of 2026: the floor may follow the network up to here and no further (D104)
	defaultSendCeiling = 0xfffffffe00000000
	defaultWorkThreads = 2
	maxWorkThreads     = 64
	defaultQueueMax    = 10_000
	maxQueueMax        = 1_000_000
	// about 165 bytes of disk per owed address (measured), so the default spends the 5 GB trap set aside (D89)
	defaultAddressMax = 30_000_000
	maxAddressMax     = 1_000_000_000
	defaultViewerMax  = 5000
	defaultIdleAfter  = 24 * time.Hour
	maxViewerMax      = 1_000_000
)

// Config is the validated runtime configuration. It never holds the seed itself, only where to find it.
type Config struct {
	DataDir          string
	SeedFile         string
	RPCURLs          []*url.URL
	WorkURLs         []*url.URL
	SendThreshold    uint64
	ReceiveThreshold uint64
	// Representative is format-checked only when a command needs it (init-vault); empty is allowed here.
	Representative string
	RPCTimeout     time.Duration
	// WorkTimeout bounds one work_generate call, which may be the local CPU fallback (D104)
	WorkTimeout time.Duration
	// SendCeiling caps how far the send threshold may rise to follow the network's own minimum (D104)
	SendCeiling uint64
	// QueueMax bounds how many addresses can wait for a payout at once; beyond it /withdraw answers 503 (F1).
	QueueMax int
	// AddressMax bounds how many addresses may be owed money at once, which bounds the ledger's disk use (D89).
	AddressMax int
	// ViewerMax caps open live streams; past it a viewer is told "stream full" and retries later (D34).
	ViewerMax int
	// IdleAfter is how long a balance may sit without a click before it is paid out by itself (D96); 0 is off.
	IdleAfter time.Duration
}

// Load reads and validates configuration through lookup, which has os.LookupEnv's shape so tests stay hermetic.
// An empty value counts as unset.
func Load(lookup func(string) (string, bool)) (*Config, error) {
	get := func(key string) string {
		v, _ := lookup(key)
		return strings.TrimSpace(v)
	}

	var errs []error
	fail := func(key, format string, args ...any) {
		errs = append(errs, fmt.Errorf("config: %s: "+format, append([]any{key}, args...)...))
	}

	c := &Config{RPCTimeout: defaultRPCTimeout, WorkTimeout: defaultWorkTimeout, SendCeiling: defaultSendCeiling,
		QueueMax: defaultQueueMax, AddressMax: defaultAddressMax, ViewerMax: defaultViewerMax, IdleAfter: defaultIdleAfter}

	if v := get(keyDataDir); v == "" {
		fail(keyDataDir, "required")
	} else {
		c.DataDir = filepath.Clean(v)
	}

	if v := get(keySeedFile); v == "" {
		fail(keySeedFile, "required")
	} else {
		c.SeedFile = filepath.Clean(v)
	}

	if v := get(keyRPCURLs); v == "" {
		fail(keyRPCURLs, "required")
	} else if urls, err := parseURLList(v); err != nil {
		fail(keyRPCURLs, "%v", err)
	} else {
		c.RPCURLs = urls
	}

	if v := get(keyWorkURLs); v == "" {
		c.WorkURLs = c.RPCURLs
	} else if urls, err := parseURLList(v); err != nil {
		fail(keyWorkURLs, "%v", err)
	} else {
		c.WorkURLs = urls
	}

	c.SendThreshold = parseThreshold(get(keySendThresh), keySendThresh, fail)
	c.ReceiveThreshold = parseThreshold(get(keyRecvThresh), keyRecvThresh, fail)

	c.Representative = get(keyRepresentive)

	if v := get(keyRPCTimeout); v != "" {
		d, err := time.ParseDuration(v)
		switch {
		case err != nil:
			fail(keyRPCTimeout, "not a duration: %q", v)
		case d <= 0 || d > maxRPCTimeout:
			fail(keyRPCTimeout, "must be between 0 and %s, got %s", maxRPCTimeout, d)
		default:
			c.RPCTimeout = d
		}
	}

	if v := get(keyWorkTimeout); v != "" {
		d, err := time.ParseDuration(v)
		switch {
		case err != nil:
			fail(keyWorkTimeout, "not a duration: %q", v)
		case d <= 0 || d > maxWorkTimeout:
			fail(keyWorkTimeout, "must be between 0 and %s, got %s", maxWorkTimeout, d)
		default:
			c.WorkTimeout = d
		}
	}
	if v := get(keySendCeiling); v != "" {
		c.SendCeiling = parseThreshold(v, keySendCeiling, fail)
	}
	if c.SendThreshold != 0 && c.SendCeiling != 0 && c.SendCeiling < c.SendThreshold {
		fail(keySendCeiling, "must not be below %s", keySendThresh)
	}

	c.QueueMax = parseCount(get(keyQueueMax), keyQueueMax, maxQueueMax, c.QueueMax, fail)
	c.AddressMax = parseCount(get(keyAddressMax), keyAddressMax, maxAddressMax, c.AddressMax, fail)
	c.ViewerMax = parseCount(get(keyViewerMax), keyViewerMax, maxViewerMax, c.ViewerMax, fail)

	if v := get(keyIdleAfter); v != "" {
		d, err := time.ParseDuration(v)
		switch {
		case v == "0":
			c.IdleAfter = 0
		case err != nil:
			fail(keyIdleAfter, "not a duration: %q", v)
		case d < time.Minute:
			fail(keyIdleAfter, "must be 0 (off) or at least 1m, got %s", d)
		default:
			c.IdleAfter = d
		}
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return c, nil
}

// thresholds are deliberately not defaulted: they are protocol values that can change, and a stale
// compiled-in value would fail silently as rejected blocks (README, DECISIONS R1)
func parseThreshold(v, key string, fail func(string, string, ...any)) uint64 {
	if v == "" {
		fail(key, "required")
		return 0
	}
	if len(v) != 16 {
		fail(key, "must be 16 hex digits, got %d characters", len(v))
		return 0
	}
	n, err := strconv.ParseUint(v, 16, 64)
	if err != nil {
		fail(key, "not hex: %q", v)
		return 0
	}
	if n == 0 {
		fail(key, "must be non-zero")
		return 0
	}
	return n
}

// EgressTargets is everything the egress proxy may connect to (D57, D93): the host:port of each configured RPC
// and work endpoint. It reads the same keys as Load, so the allow-list cannot drift from what serve calls.
func EgressTargets(lookup func(string) (string, bool)) ([]string, error) {
	get := func(key string) string {
		v, _ := lookup(key)
		return strings.TrimSpace(v)
	}
	var errs []error
	urls, err := parseURLList(get(keyRPCURLs))
	switch {
	case get(keyRPCURLs) == "":
		errs = append(errs, fmt.Errorf("config: %s: required", keyRPCURLs))
	case err != nil:
		errs = append(errs, fmt.Errorf("config: %s: %w", keyRPCURLs, err))
	}
	if v := get(keyWorkURLs); v != "" {
		work, err := parseURLList(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("config: %s: %w", keyWorkURLs, err))
		}
		urls = append(urls, work...)
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	seen := map[string]bool{}
	var out []string
	for _, u := range urls {
		if u.Scheme == "http" {
			continue // a private host, reached directly on the internal network, never through the proxy
		}
		port := u.Port()
		if port == "" {
			port = map[string]string{"https": "443", "http": "80"}[u.Scheme]
		}
		t := net.JoinHostPort(strings.ToLower(u.Hostname()), port)
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out, nil
}

func parseCount(v, key string, max, def int, fail func(string, string, ...any)) int {
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > max {
		fail(key, "must be a whole number from 1 to %d, got %q", max, v)
		return def
	}
	return n
}

func parseURLList(v string) ([]*url.URL, error) {
	var out []*url.URL
	seen := map[string]bool{}
	for _, raw := range strings.Split(v, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil, errors.New("empty entry in list")
		}
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" {
			// the raw value can carry an API key in its query, so it is not echoed back
			return nil, fmt.Errorf("entry %d is not an absolute URL", len(out)+1)
		}
		if u.User != nil {
			return nil, fmt.Errorf("entry %d must not contain credentials", len(out)+1)
		}
		switch {
		case u.Scheme == "https":
		case u.Scheme == "http" && isPrivateName(u.Hostname()):
		default:
			return nil, fmt.Errorf("entry %d (%s): must be https (plain http only for loopback or a container name)", len(out)+1, u.Host)
		}
		if seen[u.String()] {
			return nil, fmt.Errorf("entry %d (%s) is listed twice", len(out)+1, u.Host)
		}
		seen[u.String()] = true
		out = append(out, u)
	}
	return out, nil
}

// WorkThreads is how many cores `drainme work` hashes on (D104). Only that command reads it.
func WorkThreads(lookup func(string) (string, bool)) (int, error) {
	v, _ := lookup(keyWorkThreads)
	var err error
	n := parseCount(strings.TrimSpace(v), keyWorkThreads, maxWorkThreads, defaultWorkThreads, func(key, format string, args ...any) {
		err = fmt.Errorf("config: %s: "+format, append([]any{key}, args...)...)
	})
	return n, err
}

// isPrivateName is a host plain http may reach: loopback, or a single-label name such as drainme-work, which only
// resolves to a container on the same private network and never through public DNS
func isPrivateName(host string) bool {
	if isLoopback(host) {
		return true
	}
	return host != "" && !strings.Contains(host, ".") && !strings.Contains(host, ":") && net.ParseIP(host) == nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
