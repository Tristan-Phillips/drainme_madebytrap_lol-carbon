package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func validEnv() map[string]string {
	return map[string]string{
		keyDataDir:    "/data",
		keySeedFile:   "/run/secrets/drainme_seed",
		keyRPCURLs:    "https://a.example/rpc,https://b.example/api",
		keySendThresh: "fffffff800000000",
		keyRecvThresh: "fffffe0000000000",
	}
}

func lookupFrom(env map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := env[k]; return v, ok }
}

func TestLoadValid(t *testing.T) {
	c, err := Load(lookupFrom(validEnv()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.DataDir != "/data" || c.SeedFile != "/run/secrets/drainme_seed" {
		t.Errorf("paths: got %q %q", c.DataDir, c.SeedFile)
	}
	if len(c.RPCURLs) != 2 || c.RPCURLs[0].Host != "a.example" || c.RPCURLs[1].Host != "b.example" {
		t.Errorf("RPC URLs not parsed in order: %v", c.RPCURLs)
	}
	if c.SendThreshold != 0xfffffff800000000 || c.ReceiveThreshold != 0xfffffe0000000000 {
		t.Errorf("thresholds: %x %x", c.SendThreshold, c.ReceiveThreshold)
	}
	if c.RPCTimeout != defaultRPCTimeout || c.QueueMax != defaultQueueMax || c.AddressMax != defaultAddressMax || c.ViewerMax != defaultViewerMax || c.IdleAfter != 24*time.Hour {
		t.Errorf("defaults: timeout %s, queue %d, addresses %d, viewers %d", c.RPCTimeout, c.QueueMax, c.AddressMax, c.ViewerMax)
	}
	if len(c.WorkURLs) != 2 || c.WorkURLs[0] != c.RPCURLs[0] {
		t.Errorf("work URLs should default to RPC URLs, got %v", c.WorkURLs)
	}
}

func TestLoadOverrides(t *testing.T) {
	env := validEnv()
	env[keyWorkURLs] = "https://work.example"
	env[keyRPCTimeout] = "3s"
	env[keyQueueMax] = "25"
	env[keyAddressMax] = "500"
	env[keyViewerMax] = "7"
	env[keyIdleAfter] = "90m"
	env[keyRepresentive] = "nano_rep"
	env[keyDataDir] = "/data/../data/./ledger/"
	c, err := Load(lookupFrom(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(c.WorkURLs) != 1 || c.WorkURLs[0].Host != "work.example" {
		t.Errorf("work URLs: %v", c.WorkURLs)
	}
	if c.QueueMax != 25 || c.AddressMax != 500 || c.ViewerMax != 7 || c.IdleAfter != 90*time.Minute {
		t.Errorf("queue max %d, address max %d, viewer max %d", c.QueueMax, c.AddressMax, c.ViewerMax)
	}
	if c.RPCTimeout != 3*time.Second || c.Representative != "nano_rep" || c.DataDir != "/data/ledger" {
		t.Errorf("got timeout %s rep %q dir %q", c.RPCTimeout, c.Representative, c.DataDir)
	}
}

func TestLoadRejects(t *testing.T) {
	cases := []struct {
		name, key, value, want string
	}{
		{"missing data dir", keyDataDir, "", keyDataDir + ": required"},
		{"blank seed file", keySeedFile, "   ", keySeedFile + ": required"},
		{"missing rpc", keyRPCURLs, "", keyRPCURLs + ": required"},
		{"plain http remote", keyRPCURLs, "http://a.example", "must be https"},
		{"plain http to an IP", keyRPCURLs, "http://10.0.0.5:7076", "must be https"},
		{"plain http to an IPv6 address", keyRPCURLs, "http://[fd00::1]:7076", "must be https"},
		{"ceiling below threshold", keySendCeiling, "0000000000000001", "must not be below"},
		{"work timeout too long", keyWorkTimeout, "11m", "between 0 and"},
		{"work timeout not a duration", keyWorkTimeout, "soon", "not a duration"},
		{"credentials in url", keyRPCURLs, "https://user:pw@a.example", "must not contain credentials"},
		{"relative url", keyRPCURLs, "a.example/rpc", "not an absolute URL"},
		{"empty list entry", keyRPCURLs, "https://a.example,,https://b.example", "empty entry"},
		{"duplicate url", keyRPCURLs, "https://a.example,https://a.example", "listed twice"},
		{"bad work url", keyWorkURLs, "ftp://w.example", "must be https"},
		{"short threshold", keySendThresh, "fffffff8", "16 hex digits"},
		{"non-hex threshold", keyRecvThresh, "zzzzzzzzzzzzzzzz", "not hex"},
		{"zero threshold", keySendThresh, "0000000000000000", "non-zero"},
		{"missing threshold", keyRecvThresh, "", keyRecvThresh + ": required"},
		{"bad timeout", keyRPCTimeout, "soon", "not a duration"},
		{"negative timeout", keyRPCTimeout, "-1s", "must be between"},
		{"huge timeout", keyRPCTimeout, "1h", "must be between"},
		{"zero queue", keyQueueMax, "0", "whole number from 1"},
		{"negative queue", keyQueueMax, "-5", "whole number from 1"},
		{"huge queue", keyQueueMax, "1000001", "whole number from 1"},
		{"non-numeric queue", keyQueueMax, "lots", "whole number from 1"},
		{"zero addresses", keyAddressMax, "0", "whole number from 1"},
		{"huge addresses", keyAddressMax, "1000000001", "whole number from 1"},
		{"zero viewers", keyViewerMax, "0", "whole number from 1"},
		{"idle too short", keyIdleAfter, "30s", "at least 1m"},
		{"idle not a duration", keyIdleAfter, "a day", "not a duration"},
		{"negative idle", keyIdleAfter, "-1h", "at least 1m"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnv()
			env[tc.key] = tc.value
			_, err := Load(lookupFrom(env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestLoadAllowsLoopbackHTTP(t *testing.T) {
	for _, u := range []string{"http://127.0.0.1:7076", "http://localhost:7076", "http://[::1]:7076"} {
		env := validEnv()
		env[keyRPCURLs] = u
		if _, err := Load(lookupFrom(env)); err != nil {
			t.Errorf("%s: %v", u, err)
		}
	}
}

func TestLoadReportsEveryProblemAtOnce(t *testing.T) {
	_, err := Load(lookupFrom(map[string]string{}))
	if err == nil {
		t.Fatal("empty env must fail")
	}
	for _, key := range []string{keyDataDir, keySeedFile, keyRPCURLs, keySendThresh, keyRecvThresh} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s:\n%v", key, err)
		}
	}
}

func TestLoadDoesNotEchoURLSecrets(t *testing.T) {
	const secret = "sekret-api-key-123" // gitleaks:allow (a made-up key: the test proves it is never echoed)
	env := validEnv()
	env[keyRPCURLs] = "not a url?key=" + secret
	_, err := Load(lookupFrom(env))
	if err == nil {
		t.Fatal("want error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaks URL query: %v", err)
	}
}

const testSeedHex = "0000000000000000000000000000000000000000000000000000000000000001"

func writeSeed(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "seed")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestReadSeed(t *testing.T) {
	for _, content := range []string{testSeedHex, testSeedHex + "\n", "  " + strings.ToUpper(testSeedHex) + "\r\n"} {
		seed, err := ReadSeed(writeSeed(t, content))
		if err != nil {
			t.Fatalf("%q: %v", content, err)
		}
		if seed[31] != 1 || seed[0] != 0 {
			t.Errorf("%q decoded wrong: %x", content, seed)
		}
	}
}

func TestReadSeedRejects(t *testing.T) {
	cases := map[string]string{
		"empty":     "",
		"short":     testSeedHex[:62],
		"long":      testSeedHex + "00",
		"non-hex":   "g" + testSeedHex[1:],
		"two seeds": testSeedHex + "\n" + testSeedHex,
		"oversized": strings.Repeat("a", maxSeedFileBytes+10),
		"mnemonic":  "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ReadSeed(writeSeed(t, content))
			if !errors.Is(err, ErrSeedFormat) {
				t.Fatalf("want ErrSeedFormat, got %v", err)
			}
		})
	}
}

func TestReadSeedMissingFile(t *testing.T) {
	_, err := ReadSeed(filepath.Join(t.TempDir(), "nope"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("want ErrNotExist, got %v", err)
	}
}

// a directory opens fine on Linux but fails on read, which is the path a misconfigured mount takes
func TestReadSeedUnreadable(t *testing.T) {
	_, err := ReadSeed(t.TempDir())
	if err == nil || errors.Is(err, ErrSeedFormat) || !strings.Contains(err.Error(), "reading seed file") {
		t.Fatalf("want a read error, got %v", err)
	}
}

// the seed must never surface in an error, even when the file is almost right
func TestReadSeedErrorsNeverContainContent(t *testing.T) {
	const marker = "deadbeefcafe"
	for _, content := range []string{
		marker + testSeedHex[len(marker):] + "x",   // 65 chars
		marker + testSeedHex[len(marker)+1:] + "g", // 64 chars, one non-hex
	} {
		_, err := ReadSeed(writeSeed(t, content))
		if err == nil {
			t.Fatalf("%q: want error", content)
		}
		if strings.Contains(strings.ToLower(err.Error()), marker) {
			t.Fatalf("error leaks seed content: %v", err)
		}
	}
}

func TestEgressTargets(t *testing.T) {
	env := map[string]string{
		keyRPCURLs:  "https://A.example/rpc,https://b.example:8443/api",
		keyWorkURLs: "https://a.example/work,http://127.0.0.1:7076,http://drainme-work:7076",
	}
	got, err := EgressTargets(lookupFrom(env))
	if err != nil {
		t.Fatal(err)
	}
	// plain-http hosts are private and reached directly, so the proxy never needs them
	want := "a.example:443,b.example:8443"
	if strings.Join(got, ",") != want {
		t.Fatalf("targets %v, want %s", got, want)
	}
	if _, err := EgressTargets(lookupFrom(map[string]string{})); err == nil {
		t.Error("no RPC URLs accepted")
	}
	if _, err := EgressTargets(lookupFrom(map[string]string{keyRPCURLs: "http://evil.example"})); err == nil {
		t.Error("plain http to a remote host accepted")
	}
	if _, err := EgressTargets(lookupFrom(map[string]string{keyRPCURLs: "https://a.example", keyWorkURLs: "ftp://w"})); err == nil {
		t.Error("bad work URL accepted")
	}
}

func TestIdleAfterOff(t *testing.T) {
	env := validEnv()
	env[keyIdleAfter] = "0"
	c, err := Load(lookupFrom(env))
	if err != nil || c.IdleAfter != 0 {
		t.Fatalf("%v %v", c, err)
	}
}

func TestWorkSettings(t *testing.T) {
	c, err := Load(lookupFrom(validEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if c.WorkTimeout != 3*time.Minute || c.SendCeiling != 0xfffffffe00000000 || c.QueueMax != 10_000 {
		t.Fatalf("defaults %s %x %d", c.WorkTimeout, c.SendCeiling, c.QueueMax)
	}
	env := validEnv()
	env[keyWorkURLs] = "https://w.example,http://drainme-work:7076"
	env[keyWorkTimeout] = "90s"
	env[keySendCeiling] = "ffffffff00000000"
	if c, err = Load(lookupFrom(env)); err != nil || c.WorkTimeout != 90*time.Second || c.SendCeiling != 0xffffffff00000000 || len(c.WorkURLs) != 2 {
		t.Fatalf("%+v %v", c, err)
	}
	if n, err := WorkThreads(lookupFrom(map[string]string{})); n != 2 || err != nil {
		t.Fatalf("threads default %d %v", n, err)
	}
	if n, err := WorkThreads(lookupFrom(map[string]string{keyWorkThreads: "6"})); n != 6 || err != nil {
		t.Fatalf("threads %d %v", n, err)
	}
	for _, bad := range []string{"0", "65", "two"} {
		if _, err := WorkThreads(lookupFrom(map[string]string{keyWorkThreads: bad})); err == nil {
			t.Errorf("threads %q accepted", bad)
		}
	}
}
