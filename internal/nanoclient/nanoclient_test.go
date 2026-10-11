package nanoclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/fakenode"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanoclient"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

var ctx = context.Background()

// low enough that the fake node finds work instantly, high enough that a random value usually fails it
const threshold = 0xff00000000000000

var (
	key     = nanocrypto.DeriveKey([32]byte{0x51}, 0)
	genesis = nanocrypto.Hash{0x6e, 0x01}
	dest    = nanocrypto.DeriveKey([32]byte{0x52}, 0).Public()
)

// real mainnet sends: the network accepted these exact fields, so our codec must reproduce them byte for byte
func TestBlockCodecMatchesMainnet(t *testing.T) {
	data, err := os.ReadFile("../nanocrypto/testdata/chain_blocks.json")
	if err != nil {
		t.Fatal(err)
	}
	var blocks []map[string]string
	if err := json.Unmarshal(data, &blocks); err != nil {
		t.Fatal(err)
	}
	sends := 0
	for _, b := range blocks {
		if b["subtype"] != "send" {
			continue
		}
		sends++
		wire := fmt.Sprintf(`{"type":"state","account":%q,"previous":%q,"representative":%q,"balance":%q,"link":%q,"signature":%q,"work":%q}`,
			b["account"], b["previous"], b["representative"], b["balance"], b["link"], b["signature"], b["work"])
		blk, err := nanoclient.DecodeBlock([]byte(wire))
		if err != nil {
			t.Fatalf("%s: %v", b["hash"], err)
		}
		h, _ := blk.Hash()
		if fmt.Sprintf("%X", h) != b["hash"] || !nanocrypto.Verify(blk.Account, h, blk.Signature) {
			t.Errorf("%s: decoded to hash %X", b["hash"], h)
		}
		if string(blk.Encode()) != wire {
			t.Errorf("%s: re-encoded as\n%s\nwant\n%s", b["hash"], blk.Encode(), wire)
		}
	}
	if sends == 0 {
		t.Fatal("no send blocks in the fixture")
	}
}

func signed(t *testing.T, prev nanocrypto.Hash, bal *big.Int, work uint64) (nanocrypto.Hash, []byte) {
	t.Helper()
	b := nanoclient.Block{StateBlock: nanocrypto.StateBlock{Account: key.Public(), Previous: prev, Representative: key.Public(), Balance: bal, Link: dest}, Work: work}
	h, err := b.Hash()
	if err != nil {
		t.Fatal(err)
	}
	b.Signature = key.Sign(h)
	return h, b.Encode()
}

func TestDecodeBlockRejects(t *testing.T) {
	_, good := signed(t, genesis, big.NewInt(5), 0xabc)
	if _, err := nanoclient.DecodeBlock(good); err != nil {
		t.Fatalf("valid block rejected: %v", err)
	}
	s := string(good)
	field := func(name string) string { // the value of a field in the canonical encoding
		i := strings.Index(s, `"`+name+`":"`) + len(name) + 4
		return s[i : i+strings.IndexByte(s[i:], '"')]
	}
	over := new(big.Int).Lsh(big.NewInt(1), 128).String()
	cases := map[string]string{
		"empty":             "",
		"not json":          "state",
		"lowercase hash":    strings.Replace(s, field("previous"), strings.ToLower(field("previous")), 1),
		"uppercase work":    strings.Replace(s, field("work"), strings.ToUpper(field("work")), 1),
		"short work":        strings.Replace(s, `"work":"`+field("work"), `"work":"abc`, 1),
		"short signature":   strings.Replace(s, field("signature"), field("signature")[2:], 1),
		"leading zero":      strings.Replace(s, `"balance":"5"`, `"balance":"05"`, 1),
		"negative balance":  strings.Replace(s, `"balance":"5"`, `"balance":"-5"`, 1),
		"balance 2^128":     strings.Replace(s, `"balance":"5"`, `"balance":"`+over+`"`, 1),
		"legacy type":       strings.Replace(s, `"type":"state"`, `"type":"send"`, 1),
		"unknown field":     strings.Replace(s, `}`, `,"extra":"1"}`, 1),
		"missing field":     strings.Replace(s, `"work":"`+field("work")+`"`, `"x":"1"`, 1),
		"bad checksum":      strings.Replace(s, field("account"), field("account")[:64]+"1", 1),
		"whitespace":        strings.Replace(s, `,`, `, `, 1),
		"trailing data":     s + " ",
		"duplicate balance": strings.Replace(s, `"balance":"5"`, `"balance":"9","balance":"5"`, 1),
	}
	for name, data := range cases {
		if data == s {
			t.Fatalf("%s: mutation did not change the block", name)
		}
		if _, err := nanoclient.DecodeBlock([]byte(data)); !errors.Is(err, nanoclient.ErrReply) {
			t.Errorf("%s: accepted %s", name, data)
		}
	}
}

func client(urls ...*url.URL) *nanoclient.Client { return nanoclient.New(urls, urls, 5*time.Second) }

func TestAgainstFakeNode(t *testing.T) {
	node := fakenode.New(t, key.Public(), genesis, big.NewInt(100), threshold)
	c := client(node.URL)

	frontier, bal, err := c.AccountInfo(ctx, key.Public())
	if err != nil || frontier != genesis || bal.String() != "100" {
		t.Fatalf("account_info: %X %v %v", frontier, bal, err)
	}
	if _, _, err := c.AccountInfo(ctx, dest); err == nil {
		t.Error("unknown account answered")
	}

	work, err := c.Work(ctx, genesis, threshold)
	if err != nil || nanocrypto.Difficulty(genesis, work) < threshold {
		t.Fatalf("work %x: %v", work, err)
	}

	h, block := signed(t, genesis, big.NewInt(60), work)
	if ok, err := c.Confirmed(ctx, h); ok || err != nil {
		t.Fatalf("unknown block: confirmed %v, %v", ok, err)
	}
	if err := c.Process(ctx, h, block, "send"); err != nil {
		t.Fatal(err)
	}
	if err := c.Process(ctx, h, block, "send"); err != nil {
		t.Fatalf("rebroadcast of the same block must succeed: %v", err)
	}
	if ok, err := c.Confirmed(ctx, h); !ok || err != nil {
		t.Fatalf("confirmed %v, %v", ok, err)
	}
	if node.Paid(dest).String() != "40" || node.Blocks() != 1 {
		t.Fatalf("paid %s in %d blocks", node.Paid(dest), node.Blocks())
	}

	// a different block on the same previous is a fork, and a fork is reported as such
	h2, fork := signed(t, genesis, big.NewInt(50), work)
	if err := c.Process(ctx, h2, fork, "send"); !errors.Is(err, nanoclient.ErrFork) {
		t.Fatalf("fork: %v", err)
	}
	// blocks the network would refuse are refused, so a test against the fake node means something
	next, _ := c.Work(ctx, h, threshold)
	for name, tc := range map[string]struct {
		prev nanocrypto.Hash
		bal  int64
		work uint64
	}{
		"gap":       {nanocrypto.Hash{9}, 10, next},
		"low work":  {h, 10, lowWork(h)},
		"a receive": {h, 70, next},
	} {
		hh, b := signed(t, tc.prev, big.NewInt(tc.bal), tc.work)
		if err := c.Process(ctx, hh, b, "send"); err == nil || errors.Is(err, nanoclient.ErrFork) {
			t.Errorf("%s: %v", name, err)
		}
	}
	tampered, err := nanoclient.DecodeBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	tampered.Previous, tampered.Balance = h, big.NewInt(0) // a bigger payout under the old signature
	th, _ := tampered.Hash()
	if err := c.Process(ctx, th, tampered.Encode(), "send"); err == nil {
		t.Error("block with a signature over other content accepted")
	}
	if node.Blocks() != 1 {
		t.Fatalf("node accepted %d blocks, want 1", node.Blocks())
	}
}

func goodWork(root nanocrypto.Hash) uint64 {
	for w := uint64(0); ; w++ {
		if nanocrypto.Difficulty(root, w) >= threshold {
			return w
		}
	}
}

func lowWork(root nanocrypto.Hash) uint64 {
	for w := uint64(0); ; w++ {
		if nanocrypto.Difficulty(root, w) < threshold {
			return w
		}
	}
}

func serve(t *testing.T, h http.HandlerFunc) *url.URL {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL + "/rpc?key=sekret-api-key")
	return u
}

func reply(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }
}

// a broken, lying or hostile endpoint is skipped and the next one answers
func TestFailover(t *testing.T) {
	node := fakenode.New(t, key.Public(), genesis, big.NewInt(100), threshold)
	bad := map[string]http.HandlerFunc{
		"500": func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "down", 500) },
		"redirect": func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, node.URL.String(), http.StatusTemporaryRedirect)
		},
		"oversize":    reply(fmt.Sprintf(`{"work":"%016x","pad":"%s"}`, goodWork(genesis), strings.Repeat("A", 70<<10))),
		"unreachable": nil, // replaced below by a server that is already closed
		"not json":    reply(`<html>`),
		"weak work":   reply(fmt.Sprintf(`{"work":"%016x"}`, lowWork(genesis))),
		"error reply": reply(`{"error":"Unable to parse JSON"}`),
	}
	dead := httptest.NewServer(nil)
	deadURL, _ := url.Parse(dead.URL + "/rpc?key=sekret-api-key")
	dead.Close()
	for name, h := range bad {
		t.Run(name, func(t *testing.T) {
			u := deadURL
			if h != nil {
				u = serve(t, h)
			}
			c := client(u, node.URL)
			if _, _, err := c.AccountInfo(ctx, key.Public()); err != nil {
				t.Errorf("account_info did not fail over: %v", err)
			}
			if _, err := c.Work(ctx, genesis, threshold); err != nil {
				t.Errorf("work did not fail over: %v", err)
			}
			// alone, the bad endpoint is an error, and the error never carries the URL's query
			_, err := client(u).Work(ctx, genesis, threshold)
			if err == nil {
				t.Fatal("bad endpoint accepted")
			}
			if strings.Contains(err.Error(), "sekret") {
				t.Fatalf("error leaks the URL query: %v", err)
			}
		})
	}
}

// replies are parsed strictly: a value we would not have written ourselves is not a value
func TestRepliesAreStrict(t *testing.T) {
	hash := strings.Repeat("AB", 32)
	for name, body := range map[string]string{
		"lowercase frontier": `{"frontier":"` + strings.ToLower(hash) + `","balance":"1"}`,
		"short frontier":     `{"frontier":"ABCD","balance":"1"}`,
		"negative balance":   `{"frontier":"` + hash + `","balance":"-1"}`,
		"padded balance":     `{"frontier":"` + hash + `","balance":"01"}`,
		"missing balance":    `{"frontier":"` + hash + `"}`,
	} {
		if _, _, err := client(serve(t, reply(body))).AccountInfo(ctx, key.Public()); !errors.Is(err, nanoclient.ErrReply) {
			t.Errorf("account_info %s: %v", name, err)
		}
	}
	for name, body := range map[string]string{
		"confirmed missing": `{}`,
		"confirmed bool":    `{"confirmed":true}`,
		"confirmed other":   `{"confirmed":"yes"}`,
		"other error":       `{"error":"Bad hash number"}`,
	} {
		if _, err := client(serve(t, reply(body))).Confirmed(ctx, genesis); !errors.Is(err, nanoclient.ErrReply) {
			t.Errorf("block_info %s: %v", name, err)
		}
	}
	for name, body := range map[string]string{
		"other hash": `{"hash":"` + hash + `"}`,
		"empty":      `{}`,
	} {
		if err := client(serve(t, reply(body))).Process(ctx, genesis, []byte(`{}`), "send"); !errors.Is(err, nanoclient.ErrReply) {
			t.Errorf("process %s: %v", name, err)
		}
	}
	// a node saying no about our account or block is a rejection, which the worker counts towards a halt
	if err := client(serve(t, reply(`{"error":"Gap previous block"}`))).Process(ctx, genesis, []byte(`{}`), "send"); !errors.Is(err, nanoclient.ErrRejected) {
		t.Errorf("process rejection: %v", err)
	}
	if _, _, err := client(serve(t, reply(`{"error":"Account not found"}`))).AccountInfo(ctx, key.Public()); !errors.Is(err, nanoclient.ErrRejected) {
		t.Errorf("account_info rejection: %v", err)
	}
	if _, _, err := client().AccountInfo(ctx, key.Public()); err == nil {
		t.Error("no endpoints answered as success")
	}
	if err := client(serve(t, reply(`{"error":"Old block"}`))).Process(ctx, genesis, []byte(`{}`), "send"); err != nil {
		t.Errorf("Old block means already published: %v", err)
	}
	// a fork is final: no later endpoint gets the chance to report success instead
	c := client(serve(t, reply(`{"error":"Fork"}`)), serve(t, reply(`{"error":"Old block"}`)))
	if err := c.Process(ctx, genesis, []byte(`{}`), "send"); err != nanoclient.ErrFork {
		t.Errorf("fork: %v", err)
	}
}

func TestReceivable(t *testing.T) {
	hash := strings.Repeat("AB", 32)
	got, err := client(serve(t, reply(`{"blocks":""}`))).Receivable(ctx, key.Public())
	if err != nil || len(got) != 0 {
		t.Fatalf("none: %v %v", got, err)
	}
	got, err = client(serve(t, reply(`{"blocks":{"`+hash+`":"1000000000000000000000000000000"}}`))).Receivable(ctx, key.Public())
	if err != nil || len(got) != 1 {
		t.Fatalf("one: %v %v", got, err)
	}
	for h, amt := range got {
		if h.Hex() != hash || amt.String() != "1000000000000000000000000000000" {
			t.Fatalf("parsed %s %s", h.Hex(), amt)
		}
	}
	for name, body := range map[string]string{
		"lowercase hash": `{"blocks":{"` + strings.ToLower(hash) + `":"5"}}`,
		"zero amount":    `{"blocks":{"` + hash + `":"0"}}`,
		"padded amount":  `{"blocks":{"` + hash + `":"05"}}`,
		"list form":      `{"blocks":["` + hash + `"]}`,
		"error":          `{"error":"Bad account number"}`,
	} {
		if _, err := client(serve(t, reply(body))).Receivable(ctx, key.Public()); !errors.Is(err, nanoclient.ErrReply) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// an unopened account is told apart from other rejections, and still counts as one
func TestUnopened(t *testing.T) {
	_, _, err := client(serve(t, reply(`{"error":"Account not found"}`))).AccountInfo(ctx, key.Public())
	if !errors.Is(err, nanoclient.ErrUnopened) || !errors.Is(err, nanoclient.ErrRejected) {
		t.Fatalf("got %v", err)
	}
	_, _, err = client(serve(t, reply(`{"error":"Bad account number"}`))).AccountInfo(ctx, key.Public())
	if errors.Is(err, nanoclient.ErrUnopened) || !errors.Is(err, nanoclient.ErrRejected) {
		t.Fatalf("other rejection: %v", err)
	}
}

func TestBlockState(t *testing.T) {
	hash := strings.Repeat("AB", 32)
	for body, want := range map[string]nanoclient.BlockState{
		`{"error":"Block not found"}`:                      {},
		`{"confirmed":"true","height":"7","hash":"` + hash: {}, // cut short: not JSON
		`{"confirmed":"false","height":"12"}`:              {Known: true, Height: 12},
		`{"confirmed":"true","height":"1"}`:                {Known: true, Confirmed: true, Height: 1},
	} {
		got, err := client(serve(t, reply(body))).Block(ctx, genesis)
		if strings.HasSuffix(body, hash) {
			if !errors.Is(err, nanoclient.ErrReply) {
				t.Errorf("%s: %v", body, err)
			}
			continue
		}
		if err != nil || got != want {
			t.Errorf("%s: %+v %v", body, got, err)
		}
	}
	for _, body := range []string{`{"confirmed":"true"}`, `{"confirmed":"true","height":"0"}`, `{"confirmed":"true","height":"-1"}`,
		`{"confirmed":"maybe","height":"3"}`, `{"error":"Bad hash number"}`} {
		if _, err := client(serve(t, reply(body))).Block(ctx, genesis); !errors.Is(err, nanoclient.ErrReply) {
			t.Errorf("%s accepted: %v", body, err)
		}
	}
}

// D104: a node that failed is asked after the healthy ones for a minute, then first again; a node that answers
// "no" has not failed
func TestFailedNodeGoesLast(t *testing.T) {
	var badHits, goodHits int
	bad := serve(t, func(w http.ResponseWriter, _ *http.Request) { badHits++; http.Error(w, "down", 502) })
	good := serve(t, func(w http.ResponseWriter, _ *http.Request) { goodHits++; fmt.Fprint(w, `{"confirmed":"true"}`) })
	c := nanoclient.New([]*url.URL{bad, good}, nil, time.Second)
	now := time.Unix(1_000_000, 0)
	nanoclient.SetClock(c, func() time.Time { return now })
	for i := 0; i < 3; i++ {
		if ok, err := c.Confirmed(ctx, genesis); !ok || err != nil {
			t.Fatal(ok, err)
		}
	}
	if badHits != 1 || goodHits != 3 {
		t.Fatalf("bad node asked %d times, good %d: want 1 and 3", badHits, goodHits)
	}
	now = now.Add(61 * time.Second)
	c.Confirmed(ctx, genesis)
	if badHits != 2 {
		t.Fatalf("after a minute the failed node was not tried first again (%d)", badHits)
	}

	// everyone failing: each is still asked every time, so a call never gives up without trying
	both := nanoclient.New([]*url.URL{bad, bad}, nil, time.Second)
	nanoclient.SetClock(both, func() time.Time { return now })
	before := badHits
	both.Confirmed(ctx, genesis)
	both.Confirmed(ctx, genesis)
	if badHits-before != 4 {
		t.Fatalf("all-down endpoints asked %d times, want 4", badHits-before)
	}

	// a rejection keeps its place
	var noHits int
	no := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		noHits++
		fmt.Fprint(w, `{"error":"Gap previous block"}`)
	})
	c = nanoclient.New([]*url.URL{no, good}, nil, time.Second)
	nanoclient.SetClock(c, func() time.Time { return now })
	c.Process(ctx, genesis, []byte(`{}`), "send")
	c.Process(ctx, genesis, []byte(`{}`), "send")
	if noHits != 2 {
		t.Fatalf("a node that rejected was moved back (%d asks, want 2)", noHits)
	}
}

// every attempt has its own deadline: a node that hangs costs one timeout, then the next one answers
func TestHungNodeCostsOneTimeout(t *testing.T) {
	release := make(chan struct{})
	hung := serve(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { close(release) }) // registered after serve, so it runs before the server's Close waits
	good := serve(t, reply(`{"work":"`+fmt.Sprintf("%016x", goodWork(genesis))+`"}`))
	c := nanoclient.New([]*url.URL{hung, good}, []*url.URL{hung, good}, 50*time.Millisecond)
	start := time.Now()
	c.Confirmed(ctx, genesis) // good answers block_info with a work body, an unusable reply; only the time matters
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("hung node held the call for %s", took)
	}
	c2 := nanoclient.New([]*url.URL{good}, []*url.URL{hung, good}, 50*time.Millisecond)
	c2.SetWorkTimeout(200 * time.Millisecond)
	start = time.Now()
	if _, err := c2.Work(ctx, genesis, threshold); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 200*time.Millisecond || took > 2*time.Second {
		t.Fatalf("work waited %s on the hung node; want its own 200ms timeout", took)
	}
}

func TestNetworkMinimum(t *testing.T) {
	ad := func(min string) *url.URL {
		return serve(t, reply(`{"network_minimum":"`+min+`","network_current":"ffffffffffffffff"}`))
	}
	down := serve(t, func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", 500) })
	refuses := serve(t, reply(`{"error":"Unsupported RPC action"}`))
	cases := []struct {
		name string
		urls []*url.URL
		want uint64
		err  string
	}{
		{"two agree", []*url.URL{ad("fffffff800000000"), ad("fffffff800000000")}, 0xfffffff800000000, ""},
		{"skips the ones that cannot say", []*url.URL{down, refuses, ad("fffffffc00000000"), ad("fffffffc00000000")}, 0xfffffffc00000000, ""},
		{"disagree", []*url.URL{ad("fffffff800000000"), ad("ffffffff00000000")}, 0, "disagree"},
		{"only one answers", []*url.URL{down, ad("fffffff800000000")}, 0, "1 answered"},
		{"malformed", []*url.URL{ad("fff"), ad("fff")}, 0, "0 answered"},
		{"zero", []*url.URL{ad("0000000000000000"), ad("0000000000000000")}, 0, "0 answered"},
	}
	for _, tc := range cases {
		got, err := nanoclient.New(tc.urls, nil, time.Second).NetworkMinimum(ctx)
		if tc.err == "" && (err != nil || got != tc.want) {
			t.Errorf("%s: %x %v", tc.name, got, err)
		}
		if tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)) {
			t.Errorf("%s: want error %q, got %x %v", tc.name, tc.err, got, err)
		}
	}
}

// whatever bytes arrive, DecodeBlock either refuses them or returns a block whose encoding is exactly those bytes,
// so a persisted block can never mean two things
func FuzzDecodeBlock(f *testing.F) {
	b := nanoclient.Block{StateBlock: nanocrypto.StateBlock{Account: key.Public(), Previous: genesis, Representative: dest,
		Balance: big.NewInt(42), Link: dest}, Work: 7}
	f.Add(b.Encode())
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := nanoclient.DecodeBlock(data)
		if err != nil {
			return
		}
		if string(got.Encode()) != string(data) {
			t.Fatalf("decoded %q, re-encodes as %q", data, got.Encode())
		}
		if _, err := got.Hash(); err != nil {
			t.Fatalf("decoded a block that cannot be hashed: %v", err)
		}
	})
}
