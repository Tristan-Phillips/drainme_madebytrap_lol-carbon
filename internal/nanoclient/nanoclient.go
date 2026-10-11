// Package nanoclient speaks Nano node RPC for the four calls drainme makes. Nothing a node says is trusted with
// money: frontier and balance come from the ledger (D86 R6), AccountInfo exists only as a cross-check, and work
// is validated here before it is returned (D7).
package nanoclient

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

// a node's replies to these calls are a few hundred bytes; reading stops here, and a cut-off JSON object fails
// to parse, so an oversized reply costs at most this much memory and is never half-trusted
const maxReply = 64 << 10

var (
	ErrFork  = errors.New("nanoclient: node reports a fork: another block already follows our frontier")
	ErrReply = errors.New("nanoclient: unusable reply")
	// ErrRejected is a node answering that our account or block is wrong: it holds a different chain than our
	// books, or it lags or lies. One rejection proves little and failover moves on; the worker halts only when
	// rejections persist (D24, D86 R6).
	ErrRejected = errors.New("nanoclient: node rejected our account or block")
	// ErrUnopened is the node saying the account has no blocks yet. It is also a rejection, so to the payout
	// worker it is a disagreement like any other; only init-vault expects it.
	ErrUnopened = errors.New("nanoclient: account not opened")
)

// Block is a signed send in the JSON form `process` takes. These exact bytes are persisted before broadcast.
type Block struct {
	nanocrypto.StateBlock
	Signature nanocrypto.Signature
	Work      uint64
}

type wireBlock struct {
	Type           string `json:"type"`
	Account        string `json:"account"`
	Previous       string `json:"previous"`
	Representative string `json:"representative"`
	Balance        string `json:"balance"`
	Link           string `json:"link"`
	Signature      string `json:"signature"`
	Work           string `json:"work"`
}

func (b Block) Encode() []byte {
	out, err := json.Marshal(wireBlock{"state", b.Account.Address(), b.Previous.Hex(), b.Representative.Address(),
		b.Balance.String(), fmt.Sprintf("%X", b.Link), fmt.Sprintf("%X", b.Signature), fmt.Sprintf("%016x", b.Work)})
	if err != nil {
		panic(err) // a struct of strings always marshals
	}
	return out
}

// DecodeBlock accepts only what Encode produces: whatever decodes must re-encode to the identical bytes, which
// rejects unknown fields, other cases, padding and every other near-miss with one rule.
func DecodeBlock(data []byte) (Block, error) {
	var w wireBlock
	var b Block
	bad := fmt.Errorf("%w: block is not canonical", ErrReply)
	if json.Unmarshal(data, &w) != nil {
		return b, bad
	}
	var err error
	if b.Account, err = nanocrypto.ParseAddress(w.Account); err != nil {
		return b, bad
	}
	if b.Representative, err = nanocrypto.ParseAddress(w.Representative); err != nil {
		return b, bad
	}
	var ok bool
	if b.Balance, ok = new(big.Int).SetString(w.Balance, 10); !ok {
		return b, bad
	}
	if _, err := b.Hash(); err != nil {
		return b, bad
	}
	if !unhex(b.Previous[:], w.Previous) || !unhex(b.Link[:], w.Link) || !unhex(b.Signature[:], w.Signature) {
		return b, bad
	}
	if b.Work, err = strconv.ParseUint(w.Work, 16, 64); err != nil || !bytes.Equal(b.Encode(), data) {
		return b, bad
	}
	return b, nil
}

func unhex(dst []byte, s string) bool {
	// the length check comes first: hex.Decode panics when the input is longer than the destination
	if len(s) != 2*len(dst) {
		return false
	}
	_, err := hex.Decode(dst, []byte(s))
	return err == nil
}

// Client tries each endpoint in configured order (D6) and returns the first usable reply.
// skipFor is how long an endpoint that failed is tried only after the healthy ones, so a hung node costs one
// timeout a minute instead of one per call (D104)
const skipFor = time.Minute

type Client struct {
	rpc, work               []*url.URL
	http                    *http.Client
	rpcTimeout, workTimeout time.Duration
	now                     func() time.Time
	mu                      sync.Mutex
	failedAt                map[*url.URL]time.Time
}

// New makes a client whose every call, work included, waits at most timeout per endpoint; SetWorkTimeout gives
// work its own limit.
func New(rpc, work []*url.URL, timeout time.Duration) *Client {
	return &Client{rpc: rpc, work: work, rpcTimeout: timeout, workTimeout: timeout, now: time.Now, failedAt: map[*url.URL]time.Time{},
		http: &http.Client{
			// a redirect could take the request off the configured hosts, or from https to http
			CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") },
		}}
}

// SetWorkTimeout sets how long one work_generate may take per endpoint, which for the local CPU fallback is
// minutes (D104). Set it before use.
func (c *Client) SetWorkTimeout(d time.Duration) { c.workTimeout = d }

// AccountInfo is the node's view of our account, for the boot cross-check only (D24). Never build on it.
func (c *Client) AccountInfo(ctx context.Context, pub nanocrypto.PublicKey) (nanocrypto.Hash, *big.Int, error) {
	var h nanocrypto.Hash
	var bal *big.Int
	err := c.call(ctx, c.rpc, map[string]string{"action": "account_info", "account": pub.Address()}, func(body []byte) error {
		var r struct{ Frontier, Balance, Error string }
		if err := json.Unmarshal(body, &r); err != nil {
			return ErrReply
		}
		if r.Error == "Account not found" {
			return fmt.Errorf("%w: %w", ErrRejected, ErrUnopened)
		}
		if r.Error != "" {
			return rejected(r.Error)
		}
		var ok bool
		bal, ok = new(big.Int).SetString(r.Balance, 10)
		if !ok || bal.Sign() < 0 || bal.String() != r.Balance {
			return ErrReply
		}
		var err error
		if h, err = nanocrypto.ParseHash(r.Frontier); err != nil {
			return ErrReply
		}
		return nil
	})
	return h, bal, err
}

// Confirmed reports whether the block is cemented (D23). A block the node has never seen is simply not confirmed.
func (c *Client) Confirmed(ctx context.Context, hash nanocrypto.Hash) (bool, error) {
	var confirmed bool
	err := c.call(ctx, c.rpc, map[string]string{"action": "block_info", "json_block": "true", "hash": hash.Hex()}, func(body []byte) error {
		var r struct{ Confirmed, Error string }
		if err := json.Unmarshal(body, &r); err != nil {
			return ErrReply
		}
		switch {
		case r.Error == "Block not found":
			confirmed = false
		case r.Confirmed == "true" || r.Confirmed == "false":
			confirmed = r.Confirmed == "true"
		default:
			return ErrReply
		}
		return nil
	})
	return confirmed, err
}

// BlockState is what a node knows of one block. Height counts from the account's open block, which is 1.
type BlockState struct {
	Known, Confirmed bool
	Height           uint64
}

// Block is block_info in full, for the operator commands: recover and reconcile decide from where a block sits in
// the chain, not only from whether it is cemented.
func (c *Client) Block(ctx context.Context, hash nanocrypto.Hash) (BlockState, error) {
	var st BlockState
	err := c.call(ctx, c.rpc, map[string]string{"action": "block_info", "json_block": "true", "hash": hash.Hex()}, func(body []byte) error {
		var r struct{ Confirmed, Height, Error string }
		if err := json.Unmarshal(body, &r); err != nil {
			return ErrReply
		}
		if r.Error == "Block not found" {
			st = BlockState{}
			return nil
		}
		h, err := strconv.ParseUint(r.Height, 10, 64)
		if err != nil || h == 0 || (r.Confirmed != "true" && r.Confirmed != "false") {
			return ErrReply
		}
		st = BlockState{Known: true, Confirmed: r.Confirmed == "true", Height: h}
		return nil
	})
	return st, err
}

// NetworkMinimum is the network's base send threshold, as two endpoints agree it is (active_difficulty's
// network_minimum). It only ever raises the configured floor (D104), so a node cannot lower it, and the caller
// caps how far it can raise it.
func (c *Client) NetworkMinimum(ctx context.Context) (uint64, error) {
	var agreed uint64
	answers := 0
	var errs []error
	for _, u := range c.rpc {
		var got uint64
		err := c.call(ctx, []*url.URL{u}, map[string]string{"action": "active_difficulty"}, func(body []byte) error {
			var r struct {
				NetworkMinimum string `json:"network_minimum"`
			}
			if err := json.Unmarshal(body, &r); err != nil || len(r.NetworkMinimum) != 16 {
				return ErrReply
			}
			var err error
			got, err = strconv.ParseUint(r.NetworkMinimum, 16, 64)
			if err != nil || got == 0 {
				return ErrReply
			}
			return nil
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		answers++
		switch {
		case answers == 1:
			agreed = got
		case got == agreed:
			return agreed, nil
		default:
			return 0, fmt.Errorf("nanoclient: nodes disagree about the network minimum (%016x, %016x)", agreed, got)
		}
	}
	return 0, fmt.Errorf("nanoclient: two nodes must agree on the network minimum, %d answered: %w", answers, errors.Join(errs...))
}

// Process broadcasts a signed block; subtype is "send" for payouts and "open" for init-vault's receive.
// Re-sending a block the node already has succeeds, which is what makes a persisted block safe to rebroadcast
// after a crash (D6). ErrFork means someone else's block holds our place.
func (c *Client) Process(ctx context.Context, hash nanocrypto.Hash, block []byte, subtype string) error {
	req := map[string]any{"action": "process", "json_block": "true", "subtype": subtype, "block": json.RawMessage(block)}
	want := hash.Hex()
	return c.call(ctx, c.rpc, req, func(body []byte) error {
		var r struct{ Hash, Error string }
		if err := json.Unmarshal(body, &r); err != nil {
			return ErrReply
		}
		switch {
		case r.Error == "Old block" || (r.Error == "" && r.Hash == want):
			return nil
		case r.Error == "Fork":
			return ErrFork
		case r.Error != "":
			return rejected(r.Error)
		}
		return ErrReply
	})
}

// Receivable lists sends to pub not yet received, by send hash, with their amounts. Only init-vault uses it: the
// running service never receives (D22).
func (c *Client) Receivable(ctx context.Context, pub nanocrypto.PublicKey) (map[nanocrypto.Hash]*big.Int, error) {
	var out map[nanocrypto.Hash]*big.Int
	req := map[string]string{"action": "receivable", "account": pub.Address(), "count": "20", "threshold": "1"}
	err := c.call(ctx, c.rpc, req, func(body []byte) error {
		var r struct {
			Blocks json.RawMessage
			Error  string
		}
		if err := json.Unmarshal(body, &r); err != nil || r.Error != "" {
			return ErrReply
		}
		out = map[nanocrypto.Hash]*big.Int{}
		if string(r.Blocks) == `""` { // how a node says "none"
			return nil
		}
		var blocks map[string]string
		if err := json.Unmarshal(r.Blocks, &blocks); err != nil {
			return ErrReply
		}
		for hash, amount := range blocks {
			h, err := nanocrypto.ParseHash(hash)
			n, ok := new(big.Int).SetString(amount, 10)
			if err != nil || !ok || n.Sign() <= 0 || n.String() != amount {
				return ErrReply
			}
			out[h] = n
		}
		return nil
	})
	return out, err
}

// Work fetches proof of work for root and checks it locally, so a wrong or lazy answer moves on to the next
// endpoint instead of producing a block the network rejects.
func (c *Client) Work(ctx context.Context, root nanocrypto.Hash, threshold uint64) (uint64, error) {
	var work uint64
	req := map[string]string{"action": "work_generate", "hash": root.Hex(), "difficulty": fmt.Sprintf("%016x", threshold)}
	err := c.callTimeout(ctx, c.work, c.workTimeout, req, func(body []byte) error {
		var r struct{ Work string }
		if err := json.Unmarshal(body, &r); err != nil || len(r.Work) != 16 {
			return ErrReply
		}
		var err error
		if work, err = strconv.ParseUint(r.Work, 16, 64); err != nil || nanocrypto.Difficulty(root, work) < threshold {
			return fmt.Errorf("%w: work below threshold", ErrReply)
		}
		return nil
	})
	return work, err
}

func (c *Client) call(ctx context.Context, urls []*url.URL, req any, parse func([]byte) error) error {
	return c.callTimeout(ctx, urls, c.rpcTimeout, req, parse)
}

// callTimeout stops at the first endpoint whose reply parse accepts, or at a fork, which no other node can undo.
// Endpoints go in configured order, except that one which failed in the last minute goes after the rest; it is
// still tried, so a call never fails without asking everyone. A rejection is a node answering, not failing.
func (c *Client) callTimeout(ctx context.Context, urls []*url.URL, timeout time.Duration, req any, parse func([]byte) error) error {
	if len(urls) == 0 {
		return errors.New("nanoclient: no endpoints configured") // errors.Join of nothing would read as success
	}
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("nanoclient: %w", err)
	}
	var errs []error
	for _, u := range c.order(urls) {
		attempt, cancel := context.WithTimeout(ctx, timeout)
		reply, err := c.post(attempt, u, body)
		cancel()
		if err == nil {
			err = parse(reply)
		}
		if err == nil || errors.Is(err, ErrFork) {
			c.mark(u, false)
			return err
		}
		c.mark(u, !errors.Is(err, ErrRejected))
		// errors name the host only: the full URL may carry an API key in its query
		errs = append(errs, fmt.Errorf("nanoclient: %s: %w", u.Host, err))
		if ctx.Err() != nil {
			break
		}
	}
	return errors.Join(errs...)
}

func (c *Client) order(urls []*url.URL) []*url.URL {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	out := make([]*url.URL, 0, len(urls))
	var later []*url.URL
	for _, u := range urls {
		if t, ok := c.failedAt[u]; ok && now.Sub(t) < skipFor {
			later = append(later, u)
		} else {
			out = append(out, u)
		}
	}
	return append(out, later...)
}

func (c *Client) mark(u *url.URL, failed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if failed {
		c.failedAt[u] = c.now()
	} else {
		delete(c.failedAt, u)
	}
}

// the node's message goes into logs, so it is quoted and cut short
func rejected(msg string) error {
	if len(msg) > 100 {
		msg = msg[:100]
	}
	return fmt.Errorf("%w: %q", ErrRejected, msg)
}

func (c *Client) post(ctx context.Context, u *url.URL, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("building request failed")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err // url.Error's text repeats the full URL
		}
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxReply))
}
