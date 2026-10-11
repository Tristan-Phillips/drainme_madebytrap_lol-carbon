// Package fakenode is an in-process Nano node for tests (D61). It holds one account's chain, checks every block
// it is given the way the network would (hash, signature, work, previous, no balance increase) and records what
// each destination was paid, so a test can prove exactly what left the vault. The binary never imports it.
package fakenode

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanoclient"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

type Node struct {
	URL *url.URL

	mu        sync.Mutex
	threshold uint64
	account   nanocrypto.PublicKey
	frontier  nanocrypto.Hash
	balance   *big.Int
	blocks    map[nanocrypto.Hash]*entry // accepted blocks, keyed by hash
	history   map[nanocrypto.Hash]bool   // every frontier the account has had, to tell a fork from a gap
	heights   map[nanocrypto.Hash]uint64 // every block in the account's chain, including ones not sent through process
	height    uint64                     // the frontier's
	paid      map[nanocrypto.PublicKey]*big.Int
	incoming  map[nanocrypto.Hash]*big.Int // sends to the account not yet received, by send hash
	order     []nanocrypto.PublicKey       // destinations of accepted sends, in order
	processed int

	// PendingPolls is how many block_info calls a new block answers unconfirmed before it confirms.
	PendingPolls int
	// NetworkMinimum is what active_difficulty reports as the network's base send threshold; zero reports the
	// threshold the node checks work against.
	NetworkMinimum uint64
	// Intercept, when set, may replace the reply to an action; returning nil lets the node answer normally.
	Intercept func(action string) any
}

type entry struct{ polls int }

// New starts a node whose account already holds balance at frontier, as after init-vault. A zero frontier and
// balance is an account never opened, as before init-vault.
func New(t *testing.T, account nanocrypto.PublicKey, frontier nanocrypto.Hash, balance *big.Int, threshold uint64) *Node {
	n := &Node{threshold: threshold, account: account, frontier: frontier, balance: new(big.Int).Set(balance),
		blocks: map[nanocrypto.Hash]*entry{}, history: map[nanocrypto.Hash]bool{frontier: true}, paid: map[nanocrypto.PublicKey]*big.Int{},
		incoming: map[nanocrypto.Hash]*big.Int{}, heights: map[nanocrypto.Hash]uint64{}}
	if frontier != (nanocrypto.Hash{}) {
		n.heights[frontier], n.height = 1, 1
	}
	srv := httptest.NewServer(http.HandlerFunc(n.serve))
	t.Cleanup(srv.Close)
	n.URL, _ = url.Parse(srv.URL)
	return n
}

// Fund records a confirmed send of amount to the account, waiting to be received, as a wallet's send would.
func (n *Node) Fund(send nanocrypto.Hash, amount *big.Int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.incoming[send] = new(big.Int).Set(amount)
}

// Foreign appends a cemented block that did not come from drainme, as a wallet holding the same seed would: it
// becomes the frontier with the given balance, more for a receive and less for a send.
func (n *Node) Foreign(h nanocrypto.Hash, balance *big.Int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.height++
	n.frontier, n.balance = h, new(big.Int).Set(balance)
	n.history[h], n.heights[h] = true, n.height
}

// Head is the account's frontier and balance as the node holds them.
func (n *Node) Head() (nanocrypto.Hash, *big.Int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.frontier, new(big.Int).Set(n.balance)
}

// Paid is the total the node has accepted for dest.
func (n *Node) Paid(dest nanocrypto.PublicKey) *big.Int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if p := n.paid[dest]; p != nil {
		return new(big.Int).Set(p)
	}
	return new(big.Int)
}

// Order is every send's destination, in the order the node accepted them.
func (n *Node) Order() []nanocrypto.PublicKey {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]nanocrypto.PublicKey(nil), n.order...)
}

// Blocks is how many distinct blocks the node accepted; Processed counts every process call, rebroadcasts included.
func (n *Node) Blocks() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.blocks)
}

func (n *Node) Processed() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.processed
}

func (n *Node) serve(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Action, Account, Hash, Difficulty, Subtype string
		Block                                      json.RawMessage
	}
	if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&req) != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	var reply any
	if n.Intercept != nil {
		reply = n.Intercept(req.Action)
	}
	if reply == nil {
		reply = n.answer(req.Action, req.Account, req.Hash, req.Difficulty, req.Subtype, req.Block)
	}
	json.NewEncoder(w).Encode(reply)
}

func (n *Node) answer(action, account, hash, difficulty, subtype string, block []byte) any {
	switch action {
	case "account_info":
		if account != n.account.Address() || n.frontier == (nanocrypto.Hash{}) {
			return fail("Account not found")
		}
		return map[string]string{"frontier": n.frontier.Hex(), "balance": n.balance.String()}
	case "block_info":
		h, err := nanocrypto.ParseHash(hash)
		height, ok := n.heights[h]
		if err != nil || !ok {
			return fail("Block not found")
		}
		confirmed := true // blocks that did not come through process are cemented already
		if e := n.blocks[h]; e != nil {
			e.polls++
			confirmed = e.polls > n.PendingPolls
		}
		return map[string]string{"confirmed": fmt.Sprint(confirmed), "height": fmt.Sprint(height)}
	case "active_difficulty":
		m := n.NetworkMinimum
		if m == 0 {
			m = n.threshold
		}
		return map[string]string{"network_minimum": fmt.Sprintf("%016x", m)}
	case "receivable":
		if account != n.account.Address() || len(n.incoming) == 0 {
			return map[string]string{"blocks": ""} // a real node answers an empty string, not an empty object
		}
		blocks := map[string]string{}
		for h, amt := range n.incoming {
			blocks[h.Hex()] = amt.String()
		}
		return map[string]any{"blocks": blocks}
	case "process":
		n.processed++
		return n.process(subtype, block)
	case "work_generate":
		root, err := nanocrypto.ParseHash(hash)
		threshold, err2 := strconv.ParseUint(difficulty, 16, 64)
		if err != nil || err2 != nil {
			return fail("Bad request")
		}
		// test thresholds are tiny, so brute force is instant; work is a counter, any value that clears will do
		for work := uint64(0); ; work++ {
			if nanocrypto.Difficulty(root, work) >= threshold {
				return map[string]string{"work": fmt.Sprintf("%016x", work)}
			}
		}
	}
	return fail("Unknown command")
}

// process applies the network's rules for a send, or for the one receive that opens the account, in the order a
// node reports them.
func (n *Node) process(subtype string, data []byte) any {
	b, err := nanoclient.DecodeBlock(data)
	if err != nil {
		return fail("Block is invalid")
	}
	// a node checks the stated subtype against what the block does
	if want := map[bool]string{true: "open", false: "send"}[b.Previous == (nanocrypto.Hash{})]; subtype != want {
		return fail("Invalid block subtype")
	}
	h, _ := b.Hash() // DecodeBlock already checked the balance range
	root := [32]byte(b.Previous)
	if b.Previous == (nanocrypto.Hash{}) {
		root = b.Account // an account's first block works on its own key
	}
	switch {
	case n.blocks[h] != nil:
		return fail("Old block")
	case b.Account != n.account || !nanocrypto.Verify(b.Account, h, b.Signature):
		return fail("Bad signature")
	case nanocrypto.Difficulty(root, b.Work) < n.threshold:
		return fail("Block work is insufficient")
	case b.Previous == (nanocrypto.Hash{}) && n.frontier != (nanocrypto.Hash{}):
		return fail("Fork") // a second open block
	case b.Previous == (nanocrypto.Hash{}):
		amt := n.incoming[nanocrypto.Hash(b.Link)]
		if amt == nil {
			return fail("Gap source block")
		}
		if b.Balance.Cmp(amt) != 0 {
			return fail("Balance and amount delta do not match")
		}
		delete(n.incoming, nanocrypto.Hash(b.Link))
		n.balance, n.frontier = b.Balance, h
		n.blocks[h], n.history[h] = &entry{}, true
		n.height++
		n.heights[h] = n.height
		return map[string]string{"hash": h.Hex()}
	case b.Previous != n.frontier && n.history[b.Previous]:
		return fail("Fork")
	case b.Previous != n.frontier:
		return fail("Gap previous block")
	case b.Balance.Cmp(n.balance) > 0:
		return fail("Balance and amount delta do not match") // a receive, which drainme never sends
	}
	dest := nanocrypto.PublicKey(b.Link)
	if n.paid[dest] == nil {
		n.paid[dest] = new(big.Int)
	}
	n.paid[dest].Add(n.paid[dest], new(big.Int).Sub(n.balance, b.Balance))
	n.order = append(n.order, dest)
	n.balance, n.frontier = b.Balance, h
	n.blocks[h], n.history[h] = &entry{}, true
	n.height++
	n.heights[h] = n.height
	return map[string]string{"hash": h.Hex()}
}

func fail(msg string) map[string]string { return map[string]string{"error": msg} }
