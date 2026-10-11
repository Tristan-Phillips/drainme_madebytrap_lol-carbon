package main

// init-vault opens the hot wallet, once (D22, D69). `drainme init-vault` prints the address to fund. Once the
// funds are waiting, `drainme init-vault <raw>` receives them into the brand-new account and records the result
// as the ledger's first head; the exact amount is the operator's confirmation. It is the only code that receives.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/config"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanoclient"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

func runInitVault(args []string) error {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		return err
	}
	rep, err := nanocrypto.ParseAddress(cfg.Representative)
	if err != nil {
		return fmt.Errorf("init-vault: DRAINME_REPRESENTATIVE must be a nano_ address (D8)")
	}
	seed, err := config.ReadSeed(cfg.SeedFile)
	if err != nil {
		return err
	}
	key := nanocrypto.DeriveKey(seed, 0) // D85
	clear(seed[:])
	if len(args) == 0 {
		fmt.Printf("The vault's address (account 0 of the seed):\n\n  %s\n\n"+
			"Send the funds there, then run: drainme init-vault <the exact amount in raw>\n", key.Public().Address())
		return nil
	}
	want, resume, err := parseInitArgs(args)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	release, err := lockData(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("init-vault: %w", err)
	}
	defer release()
	l, err := ledger.Open(filepath.Join(cfg.DataDir, "ledger.db"), cfg.QueueMax)
	if err != nil {
		return err
	}
	defer l.Close()
	node := nanoclient.New(cfg.RPCURLs, cfg.WorkURLs, cfg.RPCTimeout)
	return initVault(ctx, l, node, key, rep, cfg.ReceiveThreshold, want, resume, os.Stdout, time.Second)
}

func parseInitArgs(args []string) (*big.Int, *nanocrypto.Hash, error) {
	want, ok := new(big.Int).SetString(args[0], 10)
	if !ok || want.Sign() <= 0 || want.BitLen() > 128 || want.String() != args[0] {
		return nil, nil, errors.New("init-vault: the amount must be the exact raw amount waiting, a positive whole number")
	}
	switch {
	case len(args) == 1:
		return want, nil, nil
	case len(args) == 3 && args[1] == "--resume":
		h, err := nanocrypto.ParseHash(args[2])
		if err != nil {
			return nil, nil, errors.New("init-vault: --resume takes the 64-character block hash an earlier run printed")
		}
		return want, &h, nil
	}
	return nil, nil, errors.New("usage: drainme init-vault [<raw> [--resume <hash>]]")
}

func initVault(ctx context.Context, l *ledger.Ledger, node *nanoclient.Client, key nanocrypto.PrivateKey, rep nanocrypto.PublicKey,
	threshold uint64, want *big.Int, resume *nanocrypto.Hash, out io.Writer, poll time.Duration) error {
	if _, _, err := l.Head(ctx); err == nil {
		return errors.New("init-vault: the ledger already has a vault; init-vault runs once")
	} else if !errors.Is(err, ledger.ErrNoHead) {
		return err
	}
	pub := key.Public()
	if resume != nil {
		// a hash that is not the account's frontier would never confirm as one, so refuse now instead of waiting
		frontier, _, err := node.AccountInfo(ctx, pub)
		if err != nil {
			return err
		}
		if frontier != *resume {
			return fmt.Errorf("init-vault: the account's frontier is %s, not %s; nothing was recorded", frontier.Hex(), resume.Hex())
		}
		return finish(ctx, l, node, pub, *resume, want, out, poll)
	}

	switch _, _, err := node.AccountInfo(ctx, pub); {
	case errors.Is(err, nanoclient.ErrUnopened):
	case err == nil:
		return fmt.Errorf("init-vault: %s is already open on chain, and init-vault only opens a fresh account. "+
			"If an earlier run was interrupted, finish it with --resume and the hash it printed", pub.Address())
	default:
		return err
	}
	incoming, err := node.Receivable(ctx, pub)
	if err != nil {
		return err
	}
	if len(incoming) != 1 {
		for h, amt := range incoming {
			fmt.Fprintf(out, "  waiting: %s raw in send %s\n", amt, h.Hex())
		}
		return fmt.Errorf("init-vault: want exactly one send waiting for %s, found %d; nothing was done", pub.Address(), len(incoming))
	}
	var source nanocrypto.Hash
	var amount *big.Int
	for h, amt := range incoming { // the only one
		source, amount = h, amt
	}
	if amount.Cmp(want) != 0 {
		return fmt.Errorf("init-vault: the waiting send is %s raw, not the %s given; nothing was done", amount, want)
	}

	// an account's first block has no previous, and its work is done on the account's own key
	work, err := node.Work(ctx, nanocrypto.Hash(pub), threshold)
	if err != nil {
		return err
	}
	b := nanoclient.Block{StateBlock: nanocrypto.StateBlock{Account: pub, Representative: rep, Balance: amount, Link: source}, Work: work}
	h, err := b.Hash()
	if err != nil {
		return err
	}
	b.Signature = key.Sign(h)
	// the hash depends on neither the work nor anything random, so a rerun builds this same block
	fmt.Fprintf(out, "Opening %s with %s raw from send %s.\nIf this is interrupted, finish with: drainme init-vault %s --resume %s\n",
		pub.Address(), amount, source.Hex(), amount, h.Hex())
	if err := node.Process(ctx, h, b.Encode(), "open"); err != nil {
		return err
	}
	return finish(ctx, l, node, pub, h, amount, out, poll)
}

// finish waits for the open block h to be confirmed and records it as the ledger's first head. What it records is
// our own block's hash and amount (D86 R6); the node is only asked to confirm that the block is cemented and is
// the account's frontier holding exactly that amount, and any other answer records nothing.
func finish(ctx context.Context, l *ledger.Ledger, node *nanoclient.Client, pub nanocrypto.PublicKey, h nanocrypto.Hash,
	amount *big.Int, out io.Writer, poll time.Duration) error {
	for {
		ok, err := node.Confirmed(ctx, h)
		if err == nil && ok {
			break
		}
		if err != nil {
			fmt.Fprintf(out, "waiting for confirmation: %v\n", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("init-vault: stopped before confirmation; finish with --resume %s", h.Hex())
		case <-time.After(poll):
		}
	}
	frontier, bal, err := node.AccountInfo(ctx, pub)
	if err != nil {
		return err
	}
	if frontier != h || bal.Cmp(amount) != 0 {
		return fmt.Errorf("init-vault: the node reports frontier %s holding %s raw, not open block %s holding %s; nothing was recorded",
			frontier.Hex(), bal, h.Hex(), amount)
	}
	if err := l.SetHead(ctx, h, amount); err != nil {
		return err
	}
	fmt.Fprintf(out, "Vault open: %s raw, frontier %s. Start the service: docker compose up -d\n", amount, h.Hex())
	return nil
}
