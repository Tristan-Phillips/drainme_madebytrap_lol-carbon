package main

// The operator commands (D68, D101). status and backup only read, so they run beside a live serve. recover and
// reconcile change the books, so they take the data lock serve holds and refuse to run beside it. Both act only
// on what every node that answers agrees on, and change nothing unless the operator repeats the decision on the
// command line.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"syscall"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/config"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanoclient"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

const lockName = "drainme.lock"

// lockData takes the lock every command that writes the ledger holds. It is released when the process exits,
// however it exits, so a crash never leaves it stuck.
func lockData(dir string) (release func(), err error) {
	f, err := os.OpenFile(filepath.Join(dir, lockName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("another drainme command is using the ledger. If it is the service, stop it first: docker compose stop app")
	}
	return func() { f.Close() }, nil
}

func account(cfg *config.Config) (nanocrypto.PublicKey, error) {
	seed, err := config.ReadSeed(cfg.SeedFile)
	if err != nil {
		return nanocrypto.PublicKey{}, err
	}
	defer clear(seed[:])
	return nanocrypto.DeriveKey(seed, 0).Public(), nil
}

// endpoint is one RPC node asked on its own, so that nodes can be compared instead of failed over
type endpoint struct {
	host string
	node *nanoclient.Client
}

func endpoints(cfg *config.Config) []endpoint {
	var out []endpoint
	for _, u := range cfg.RPCURLs {
		out = append(out, endpoint{u.Host, nanoclient.New([]*url.URL{u}, nil, cfg.RPCTimeout)})
	}
	return out
}

// quorum is how many nodes must answer, all alike, before recover or reconcile acts: one node can be behind or
// wrong, so two whenever two are configured
func quorum(n int) int { return min(n, 2) }

type chainHead struct {
	frontier nanocrypto.Hash
	balance  *big.Int
}

// agreedHead is the account's head as every answering node reports it, or an error if they differ or too few answer
func agreedHead(ctx context.Context, eps []endpoint, pub nanocrypto.PublicKey, out io.Writer) (chainHead, error) {
	var got *chainHead
	answered := 0
	for _, e := range eps {
		f, b, err := e.node.AccountInfo(ctx, pub)
		if err != nil {
			fmt.Fprintf(out, "  %s: no answer (%v)\n", e.host, err)
			continue
		}
		fmt.Fprintf(out, "  %s: frontier %s, %s raw\n", e.host, f.Hex(), b)
		answered++
		if got == nil {
			got = &chainHead{f, b}
		} else if got.frontier != f || got.balance.Cmp(b) != 0 {
			return chainHead{}, errors.New("the nodes disagree about the account; wait a minute and run it again")
		}
	}
	if answered < quorum(len(eps)) {
		return chainHead{}, fmt.Errorf("%d of %d nodes answered, and %d must", answered, len(eps), quorum(len(eps)))
	}
	return *got, nil
}

// agreedBlock is what every answering node knows of block h
func agreedBlock(ctx context.Context, eps []endpoint, h nanocrypto.Hash, out io.Writer) (nanoclient.BlockState, error) {
	var got *nanoclient.BlockState
	answered := 0
	for _, e := range eps {
		st, err := e.node.Block(ctx, h)
		if err != nil {
			fmt.Fprintf(out, "  %s: no answer (%v)\n", e.host, err)
			continue
		}
		fmt.Fprintf(out, "  %s: %s\n", e.host, describe(st))
		answered++
		if got == nil {
			got = &st
		} else if *got != st {
			return nanoclient.BlockState{}, fmt.Errorf("the nodes disagree about block %s; wait a minute and run it again", h.Hex())
		}
	}
	if answered < quorum(len(eps)) {
		return nanoclient.BlockState{}, fmt.Errorf("%d of %d nodes answered, and %d must", answered, len(eps), quorum(len(eps)))
	}
	return *got, nil
}

func describe(st nanoclient.BlockState) string {
	switch {
	case !st.Known:
		return "never seen it"
	case st.Confirmed:
		return fmt.Sprintf("confirmed, height %d", st.Height)
	}
	return fmt.Sprintf("seen, not confirmed yet, height %d", st.Height)
}

// status prints the books and the chain side by side. It changes nothing and is safe while serve runs; it fails
// only when the books do not balance. failover is the usual client, for the one question any node can answer.
func status(ctx context.Context, l *ledger.Ledger, eps []endpoint, failover *nanoclient.Client, pub nanocrypto.PublicKey, sendThreshold uint64, out io.Writer) error {
	r, err := l.Report(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "vault      %s\n", pub.Address())
	if r.Frontier == nil {
		fmt.Fprintln(out, "ledger     not opened yet: run drainme init-vault")
		return r.Books
	}
	fmt.Fprintf(out, "ledger     frontier %s, %s raw\n", r.Frontier.Hex(), r.Chain)
	fmt.Fprintf(out, "           clicks may still take %s raw; %s raw credited in all\n", r.Available, r.Credited)
	fmt.Fprintf(out, "owed       %s raw to %d addresses, %s raw in %d queued withdrawals\n", r.Unpaid, r.Addresses, r.Queued, r.QueuedCount)
	fmt.Fprintf(out, "paid       %s raw in %d sends; %d recredited by recover\n", r.Paid, r.PaidCount, r.Recredited)
	if s := r.InFlight; s != nil {
		fmt.Fprintf(out, "in flight  withdrawal %d: %s raw to %s, block %s\n", s.ID, s.Amount, s.Address.Address(), s.Hash.Hex())
	} else {
		fmt.Fprintln(out, "in flight  nothing")
	}
	if r.Books != nil {
		fmt.Fprintf(out, "books      DO NOT BALANCE: %v\n", r.Books)
	} else {
		fmt.Fprintln(out, "books      balance")
	}
	fmt.Fprintln(out, "chain")
	for _, e := range eps {
		f, b, err := e.node.AccountInfo(ctx, pub)
		var verdict string
		switch {
		case err != nil:
			verdict = fmt.Sprintf("no answer (%v)", err)
		case f == *r.Frontier && b.Cmp(r.Chain) == 0:
			verdict = "matches the ledger"
		case r.InFlight != nil && f == r.InFlight.Hash && new(big.Int).Add(b, r.InFlight.Amount).Cmp(r.Chain) == 0:
			verdict = "matches, with the send in flight already published"
		default:
			verdict = fmt.Sprintf("DIFFERS: frontier %s, %s raw (see drainme reconcile)", f.Hex(), b)
		}
		fmt.Fprintf(out, "  %-24s %s\n", e.host, verdict)
	}
	receivableLine(ctx, failover, pub, out)
	switch network, err := failover.NetworkMinimum(ctx); {
	case err != nil:
		fmt.Fprintf(out, "threshold  send %016x configured; network minimum unknown (%v)\n", sendThreshold, err)
	case network > sendThreshold:
		fmt.Fprintf(out, "threshold  send %016x configured, BELOW the network minimum %016x: payouts follow it up to DRAINME_SEND_THRESHOLD_MAX\n", sendThreshold, network)
	default:
		fmt.Fprintf(out, "threshold  send %016x configured; network minimum %016x\n", sendThreshold, network)
	}
	return r.Books
}

// receivableLine reports sends waiting for the vault, which the service never receives (D22). Someone may have
// donated; receiving them in a wallet and then reconciling adds them to the vault.
func receivableLine(ctx context.Context, node *nanoclient.Client, pub nanocrypto.PublicKey, out io.Writer) {
	waiting, err := node.Receivable(ctx, pub)
	if err != nil {
		fmt.Fprintf(out, "waiting    unknown (%v)\n", err)
		return
	}
	total := new(big.Int)
	for _, amt := range waiting {
		total.Add(total, amt)
	}
	fmt.Fprintf(out, "waiting    %s raw in %d sends to the vault, not received (the service never receives)\n", total, len(waiting))
}

// backupName matches the files backup writes, and nothing else, so pruning can never touch another file
var backupName = regexp.MustCompile(`^ledger-\d{8}-\d{6}\.db$`)

// backup copies the ledger into dir and checks the copy: it must open, pass SQLite's integrity check, and balance.
// With keep above zero it then deletes the oldest copies beyond keep (D104: hourly, keeping 48).
func backup(ctx context.Context, l *ledger.Ledger, dir string, keep int, now time.Time, out io.Writer) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("backup: %w", err)
	}
	dest := filepath.Join(dir, "ledger-"+now.UTC().Format("20060102-150405")+".db")
	if err := l.Backup(ctx, dest); err != nil {
		return "", err
	}
	c, err := ledger.OpenReader(dest)
	if err != nil {
		return dest, fmt.Errorf("backup: the copy at %s does not open: %w", dest, err)
	}
	defer c.Close()
	if err := c.Integrity(ctx); err != nil {
		return dest, fmt.Errorf("backup: the copy at %s is damaged: %w", dest, err)
	}
	r, err := c.Report(ctx)
	if err == nil {
		err = r.Books
	}
	if err != nil {
		return dest, fmt.Errorf("backup: the copy at %s does not balance: %w", dest, err)
	}
	fi, err := os.Stat(dest)
	if err != nil {
		return dest, err
	}
	fmt.Fprintf(out, "Backed up to %s (%d bytes): %d addresses owed %s raw, %d sends paid. The copy opens and balances.\n",
		dest, fi.Size(), r.Addresses, r.Unpaid, r.PaidCount)
	if keep > 0 {
		if err := prune(dir, keep, out); err != nil {
			return dest, err
		}
	}
	return dest, nil
}

// prune deletes the oldest backups in dir beyond keep. Names sort by time, so the newest are kept.
func prune(dir string, keep int, out io.Writer) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("backup: pruning: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.Type().IsRegular() && backupName.MatchString(e.Name()) {
			names = append(names, e.Name())
		}
	}
	slices.Sort(names)
	for len(names) > keep {
		if err := os.Remove(filepath.Join(dir, names[0])); err != nil {
			return fmt.Errorf("backup: pruning: %w", err)
		}
		fmt.Fprintf(out, "Removed the old copy %s.\n", names[0])
		names = names[1:]
	}
	return nil
}

// recoverSend settles the send in flight from what the nodes agree on (D14). A cemented send is recorded as paid.
// One that can never confirm, because the account's chain has moved past the block it builds on, is put back in
// the address's balance, but only with recredit set. Anything else changes nothing: the block could still land,
// and the service rebroadcasts it.
func recoverSend(ctx context.Context, l *ledger.Ledger, eps []endpoint, pub nanocrypto.PublicKey, recredit bool, out io.Writer, log *slog.Logger) error {
	s, err := l.InFlight(ctx)
	if errors.Is(err, ledger.ErrNotInFlight) {
		fmt.Fprintln(out, "Nothing is in flight, so there is nothing to recover.")
		return nil
	}
	if err != nil {
		return err
	}
	frontier, _, err := l.Head(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "In flight: withdrawal %d, %s raw to %s\n  block %s, built on %s\nThe nodes on this block:\n",
		s.ID, s.Amount, s.Address.Address(), s.Hash.Hex(), frontier.Hex())
	st, err := agreedBlock(ctx, eps, s.Hash, out)
	if err != nil {
		return fmt.Errorf("recover: %w. Nothing was changed", err)
	}
	if st.Confirmed {
		if err := l.MarkConfirmed(ctx, s.ID); err != nil {
			return err
		}
		log.Warn("recover: send recorded as paid", "id", s.ID, "to", s.Address.Address(), "raw", s.Amount.String(), "hash", s.Hash.Hex())
		fmt.Fprintln(out, "It is on chain and confirmed, so it is now recorded as paid. Start the service.")
		return nil
	}
	if st.Known {
		return errors.New("recover: the network has this block but has not confirmed it yet. Start the service; it rebroadcasts the same block until it confirms. Nothing was changed")
	}

	fmt.Fprintln(out, "No node has it. The account, as the nodes report it:")
	head, err := agreedHead(ctx, eps, pub, out)
	if err != nil {
		return fmt.Errorf("recover: %w. Nothing was changed", err)
	}
	if head.frontier == frontier {
		return errors.New("recover: the account has not moved past the block this send builds on, so the network could still " +
			"accept it, and recrediting it now could pay it twice. Start the service, which rebroadcasts it. Nothing was changed")
	}
	if err := movedPast(ctx, eps, frontier, head.frontier, out); err != nil {
		return fmt.Errorf("recover: %w. Nothing was changed", err)
	}
	fmt.Fprintf(out, "The account's chain has moved past %s with blocks that are not this send, so this send can never confirm.\n", frontier.Hex())
	if !recredit {
		fmt.Fprintf(out, "To put %s raw back in %s's balance, run: drainme recover --recredit\n", s.Amount, s.Address.Address())
		return nil
	}
	if err := l.Recredit(ctx, s.ID); err != nil {
		return err
	}
	log.Warn("recover: send recredited", "id", s.ID, "to", s.Address.Address(), "raw", s.Amount.String(), "hash", s.Hash.Hex())
	fmt.Fprintf(out, "Recredited %s raw to %s. The ledger is now behind the chain: run drainme reconcile next.\n", s.Amount, s.Address.Address())
	return nil
}

// movedPast checks that to is a cemented block further along the account's chain than from, which is cemented
// too: then the block after from is cemented, and nothing else can ever take its place.
func movedPast(ctx context.Context, eps []endpoint, from, to nanocrypto.Hash, out io.Writer) error {
	fmt.Fprintf(out, "The nodes on the ledger's frontier %s:\n", from.Hex())
	was, err := agreedBlock(ctx, eps, from, out)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "The nodes on the chain's frontier %s:\n", to.Hex())
	now, err := agreedBlock(ctx, eps, to, out)
	if err != nil {
		return err
	}
	switch {
	case !was.Known:
		return fmt.Errorf("no node knows the ledger's frontier %s, so the chain cannot be shown to follow it", from.Hex())
	case !was.Confirmed || !now.Confirmed:
		return errors.New("the chain is still settling; wait a minute and run it again")
	case now.Height <= was.Height:
		return errors.New("the chain's frontier is not ahead of the ledger's; a node may be behind")
	}
	return nil
}

// reconcile brings the ledger's head up to the chain after blocks drainme did not make: a wallet holding the seed
// received a donation, or spent from the vault (D24). It adopts the chain's head only with adopt set to that
// head's hash, and only if the chain still covers everything owed.
func reconcile(ctx context.Context, l *ledger.Ledger, eps []endpoint, pub nanocrypto.PublicKey, adopt *nanocrypto.Hash, out io.Writer, log *slog.Logger) error {
	if _, err := l.InFlight(ctx); err == nil {
		return errors.New("reconcile: a send is in flight; run drainme recover first")
	} else if !errors.Is(err, ledger.ErrNotInFlight) {
		return err
	}
	frontier, bal, err := l.Head(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "Ledger: frontier %s, %s raw. The nodes:\n", frontier.Hex(), bal)
	head, err := agreedHead(ctx, eps, pub, out)
	if err != nil {
		return fmt.Errorf("reconcile: %w. Nothing was changed", err)
	}
	if head.frontier == frontier && head.balance.Cmp(bal) == 0 {
		fmt.Fprintln(out, "The ledger and the chain agree. Nothing to do.")
		return nil
	}
	if err := movedPast(ctx, eps, frontier, head.frontier, out); err != nil {
		return fmt.Errorf("reconcile: %w. Nothing was changed", err)
	}
	recredited, err := l.Recredited(ctx)
	if err != nil {
		return err
	}
	for _, h := range recredited {
		fmt.Fprintf(out, "The nodes on recredited send %s:\n", h.Hex())
		st, err := agreedBlock(ctx, eps, h, out)
		if err != nil {
			return fmt.Errorf("reconcile: %w. Nothing was changed", err)
		}
		if st.Known {
			return fmt.Errorf("reconcile: send %s was recredited by recover, but it is on chain after all, so its address was paid "+
				"and is owed it again. The ledger cannot undo that by itself; nothing was changed", h.Hex())
		}
	}
	_, available, _, err := l.Totals(ctx)
	if err != nil {
		return err
	}
	delta := new(big.Int).Sub(head.balance, bal)
	after := new(big.Int).Add(available, delta)
	fmt.Fprintf(out, "The chain has moved on without drainme: frontier %s, %s raw (%+d raw).\n", head.frontier.Hex(), head.balance, delta)
	if after.Sign() < 0 {
		return fmt.Errorf("reconcile: the chain holds %s raw less than is already owed; adopting it would owe more than the vault has. Nothing was changed", new(big.Int).Neg(after))
	}
	fmt.Fprintf(out, "Adopting it leaves %s raw for clicks to take (now %s).\n", after, available)
	if adopt == nil {
		fmt.Fprintf(out, "To adopt it, run: drainme reconcile --adopt %s\n", head.frontier.Hex())
		return nil
	}
	if *adopt != head.frontier {
		return fmt.Errorf("reconcile: the chain's frontier is %s, not the %s given. Nothing was changed", head.frontier.Hex(), adopt.Hex())
	}
	if err := l.SetHead(ctx, head.frontier, head.balance); err != nil {
		return err
	}
	log.Warn("reconcile: adopted the chain's head", "frontier", head.frontier.Hex(), "raw", head.balance.String(), "change", delta.String())
	fmt.Fprintln(out, "Adopted. Start the service.")
	return nil
}
