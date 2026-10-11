// Package ledger is drainme's money state in SQLite: who is owed what, the withdrawal queue, and the vault.
// Structural rules live in schema.sql where no Go bug can bypass them; 128-bit arithmetic happens here inside
// a single transaction per operation, and Audit re-derives every total from the rows.
package ledger

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

// Version 1 (no vault.frontier) was never deployed and is refused by name. Version 2 ran the dust test and gained
// balances.last_active in 3; version 3 ran it next and gains the leaderboard's index in 4.
const schemaVersion = 4

// from2 is the v2 to v3 migration. Existing balances start at last_active 0, so they count as long idle.
const from2 = `ALTER TABLE balances ADD COLUMN last_active INTEGER NOT NULL DEFAULT 0 CHECK (last_active >= 0);
CREATE INDEX balances_idle ON balances (last_active);`

// from3 is the v3 to v4 migration: an index only, so no row changes
const from3 = `CREATE INDEX withdrawals_paid_by_address ON withdrawals (address) WHERE state = 'confirmed';`

var (
	ErrVaultExhausted    = errors.New("ledger: credit would exceed what the vault can pay")
	ErrOvercommitted     = errors.New("ledger: chain balance is below what is already owed")
	ErrNothingToWithdraw = errors.New("ledger: address has no unpaid balance")
	ErrNothingIdle       = errors.New("ledger: no balance has been idle that long")
	ErrQueueEmpty        = errors.New("ledger: no queued withdrawal")
	ErrQueueFull         = errors.New("ledger: payout queue is full; the balance stays credited")
	ErrChanged           = errors.New("ledger: withdrawal changed or is no longer queued")
	ErrNotInFlight       = errors.New("ledger: withdrawal is not awaiting confirmation")
	ErrInFlight          = errors.New("ledger: a signed send is awaiting confirmation; settle it first")
	ErrNoHead            = errors.New("ledger: vault has no frontier yet; run init-vault")
	ErrZeroFrontier      = errors.New("ledger: frontier must be a real block hash; init-vault opens the account first")
	ErrInvariant         = errors.New("ledger: invariant violated")
)

// Withdrawal is one queued payout.
type Withdrawal struct {
	ID      int64
	Address nanocrypto.PublicKey
	Amount  *big.Int
}

// Signed is the send awaiting confirmation, with the exact block that was persisted before broadcast, so a
// restarted worker rebroadcasts it instead of building a new one (README §3).
type Signed struct {
	Withdrawal
	Hash  nanocrypto.Hash
	Block []byte
}

type Ledger struct {
	db       *sql.DB // the single writer: every transaction
	read     *sql.DB // plain reads, which WAL lets run beside the writer instead of queueing behind it
	queueMax int64
	file     string
}

// Open creates or opens the ledger at path. queueMax bounds how many addresses can wait for a payout at once
// (F1): beyond it Withdraw returns ErrQueueFull and nothing is debited. One connection only: SQLite has one
// writer, and a pool of connections is how SQLITE_BUSY happens (stack-pitfalls P5).
func Open(path string, queueMax int) (*Ledger, error) {
	if queueMax < 1 {
		return nil, fmt.Errorf("ledger: queue cap must be at least 1, got %d", queueMax)
	}
	dsn := "file:" + url.PathEscape(path) +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("ledger: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	l := &Ledger{db: db, queueMax: int64(queueMax), file: path}
	if err := l.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	// opened after migrate, so the file and its WAL exist; query_only makes a stray write here an error
	read, err := sql.Open("sqlite", "file:"+url.PathEscape(path)+"?_pragma=busy_timeout(5000)&_pragma=query_only(1)")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("ledger: open: %w", err)
	}
	read.SetMaxOpenConns(4)
	l.read = read
	return l, nil
}

func (l *Ledger) Close() error {
	if l.db == l.read { // OpenReader: one pool
		return l.read.Close()
	}
	return errors.Join(l.read.Close(), l.db.Close())
}

func (l *Ledger) migrate() error {
	var v int
	if err := l.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		return fmt.Errorf("ledger: reading schema version: %w", err)
	}
	switch v {
	case schemaVersion:
		return nil
	case 0, 2, 3:
		ddl := map[int]string{0: schema, 2: from2 + "\n" + from3, 3: from3}[v]
		return l.tx(context.Background(), func(tx *sql.Tx) error {
			if _, err := tx.Exec(ddl); err != nil {
				return fmt.Errorf("ledger: migrating schema from version %d: %w", v, err)
			}
			_, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", schemaVersion))
			return err
		})
	default:
		return fmt.Errorf("ledger: unknown schema version %d", v)
	}
}

// SetHead records the hot wallet's confirmed frontier and balance, for init-vault and operator reconcile only;
// the payout worker advances the head itself through MarkConfirmed. The balance difference goes to or comes from
// what clicks may still claim; it fails if the chain holds less than is already owed. It refuses while a send is
// in flight: the chain may or may not already show that send, so the caller settles it first.
func (l *Ledger) SetHead(ctx context.Context, frontier nanocrypto.Hash, bal *big.Int) error {
	// a zero previous means an unopened account, whose first block is a receive: never something we build on (D22)
	if frontier == (nanocrypto.Hash{}) {
		return ErrZeroFrontier
	}
	return l.tx(ctx, func(tx *sql.Tx) error {
		if err := noneInFlight(tx); err != nil {
			return err
		}
		v, err := readVault(tx)
		if err != nil {
			return err
		}
		v.available.Add(v.available, new(big.Int).Sub(bal, v.chain))
		if v.available.Sign() < 0 {
			return ErrOvercommitted
		}
		v.chain, v.frontier = bal, &frontier
		return v.write(tx)
	})
}

// Head is the frontier and balance every send is built on (D86 R6): only our own confirmed blocks move it.
// One row, so a plain read: no transaction and no write lock.
func (l *Ledger) Head(ctx context.Context) (nanocrypto.Hash, *big.Int, error) {
	v, err := readVault(l.read)
	if err != nil {
		return nanocrypto.Hash{}, nil, err
	}
	if v.frontier == nil {
		return nanocrypto.Hash{}, nil, ErrNoHead
	}
	return *v.frontier, v.chain, nil
}

// Credit applies a batch of click counts all-or-nothing. ErrVaultExhausted is the "halt the service" case (D20).
func (l *Ledger) Credit(ctx context.Context, clicks map[nanocrypto.PublicKey]uint64) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		v, err := readVault(tx)
		if err != nil {
			return err
		}
		now := time.Now().Unix()
		// a batch can hold thousands of addresses (ingest's bound): parsing these two statements once instead of per
		// address is most of the cost of a large flush, which holds the only connection while it runs
		read, err := tx.Prepare(`SELECT unpaid FROM balances WHERE address = ?`)
		if err != nil {
			return fmt.Errorf("ledger: crediting: %w", err)
		}
		defer read.Close()
		upsert, err := tx.Prepare(`INSERT INTO balances (address, unpaid, last_active) VALUES (?, ?, ?)
			ON CONFLICT (address) DO UPDATE SET unpaid = excluded.unpaid, last_active = excluded.last_active`)
		if err != nil {
			return fmt.Errorf("ledger: crediting: %w", err)
		}
		defer upsert.Close()
		for pub, n := range clicks {
			if n == 0 {
				continue
			}
			amt := new(big.Int).SetUint64(n)
			if v.available.Cmp(amt) < 0 {
				return ErrVaultExhausted
			}
			v.available.Sub(v.available, amt)
			v.credited.Add(v.credited, amt)
			addr := pub.Address()
			unpaid, err := scanUnpaid(read.QueryRow(addr))
			if err != nil {
				return err
			}
			if _, err := upsert.Exec(addr, unpaid.Add(unpaid, amt).String(), now); err != nil {
				return fmt.Errorf("ledger: crediting: %w", err)
			}
		}
		return v.write(tx)
	})
}

// Withdraw debits the address's whole unpaid balance and queues it in the same transaction (debit first, then
// enqueue). If the address already has a queued withdrawal the amount merges into it. Returns the queued total
// and the position in line, counting the send in flight. ErrQueueFull leaves the balance untouched.
func (l *Ledger) Withdraw(ctx context.Context, pub nanocrypto.PublicKey) (*big.Int, int64, error) {
	var total *big.Int
	var position int64
	err := l.tx(ctx, func(tx *sql.Tx) error {
		var err error
		total, position, err = l.withdraw(tx, pub)
		return err
	})
	if err != nil {
		return nil, 0, err
	}
	return total, position, nil
}

// SweepIdle queues the balance of the address idle longest, if its last credit came before idleSince (D96). The
// payout worker calls it only when no withdrawal is waiting or in flight, so a person never waits behind more
// than one sweep. It goes through the same debit and enqueue as Withdraw.
func (l *Ledger) SweepIdle(ctx context.Context, idleSince time.Time) (nanocrypto.PublicKey, *big.Int, error) {
	var pub nanocrypto.PublicKey
	var total *big.Int
	err := l.tx(ctx, func(tx *sql.Tx) error {
		var addr string
		err := tx.QueryRow(`SELECT address FROM balances WHERE last_active < ? ORDER BY last_active LIMIT 1`, idleSince.Unix()).Scan(&addr)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNothingIdle
		}
		if err != nil {
			return fmt.Errorf("ledger: finding idle balances: %w", err)
		}
		if pub, err = nanocrypto.ParseAddress(addr); err != nil {
			return fmt.Errorf("%w: stored address", ErrInvariant)
		}
		total, _, err = l.withdraw(tx, pub)
		return err
	})
	return pub, total, err
}

// withdraw debits pub's whole unpaid balance and enqueues it, inside the caller's transaction.
func (l *Ledger) withdraw(tx *sql.Tx, pub nanocrypto.PublicKey) (total *big.Int, position int64, err error) {
	unpaid, err := readUnpaid(tx, pub)
	if err != nil {
		return nil, 0, err
	}
	if unpaid.Sign() == 0 {
		return nil, 0, ErrNothingToWithdraw
	}
	addr := pub.Address()
	// the whole balance moves to the queue, so the address no longer holds a row or a slot under the cap
	if _, err := tx.Exec(`DELETE FROM balances WHERE address = ?`, addr); err != nil {
		return nil, 0, fmt.Errorf("ledger: debiting: %w", err)
	}
	var id int64
	var queued string
	switch err := tx.QueryRow(`SELECT id, amount FROM withdrawals WHERE address = ? AND state = 'queued'`, addr).Scan(&id, &queued); {
	case errors.Is(err, sql.ErrNoRows):
		// a merge adds no row, so only a new entry can hit the cap; returning here rolls back the debit
		var queued int64
		if err := tx.QueryRow(`SELECT count(*) FROM withdrawals WHERE state = 'queued'`).Scan(&queued); err != nil {
			return nil, 0, fmt.Errorf("ledger: reading queue: %w", err)
		}
		if queued >= l.queueMax {
			return nil, 0, ErrQueueFull
		}
		total = unpaid
		res, err := tx.Exec(`INSERT INTO withdrawals (address, amount, state, created) VALUES (?, ?, 'queued', ?)`,
			addr, total.String(), time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return nil, 0, fmt.Errorf("ledger: enqueueing: %w", err)
		}
		if id, err = res.LastInsertId(); err != nil {
			return nil, 0, fmt.Errorf("ledger: enqueueing: %w", err)
		}
	case err != nil:
		return nil, 0, fmt.Errorf("ledger: reading queue: %w", err)
	default:
		prev, err := parseAmount(queued)
		if err != nil {
			return nil, 0, err
		}
		total = prev.Add(prev, unpaid)
		if _, err := tx.Exec(`UPDATE withdrawals SET amount = ? WHERE id = ?`, total.String(), id); err != nil {
			return nil, 0, fmt.Errorf("ledger: merging: %w", err)
		}
	}
	err = tx.QueryRow(`SELECT count(*) FROM withdrawals WHERE state IN ('queued', 'signed') AND id <= ?`, id).Scan(&position)
	return total, position, err
}

// Next returns the oldest queued withdrawal without changing it.
func (l *Ledger) Next(ctx context.Context) (Withdrawal, error) {
	var w Withdrawal
	var addr, amt string
	err := l.read.QueryRowContext(ctx, `SELECT id, address, amount FROM withdrawals WHERE state = 'queued' ORDER BY id LIMIT 1`).Scan(&w.ID, &addr, &amt)
	if errors.Is(err, sql.ErrNoRows) {
		return w, ErrQueueEmpty
	}
	if err != nil {
		return w, fmt.Errorf("ledger: reading queue: %w", err)
	}
	if w.Address, err = nanocrypto.ParseAddress(addr); err != nil {
		return w, fmt.Errorf("ledger: stored address: %w", err)
	}
	w.Amount, err = parseAmount(amt)
	return w, err
}

// MarkSigned persists the signed block before it is broadcast. It succeeds only if the row is still queued with
// exactly the amount the block pays; a merge in between returns ErrChanged and the caller re-reads Next.
// ErrInFlight means an earlier send is unconfirmed: the chain is sequential, so it must settle first.
func (l *Ledger) MarkSigned(ctx context.Context, w Withdrawal, hash nanocrypto.Hash, block []byte) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		if err := noneInFlight(tx); err != nil {
			return err
		}
		res, err := tx.Exec(`UPDATE withdrawals SET state = 'signed', block_hash = ?, block = ?
			WHERE id = ? AND state = 'queued' AND amount = ?`, hash.Hex(), string(block), w.ID, w.Amount.String())
		if err != nil {
			return fmt.Errorf("ledger: marking signed: %w", err)
		}
		return oneRow(res, ErrChanged)
	})
}

// InFlight returns the signed send awaiting confirmation, or ErrNotInFlight.
func (l *Ledger) InFlight(ctx context.Context) (Signed, error) {
	var s Signed
	var addr, amt, hash, block string
	err := l.read.QueryRowContext(ctx, `SELECT id, address, amount, block_hash, block FROM withdrawals WHERE state = 'signed'`).
		Scan(&s.ID, &addr, &amt, &hash, &block)
	if errors.Is(err, sql.ErrNoRows) {
		return s, ErrNotInFlight
	}
	if err != nil {
		return s, fmt.Errorf("ledger: reading in-flight send: %w", err)
	}
	if s.Address, err = nanocrypto.ParseAddress(addr); err != nil {
		return s, fmt.Errorf("ledger: stored address: %w", err)
	}
	if s.Amount, err = parseAmount(amt); err != nil {
		return s, err
	}
	if s.Hash, err = parseHash(hash); err != nil {
		return s, err
	}
	s.Block = []byte(block)
	return s, nil
}

// MarkConfirmed records that the in-flight send is cemented: the amount leaves both the debt and the chain balance,
// and the block becomes the frontier the next send is built on.
func (l *Ledger) MarkConfirmed(ctx context.Context, id int64) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		_, paid, hash, err := readSigned(tx, id)
		if err != nil {
			return err
		}
		v, err := readVault(tx)
		if err != nil {
			return err
		}
		v.chain.Sub(v.chain, paid)
		v.frontier = &hash
		if _, err := tx.Exec(`UPDATE withdrawals SET state = 'confirmed' WHERE id = ?`, id); err != nil {
			return fmt.Errorf("ledger: marking confirmed: %w", err)
		}
		return v.write(tx)
	})
}

// Recredit is the manual recovery path (D14): an in-flight send that the operator has verified is NOT on chain
// goes back to the address's unpaid balance. Nothing calls this automatically.
func (l *Ledger) Recredit(ctx context.Context, id int64) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		addr, amt, _, err := readSigned(tx, id)
		if err != nil {
			return err
		}
		pub, err := nanocrypto.ParseAddress(addr)
		if err != nil {
			return fmt.Errorf("ledger: stored address: %w", err)
		}
		unpaid, err := readUnpaid(tx, pub)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE withdrawals SET state = 'recredited' WHERE id = ?`, id); err != nil {
			return fmt.Errorf("ledger: recrediting: %w", err)
		}
		_, err = tx.Exec(`INSERT INTO balances (address, unpaid, last_active) VALUES (?, ?, ?)
			ON CONFLICT (address) DO UPDATE SET unpaid = excluded.unpaid, last_active = excluded.last_active`,
			addr, unpaid.Add(unpaid, amt).String(), time.Now().Unix())
		if err != nil {
			return fmt.Errorf("ledger: recrediting: %w", err)
		}
		return nil
	})
}

// Totals is the vault row as the page shows it (D21): the confirmed chain balance, what clicks may still claim,
// and every raw ever credited. One row, so a plain read with no lock.
func (l *Ledger) Totals(ctx context.Context) (chain, available, credited *big.Int, err error) {
	v, err := readVault(l.read)
	if err != nil {
		return nil, nil, nil, err
	}
	return v.chain, v.available, v.credited, nil
}

// QueueLength is how many withdrawals are waiting or being sent.
func (l *Ledger) QueueLength(ctx context.Context) (int64, error) {
	var n int64
	err := l.read.QueryRowContext(ctx, `SELECT count(*) FROM withdrawals WHERE state IN ('queued', 'signed')`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("ledger: reading queue: %w", err)
	}
	return n, nil
}

// Addresses is how many addresses are owed money, the number the address cap (D89) is checked against.
func (l *Ledger) Addresses(ctx context.Context) (int64, error) {
	var n int64
	if err := l.read.QueryRowContext(ctx, `SELECT addresses FROM vault WHERE id = 1`).Scan(&n); err != nil {
		return 0, fmt.Errorf("ledger: reading address count: %w", err)
	}
	return n, nil
}

// Balance is the address's unpaid balance; zero for an address not owed anything. A single read, so no transaction.
func (l *Ledger) Balance(ctx context.Context, pub nanocrypto.PublicKey) (*big.Int, error) {
	return readUnpaid(l.read, pub)
}

// Audit re-derives the totals from every row and checks both conservation laws, plus the address count. It reads
// the whole history under the write lock, so it is for boot and operator checks, not a hot path:
//
//	available + unpaid + in flight = chain balance
//	credited = unpaid + in flight + confirmed
func (l *Ledger) Audit(ctx context.Context) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		v, err := readVault(tx)
		if err != nil {
			return err
		}
		s, err := readSums(tx)
		if err != nil {
			return err
		}
		return s.check(v)
	})
}

// sums are the totals re-derived from the rows, which the vault row's own figures must agree with
type sums struct {
	unpaid, owedQueue, confirmed *big.Int // owedQueue: queued plus in flight
	rows, counted                int64    // balances rows, and the vault's count of them
}

func readSums(tx *sql.Tx) (s sums, err error) {
	if s.unpaid, err = sumColumn(tx, `SELECT unpaid FROM balances`); err != nil {
		return s, err
	}
	if s.owedQueue, err = sumColumn(tx, `SELECT amount FROM withdrawals WHERE state IN ('queued', 'signed')`); err != nil {
		return s, err
	}
	if s.confirmed, err = sumColumn(tx, `SELECT amount FROM withdrawals WHERE state = 'confirmed'`); err != nil {
		return s, err
	}
	if err := tx.QueryRow(`SELECT (SELECT count(*) FROM balances), addresses FROM vault WHERE id = 1`).Scan(&s.rows, &s.counted); err != nil {
		return s, fmt.Errorf("ledger: counting addresses: %w", err)
	}
	return s, nil
}

func (s sums) check(v vault) error {
	owed := new(big.Int).Add(s.unpaid, s.owedQueue)
	if new(big.Int).Add(v.available, owed).Cmp(v.chain) != 0 {
		return fmt.Errorf("%w: available %s + owed %s != chain %s", ErrInvariant, v.available, owed, v.chain)
	}
	if new(big.Int).Add(owed, s.confirmed).Cmp(v.credited) != 0 {
		return fmt.Errorf("%w: owed %s + confirmed %s != credited %s", ErrInvariant, owed, s.confirmed, v.credited)
	}
	if s.rows != s.counted {
		return fmt.Errorf("%w: %d owed addresses, vault counts %d", ErrInvariant, s.rows, s.counted)
	}
	return nil
}

type vault struct {
	chain, available, credited *big.Int
	frontier                   *nanocrypto.Hash // nil until init-vault
}

func readVault(q rowQuerier) (vault, error) {
	var c, a, cr string
	var f sql.NullString
	var v vault
	if err := q.QueryRow(`SELECT chain_balance, available, credited, frontier FROM vault WHERE id = 1`).Scan(&c, &a, &cr, &f); err != nil {
		return vault{}, fmt.Errorf("ledger: reading vault: %w", err)
	}
	if f.Valid {
		h, err := parseHash(f.String)
		if err != nil {
			return v, err
		}
		v.frontier = &h
	}
	var err error
	if v.chain, err = parseAmount(c); err != nil {
		return v, err
	}
	if v.available, err = parseAmount(a); err != nil {
		return v, err
	}
	v.credited, err = parseAmount(cr)
	return v, err
}

// write stores the vault row; a negative value is refused here and again by the schema's format CHECKs.
func (v vault) write(tx *sql.Tx) error {
	for _, n := range []*big.Int{v.chain, v.available, v.credited} {
		if n.Sign() < 0 || n.Cmp(maxAmount) > 0 {
			return fmt.Errorf("%w: vault amount out of range", ErrInvariant)
		}
	}
	var frontier any // NULL until init-vault
	if v.frontier != nil {
		frontier = v.frontier.Hex()
	}
	_, err := tx.Exec(`UPDATE vault SET chain_balance = ?, available = ?, credited = ?, frontier = ? WHERE id = 1`,
		v.chain.String(), v.available.String(), v.credited.String(), frontier)
	if err != nil {
		return fmt.Errorf("ledger: writing vault: %w", err)
	}
	return nil
}

// rowQuerier is satisfied by both *sql.DB and *sql.Tx.
type rowQuerier interface {
	QueryRow(query string, args ...any) *sql.Row
}

func readUnpaid(q rowQuerier, pub nanocrypto.PublicKey) (*big.Int, error) {
	return scanUnpaid(q.QueryRow(`SELECT unpaid FROM balances WHERE address = ?`, pub.Address()))
}

func scanUnpaid(row *sql.Row) (*big.Int, error) {
	var s string
	err := row.Scan(&s)
	if errors.Is(err, sql.ErrNoRows) {
		return new(big.Int), nil
	}
	if err != nil {
		return nil, fmt.Errorf("ledger: reading balance: %w", err)
	}
	return parseAmount(s)
}

func noneInFlight(tx *sql.Tx) error {
	var n int
	if err := tx.QueryRow(`SELECT count(*) FROM withdrawals WHERE state = 'signed'`).Scan(&n); err != nil {
		return fmt.Errorf("ledger: reading queue: %w", err)
	}
	if n > 0 {
		return ErrInFlight
	}
	return nil
}

func readSigned(tx *sql.Tx, id int64) (addr string, amount *big.Int, hash nanocrypto.Hash, err error) {
	var amt, h string
	err = tx.QueryRow(`SELECT address, amount, block_hash FROM withdrawals WHERE id = ? AND state = 'signed'`, id).Scan(&addr, &amt, &h)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, hash, ErrNotInFlight
	}
	if err != nil {
		return "", nil, hash, fmt.Errorf("ledger: reading withdrawal: %w", err)
	}
	if amount, err = parseAmount(amt); err != nil {
		return "", nil, hash, err
	}
	hash, err = parseHash(h)
	return addr, amount, hash, err
}

func sumColumn(tx *sql.Tx, query string) (*big.Int, error) {
	rows, err := tx.Query(query)
	if err != nil {
		return nil, fmt.Errorf("ledger: summing: %w", err)
	}
	defer rows.Close()
	total := new(big.Int)
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, fmt.Errorf("ledger: summing: %w", err)
		}
		n, err := parseAmount(s)
		if err != nil {
			return nil, err
		}
		total.Add(total, n)
	}
	return total, rows.Err()
}

var maxAmount = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))

// parseAmount accepts only what this package writes: canonical unsigned decimal up to 2^128-1.
func parseAmount(s string) (*big.Int, error) {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok || n.Sign() < 0 || n.Cmp(maxAmount) > 0 || n.String() != s {
		return nil, fmt.Errorf("%w: stored amount %q is not canonical", ErrInvariant, s)
	}
	return n, nil
}

// hashes are stored as nanocrypto.Hash.Hex writes them; anything else in the database is corruption
func parseHash(s string) (nanocrypto.Hash, error) {
	h, err := nanocrypto.ParseHash(s)
	if err != nil {
		return h, fmt.Errorf("%w: stored hash %q", ErrInvariant, s)
	}
	return h, nil
}

func oneRow(res sql.Result, none error) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	if n != 1 {
		return none
	}
	return nil
}

func (l *Ledger) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("ledger: begin: %w", err)
	}
	if err := fn(tx); err != nil {
		tx.Rollback() // its error adds nothing: the transaction is abandoned either way, and fn's error is the cause
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("ledger: commit: %w", err)
	}
	return nil
}
