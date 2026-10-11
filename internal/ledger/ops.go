package ledger

// What the operator commands need from the ledger (D68): a reader that is safe beside a running serve, the whole
// ledger at one instant for status, an online backup, and the sends recover ever put back.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

// OpenReader opens an existing ledger for reading only, for status and backup while serve keeps running. It never
// creates or migrates a file, its transactions take no write lock, and every write is refused (query_only).
func OpenReader(path string) (*Ledger, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("ledger: %w", err)
	}
	read, err := sql.Open("sqlite", "file:"+url.PathEscape(path)+"?_pragma=busy_timeout(5000)&_pragma=query_only(1)")
	if err != nil {
		return nil, fmt.Errorf("ledger: open: %w", err)
	}
	read.SetMaxOpenConns(4)
	var v int
	if err := read.QueryRow("PRAGMA user_version").Scan(&v); err != nil || v != schemaVersion {
		read.Close()
		return nil, fmt.Errorf("ledger: schema version %d is not %d (serve migrates it): %v", v, schemaVersion, err)
	}
	return &Ledger{db: read, read: read, queueMax: 1, file: path}, nil
}

// Report is the whole ledger at one instant.
type Report struct {
	Frontier                   *nanocrypto.Hash // nil before init-vault
	Chain, Available, Credited *big.Int
	Unpaid                     *big.Int
	Addresses                  int64
	Queued                     *big.Int // waiting, not yet signed
	QueuedCount                int64
	InFlight                   *Signed // nil when nothing is awaiting confirmation
	Paid                       *big.Int
	PaidCount, Recredited      int64
	Books                      error // what Audit would say; nil when the books balance
}

// Report reads everything in one read transaction, so the figures agree with each other and a running serve is
// never held up. Like Audit it reads every row: an operator command, not a hot path.
func (l *Ledger) Report(ctx context.Context) (Report, error) {
	var r Report
	tx, err := l.read.BeginTx(ctx, nil)
	if err != nil {
		return r, fmt.Errorf("ledger: begin: %w", err)
	}
	defer tx.Rollback() // read only: nothing to commit
	v, err := readVault(tx)
	if err != nil {
		return r, err
	}
	s, err := readSums(tx)
	if err != nil {
		return r, err
	}
	r.Frontier, r.Chain, r.Available, r.Credited = v.frontier, v.chain, v.available, v.credited
	r.Unpaid, r.Addresses, r.Paid, r.Books = s.unpaid, s.rows, s.confirmed, s.check(v)
	err = tx.QueryRow(`SELECT (SELECT count(*) FROM withdrawals WHERE state = 'queued'),
		(SELECT count(*) FROM withdrawals WHERE state = 'confirmed'), (SELECT count(*) FROM withdrawals WHERE state = 'recredited')`).
		Scan(&r.QueuedCount, &r.PaidCount, &r.Recredited)
	if err != nil {
		return r, fmt.Errorf("ledger: counting withdrawals: %w", err)
	}
	r.Queued = new(big.Int).Set(s.owedQueue)
	var id int64
	err = tx.QueryRow(`SELECT id FROM withdrawals WHERE state = 'signed'`).Scan(&id)
	if err == nil {
		sg := Signed{}
		var addr string
		if addr, sg.Amount, sg.Hash, err = readSigned(tx, id); err != nil {
			return r, err
		}
		sg.ID = id
		if sg.Address, err = nanocrypto.ParseAddress(addr); err != nil {
			return r, fmt.Errorf("ledger: stored address: %w", err)
		}
		r.InFlight = &sg
		r.Queued.Sub(r.Queued, sg.Amount)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return r, fmt.Errorf("ledger: reading in-flight send: %w", err)
	}
	return r, nil
}

// Backup writes a consistent copy of the ledger to dest, a file that must not exist yet (D70). VACUUM INTO reads
// one snapshot, so it runs beside a live serve without stopping its writes, and the copy comes out compacted.
func (l *Ledger) Backup(ctx context.Context, dest string) error {
	if _, err := os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("ledger: backup %s: already exists or cannot be checked: %v", dest, err)
	}
	// its own connection: the reader's query_only refuses the write VACUUM INTO makes to the new file
	db, err := sql.Open("sqlite", "file:"+url.PathEscape(l.file)+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return fmt.Errorf("ledger: backup: %w", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `VACUUM INTO ?`, dest); err != nil {
		return fmt.Errorf("ledger: backup: %w", err)
	}
	return nil
}

// Recredited is the block hash of every send recover put back in a balance. reconcile refuses to adopt a chain
// holding one of them: that send was paid after all, and the address is owed it a second time.
func (l *Ledger) Recredited(ctx context.Context) ([]nanocrypto.Hash, error) {
	rows, err := l.read.QueryContext(ctx, `SELECT block_hash FROM withdrawals WHERE state = 'recredited' ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("ledger: reading recredited sends: %w", err)
	}
	defer rows.Close()
	var out []nanocrypto.Hash
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, fmt.Errorf("ledger: reading recredited sends: %w", err)
		}
		h, err := parseHash(s)
		if err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// Integrity is SQLite's own check of the whole file: every page, index and constraint.
func (l *Ledger) Integrity(ctx context.Context) error {
	var res string
	if err := l.read.QueryRowContext(ctx, `PRAGMA integrity_check(1)`).Scan(&res); err != nil {
		return fmt.Errorf("ledger: integrity check: %w", err)
	}
	if res != "ok" {
		return fmt.Errorf("ledger: integrity check: %s", res)
	}
	return nil
}

// Taker is an address and everything it has been paid, for the leaderboard (D104).
type Taker struct {
	Address nanocrypto.PublicKey
	Paid    *big.Int
}

// TopPaid is the n addresses paid the most, most first. SQLite sums in 64-bit integers here: one address would
// need more than 9 × 10^18 clicks to overflow one, and an overflow is an error, never a wrong total.
func (l *Ledger) TopPaid(ctx context.Context, n int) ([]Taker, error) {
	rows, err := l.read.QueryContext(ctx, `SELECT address, SUM(CAST(amount AS INTEGER)) AS paid FROM withdrawals
		WHERE state = 'confirmed' GROUP BY address ORDER BY paid DESC, address LIMIT ?`, n)
	if err != nil {
		return nil, fmt.Errorf("ledger: reading the leaderboard: %w", err)
	}
	defer rows.Close()
	var out []Taker
	for rows.Next() {
		var addr string
		var paid int64
		if err := rows.Scan(&addr, &paid); err != nil {
			return nil, fmt.Errorf("ledger: reading the leaderboard: %w", err)
		}
		pub, err := nanocrypto.ParseAddress(addr)
		if err != nil {
			return nil, fmt.Errorf("ledger: stored address: %w", err)
		}
		out = append(out, Taker{pub, big.NewInt(paid)})
	}
	return out, rows.Err()
}

// PaidTo is everything pub has been paid, summed exactly from its confirmed sends through the index.
func (l *Ledger) PaidTo(ctx context.Context, pub nanocrypto.PublicKey) (*big.Int, error) {
	rows, err := l.read.QueryContext(ctx, `SELECT amount FROM withdrawals WHERE address = ? AND state = 'confirmed'`, pub.Address())
	if err != nil {
		return nil, fmt.Errorf("ledger: reading payouts: %w", err)
	}
	defer rows.Close()
	total := new(big.Int)
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, fmt.Errorf("ledger: reading payouts: %w", err)
		}
		n, err := parseAmount(s)
		if err != nil {
			return nil, err
		}
		total.Add(total, n)
	}
	return total, rows.Err()
}
