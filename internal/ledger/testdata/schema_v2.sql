-- drainme ledger, schema v2 (v1, without vault.frontier and vault.addresses, was never deployed).
-- Amounts are canonical unsigned decimal TEXT in 0..2^128-1: SQLite has no 128-bit integer, and the format CHECKs
-- make a negative, malformed or out-of-range amount unstorable no matter what the Go code computes.
-- Conservation (sums across rows) is checked in Go inside one transaction and re-verified by Ledger.Audit.

CREATE TABLE vault (
  id            INTEGER PRIMARY KEY CHECK (id = 1),
  -- last confirmed on-chain balance of the hot wallet
  chain_balance TEXT NOT NULL CHECK ((length(chain_balance) BETWEEN 1 AND 38 OR (length(chain_balance) = 39 AND chain_balance <= '340282366920938463463374607431768211455')) AND chain_balance NOT GLOB '*[^0-9]*' AND (chain_balance = '0' OR chain_balance NOT GLOB '0*')),
  -- chain_balance minus everything owed (unpaid + in flight); clicks spend it, so it can never go below zero
  available     TEXT NOT NULL CHECK ((length(available) BETWEEN 1 AND 38 OR (length(available) = 39 AND available <= '340282366920938463463374607431768211455')) AND available NOT GLOB '*[^0-9]*' AND (available = '0' OR available NOT GLOB '0*')),
  -- every raw ever credited by a click
  credited      TEXT NOT NULL CHECK ((length(credited) BETWEEN 1 AND 38 OR (length(credited) = 39 AND credited <= '340282366920938463463374607431768211455')) AND credited NOT GLOB '*[^0-9]*' AND (credited = '0' OR credited NOT GLOB '0*')),
  -- hash of our latest confirmed block: every send is built on it, never on what an RPC reports (D86 R6)
  frontier      TEXT CHECK (frontier IS NULL OR (length(frontier) = 64 AND frontier NOT GLOB '*[^0-9A-F]*')),
  -- rows in balances, i.e. addresses owed money; kept by the triggers below, so the address cap cannot drift
  addresses     INTEGER NOT NULL CHECK (addresses >= 0),
  CHECK (frontier IS NOT NULL OR chain_balance = '0')
) STRICT;

INSERT INTO vault (id, chain_balance, available, credited, frontier, addresses) VALUES (1, '0', '0', '0', NULL, 0);

-- one row per address that is owed money: a withdrawal deletes the row, so the table (and the disk it uses) holds
-- only live debts, and the address cap counts exactly those (D89)
CREATE TABLE balances (
  address TEXT PRIMARY KEY CHECK (length(address) = 65 AND address GLOB 'nano_*'),
  unpaid  TEXT NOT NULL CHECK ((length(unpaid) BETWEEN 1 AND 38 OR (length(unpaid) = 39 AND unpaid <= '340282366920938463463374607431768211455')) AND unpaid NOT GLOB '*[^0-9]*' AND unpaid NOT GLOB '0*')
) STRICT;

CREATE TRIGGER balances_count_insert AFTER INSERT ON balances
BEGIN UPDATE vault SET addresses = addresses + 1 WHERE id = 1; END;

CREATE TRIGGER balances_count_delete AFTER DELETE ON balances
BEGIN UPDATE vault SET addresses = addresses - 1 WHERE id = 1; END;

CREATE TABLE withdrawals (
  id         INTEGER PRIMARY KEY AUTOINCREMENT, -- AUTOINCREMENT: ids are never reused, so id order is FIFO order
  address    TEXT NOT NULL CHECK (length(address) = 65 AND address GLOB 'nano_*'),
  amount     TEXT NOT NULL CHECK ((length(amount) BETWEEN 1 AND 38 OR (length(amount) = 39 AND amount <= '340282366920938463463374607431768211455')) AND amount NOT GLOB '*[^0-9]*' AND amount NOT GLOB '0*'),
  state      TEXT NOT NULL CHECK (state IN ('queued', 'signed', 'confirmed', 'recredited')),
  -- the signed block is persisted before broadcast; its hash is the idempotency key
  block_hash TEXT UNIQUE CHECK (block_hash IS NULL OR (length(block_hash) = 64 AND block_hash NOT GLOB '*[^0-9A-F]*')),
  block      TEXT,
  created    TEXT NOT NULL,
  CHECK ((state = 'queued') = (block_hash IS NULL)),
  CHECK ((block_hash IS NULL) = (block IS NULL))
) STRICT;

-- per-address dedupe: further withdrawals merge into the one queued row
CREATE UNIQUE INDEX one_queued_per_address ON withdrawals (address) WHERE state = 'queued';
-- one worker, one chain: at most one signed send awaiting confirmation
CREATE UNIQUE INDEX one_send_in_flight ON withdrawals (state) WHERE state = 'signed';

CREATE TRIGGER withdrawal_starts_queued BEFORE INSERT ON withdrawals
WHEN NEW.state <> 'queued'
BEGIN SELECT RAISE(ABORT, 'withdrawals start queued'); END;

CREATE TRIGGER withdrawal_final_once_signed BEFORE UPDATE OF address, amount, block_hash, block ON withdrawals
WHEN OLD.state <> 'queued'
BEGIN SELECT RAISE(ABORT, 'withdrawal is final once signed'); END;

CREATE TRIGGER withdrawal_state_machine BEFORE UPDATE OF state ON withdrawals
WHEN NOT ((OLD.state = 'queued' AND NEW.state = 'signed')
       OR (OLD.state = 'signed' AND NEW.state IN ('confirmed', 'recredited'))
       OR OLD.state = NEW.state)
BEGIN SELECT RAISE(ABORT, 'illegal withdrawal state change'); END;

CREATE TRIGGER withdrawals_append_only BEFORE DELETE ON withdrawals
BEGIN SELECT RAISE(ABORT, 'withdrawals are append-only'); END;

CREATE TRIGGER vault_single_row BEFORE DELETE ON vault
BEGIN SELECT RAISE(ABORT, 'the vault row is never deleted'); END;
