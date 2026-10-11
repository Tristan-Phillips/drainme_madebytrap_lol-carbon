# drainme

**A vault of 1 XNO behind a button. Every click takes one raw, paid out on chain. Bots welcome.**

Live at **[drainme.madebytrap.lol](https://drainme.madebytrap.lol)**.

The vault was funded once with exactly 1 XNO and is never topped up. There is no captcha, no login and no per-IP limit. A `curl` loop is an officially supported client. It still cannot be emptied by clicking, and that is the point.

## Why it cannot be emptied by clicking

Nano is divisible into raw: 1 XNO is 10^30 raw. Each click credits exactly 1 raw, so emptying the vault takes 10^30 clicks.

| Clicks per second, worldwide | Time to drain 1 XNO |
|---|---|
| 1,000 | about 3 × 10^19 years |
| 100,000 | about 3 × 10^17 years |
| 1,000,000,000 | about 3 × 10^13 years |

The universe is about 1.4 × 10^10 years old. The server falls over long before any of these rates. Every raw taken is real, spendable Nano, sent on chain with no fee.

### What it claims, and what it does not

**The claim:** the vault cannot be drained by clicking.

**Not claimed:**
- **That it cannot be drained at all.** A bug (a double withdrawal, an overflow) or a leaked key could drain it. The arithmetic does not protect against those, so the code is built and tested as if they were the real threat.
- **That it cannot be taken offline.** Availability attacks are possible. If one happens, the page says "availability lost, funds intact".
- **That it is bot-proof.** Bots are welcome.

## Bring your own bot

```sh
# one click: 1 raw to your address
curl -X POST https://drainme.madebytrap.lol/click/nano_YOUR_ADDRESS

# as many as you like
while true; do curl -s -o /dev/null -X POST https://drainme.madebytrap.lol/click/nano_YOUR_ADDRESS; done

# send your balance to that same address
curl -X POST https://drainme.madebytrap.lol/withdraw/nano_YOUR_ADDRESS
```

| Route | What it does |
|---|---|
| `POST /click/{address}` | Credits 1 raw. The request carries no amount, and there is nothing to change. |
| `POST /withdraw/{address}` | Queues the address's whole balance for payout **to that address**. There is no destination parameter, so anyone can trigger a withdrawal for any address and the money still only goes to its owner. |
| `GET /events` | Server-sent events: the vault, click rate, sampled clicks and confirmed payouts, once a second. |

Replies are short plain text. `409` means nothing is owed yet. `503` comes with a `Retry-After` header and means the payout line is full, the address limit is reached, or payouts are paused. Your balance stays credited in every case.

A balance left with no clicks for 24 hours is paid out to its address by itself.

## How it works

One statically linked Go binary, with the page embedded in it. Its data is a single SQLite file.

```
client ──POST /click──▶ ingest ──(counted in memory, flushed in batches)──▶ ledger (SQLite)
client ──POST /withdraw─▶ ledger: debit and enqueue in one transaction ─▶ payout worker ─▶ Nano RPC
browser ◀──GET /events── stats (vault, rate, clicks, payouts)
```

- **Clicks cost memory, not disk.** They are counted in memory and written to the ledger every 5 seconds, or sooner under load.
- **One payout worker owns the account chain.** Sends are strictly one at a time, first in first out, with one place in line per address. A second withdrawal from an address already waiting merges into its pending send.
- **Blocks are built and signed in the process.** The seed is read once at startup and never leaves the process. Nano RPC nodes are used only to read and broadcast. The service never receives, so the vault's balance only moves through its own sends.
- **Proof of work** for each send comes from public work servers first, then from a local `drainme work` service as a last resort. Every work value is checked locally before use.

### Rules the books follow

- **Exact integers only.** Every amount is a `big.Int`, stored in SQLite as decimal text. No floats, and no 64-bit integer, ever holds a balance.
- **What is owed plus what is in flight never exceeds the vault's confirmed balance on chain.** This is checked on every credit and audited in full at every start. If it ever fails, the service halts instead of guessing.
- **A withdrawal debits first, then enqueues,** in one transaction. A failed send is recredited only through an explicit recovery command, never automatically.
- **Every block is checked before every broadcast,** including rebroadcasts after a crash: previous block, amount, destination, hash, signature and work, all against the ledger's own record and never against what an RPC node says. Any mismatch halts the worker.
- **Crash safe.** The signed block is saved before it is broadcast. After a restart the same block is rebroadcast, so a send can only ever land once.
- **RPC nodes are never trusted** for the vault's balance or frontier. Several are asked, and recovery commands act only when every node that answers agrees.

### Tested

- Unit and race tests across every package.
- Crash injection between each step of a payout.
- Overflow tests around 2^64, 2^127 and 10^30.
- Fuzzing: about 177 million generated inputs through the parsers and the full route table.
- Mutation testing: deliberately broken invariants, each caught by a test.
- A load generator in `tools/stress`. At 0.5 CPU and 512 MB the service took 9,362 clicks a second on one address, 5,094 a second on new addresses, and 5,000 live viewers in 167 MB.

```sh
go vet ./... && go test -race ./...
```

## Run it locally

The whole stack runs against an in-process fake Nano node with a throwaway seed, so nothing touches a real network or real money:

```sh
go test ./cmd/drainme -run TestLocal -local 127.0.0.1:8080
```

Open http://127.0.0.1:8080 and click. Opening `/?mock=1` instead gives the page alone, with demo data and controls for simulated states.

Load-test that local stack:

```sh
go run ./tools/stress -url http://127.0.0.1:8080 -mode click -c 200 -d 20s
```

Modes are `click`, `clickuniq`, `withdraw`, `page`, `sse` and `abuse`. Point it only at a stack you run. It refuses the live site unless you pass `-i-own-this`, and even then it allows only gentle clicks for your own address, page loads and streams.

## Run your own

You need Docker or Go 1.26, a fresh Nano seed, and an amount you are happy to give away.

```sh
docker build -t drainme .
```

The image is about 17 MB, built `FROM scratch`. It holds only the binary and CA certificates, and runs as a non-root user. The binary has these subcommands:

| Command | Job |
|---|---|
| `serve` | The site, on port 8080 |
| `egress` | A CONNECT proxy on port 3128 that reaches only the configured RPC and work hosts |
| `work` | A local proof-of-work server on port 7076, the last fallback |
| `init-vault [<raw>]` | Prints the vault address, then opens the vault from one waiting send of exactly `<raw>` |
| `status` | The books, the queue and each node's view of the vault |
| `backup [--keep N]` | A checked copy of the ledger |
| `recover [--recredit]` | Settles a send in flight after a halt |
| `reconcile [--adopt <hash>]` | Lines the ledger up with the chain after a halt |
| `healthcheck` | For the container's health check |

### Configuration

Everything comes from environment variables, and the service refuses to start if one is invalid.

| Variable | Default | Meaning |
|---|---|---|
| `DRAINME_DATA_DIR` | required | Directory for the SQLite ledger |
| `DRAINME_SEED_FILE` | required | File holding the 64-hex seed. Account 0 is the vault. Use a file, never an environment variable. |
| `DRAINME_RPC_URLS` | required | Nano RPC endpoints, comma-separated, tried in order |
| `DRAINME_WORK_URLS` | the RPC URLs | `work_generate` endpoints, tried in order |
| `DRAINME_SEND_THRESHOLD` | required | Send work threshold, 16 hex digits (today `fffffff800000000`) |
| `DRAINME_RECEIVE_THRESHOLD` | required | Receive work threshold (today `fffffe0000000000`) |
| `DRAINME_SEND_THRESHOLD_MAX` | `fffffffe00000000` | Ceiling when following the network's minimum upward |
| `DRAINME_REPRESENTATIVE` | required | Representative set when the vault is opened |
| `DRAINME_RPC_TIMEOUT` | `10s` | Per RPC request |
| `DRAINME_WORK_TIMEOUT` | `3m` | Per work request |
| `DRAINME_WORK_THREADS` | `2` | Cores for `drainme work` |
| `DRAINME_QUEUE_MAX` | `10000` | Addresses that can wait for a payout at once |
| `DRAINME_ADDRESS_MAX` | `30000000` | Addresses that can be owed at once, about 165 bytes of disk each |
| `DRAINME_SSE_MAX` | `5000` | Live viewers at once |
| `DRAINME_IDLE_AFTER` | `24h` | Pay out a balance after this long without a click (`0` turns it off) |

### With Docker Compose

`compose.yaml` is the production setup. `serve` sits on a network with no route out except the `egress` proxy (`HTTPS_PROXY`), and the site is reachable only through a Cloudflare Tunnel, with no ports published on the host. Every container has a read-only root filesystem and drops all capabilities.

```sh
cp .env.example .env                 # fill in DRAINME_TUNNEL_TOKEN and DRAINME_REPRESENTATIVE; every key is explained there
mkdir -p data/drainme secrets
sudo chown 65532:65532 data/drainme  # the containers run as uid 65532
docker compose build
```

The seed goes in `secrets/drainme_seed`, owned by 65532 with mode 0400 (see below). `data/`, `secrets/` and `.env` are ignored by git. Run the commands below as `docker compose run --rm app <command>` while the stack is down, and `docker compose exec app /drainme status` or `backup` while it is up. `scripts/teardown.sh` removes the stack and keeps the ledger unless you pass `--delete-data`. `scripts/ops/` has optional systemd user units for hourly backups, a daily summary and Telegram alerts when payouts halt.

### Open the vault

1. **Make the seed on the server,** for example with `openssl rand -hex 32`. Keep a backup on paper or in a password manager.
   **Never import it into a wallet app.** A wallet picks up the funding send by itself and opens the account, and `init-vault` then refuses. Once the vault is live, a wallet receiving to it would break the books and halt payouts.
2. **Run `init-vault`** with no argument. It prints the vault's address.
3. **Fund it with a single send** of the exact amount. 1 XNO is `1000000000000000000000000000000` raw.
4. **Run `init-vault <raw>`.** It receives that one send and records the vault. It refuses unless the ledger is empty, the account has never been opened, and exactly one send of exactly that amount is waiting. Before broadcasting it prints a `--resume <hash>` line, for finishing an interrupted run.
5. **Start `serve`** (`docker compose up -d`).

Test the whole path with a dust amount and a throwaway seed before funding anything real.

### When payouts halt

The page says "payouts paused, funds intact", and the log says why.

1. Stop `serve`.
2. **Run `recover`.** It records the send in flight as paid if it confirmed. If the block could still land, it changes nothing, and you restart so the same block is rebroadcast. With `--recredit`, it puts the amount back only when the chain has moved past the block for good.
3. **Run `reconcile`** if the chain moved on without the service. It shows the difference and prints the exact `--adopt <hash>` line to run.

Restoring a backup is a last resort. A backup does not know about payouts sent after it was taken, so restoring it could pay those addresses twice.

## License

[AGPL-3.0](LICENSE). If you run a changed copy as a service, you share your changes.
