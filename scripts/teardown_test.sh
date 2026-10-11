#!/bin/sh
# Proves teardown leaves nothing behind (D58): brings the stack up against throwaway paths, tears it down, and
# requires the host's container, network and volume lists to match what they were before. It refuses to run
# while any drainme container exists, so it can never touch a live stack. Needs the image built
# (docker compose build) and runs real containers, so run it by hand, not from go test.
set -eu
cd "$(dirname "$0")/.."

if docker ps -a --format '{{.Names}}' | grep -q '^drainme-'; then
	echo "drainme containers exist; refusing to test against a live stack" >&2
	exit 1
fi

snapshot() {
	docker ps -a --format 'container {{.Names}}' | sort
	docker network ls --format 'network {{.Name}}' | sort
	docker volume ls --format 'volume {{.Name}}' | sort
}

tmp=$(mktemp -d)
# whatever happens below, nothing is left running
trap 'scripts/teardown.sh >/dev/null 2>&1 || true; rm -rf "$tmp"' EXIT
mkdir "$tmp/data"
printf '%064d\n' 1 >"$tmp/seed" # a throwaway seed: the stack only has to start, not to pay
export DRAINME_HOST_DATA="$tmp/data" DRAINME_SEED_HOST_FILE="$tmp/seed"
export DRAINME_RPC_URLS=https://rpc.invalid DRAINME_SEND_THRESHOLD=fffffff800000000 DRAINME_RECEIVE_THRESHOLD=fffffe0000000000

before=$(snapshot)
# with throwaway config the app cannot become healthy, so up reports a failure; the containers and networks
# exist all the same, which is what teardown has to remove
docker compose up -d --no-build || true
up=$(snapshot)
if [ "$up" = "$before" ]; then
	echo "FAIL: compose up created nothing, so the test proves nothing" >&2
	exit 1
fi
scripts/teardown.sh
after=$(snapshot)

if [ "$before" != "$after" ]; then
	echo "FAIL: teardown left resources behind:" >&2
	printf '%s\n' "$before" >"$tmp/before"
	printf '%s\n' "$after" >"$tmp/after"
	diff "$tmp/before" "$tmp/after" >&2 || true
	exit 1
fi
[ -d "$tmp/data" ] || { echo "FAIL: teardown deleted the ledger without --delete-data" >&2; exit 1; }
echo "PASS: teardown removed everything compose created and kept the ledger"
