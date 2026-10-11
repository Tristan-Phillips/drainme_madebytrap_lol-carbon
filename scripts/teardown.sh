#!/bin/sh
# Removes every container, network and volume of the drainme compose project (D58). The ledger is kept unless
# --delete-data is given: it records who is owed what, and losing it while the vault holds funds leaves debts
# nobody can reconstruct.
set -eu
cd "$(dirname "$0")/.."

case "${1:-}" in
"") delete=no ;;
--delete-data) delete=yes ;;
*)
	echo "usage: scripts/teardown.sh [--delete-data]" >&2
	exit 2
	;;
esac

docker compose down --volumes --remove-orphans

if [ "$delete" = yes ]; then
	data="${DRAINME_HOST_DATA:-./data/drainme}"
	if [ ! -d "$data" ]; then
		echo "no ledger at $data; nothing to delete"
		exit 0
	fi
	echo "deleting the ledger at $(cd "$data" && pwd)"
	rm -rf -- "$data"
fi
