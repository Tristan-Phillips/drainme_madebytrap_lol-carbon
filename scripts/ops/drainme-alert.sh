#!/bin/sh
# drainme-alert: Telegram messages for the operator (D104). It runs on the host, never in a container, so the app
# gains no route out and never sees the bot token.
#
#   drainme-alert.sh watch     message when payouts halt (at most once per 30 minutes), until stopped
#   drainme-alert.sh summary   the daily summary
#   drainme-alert.sh test      one message, to check the bot is set up
#
# The token and chat id are the two lines of secrets/drainme_telegram. Without that file every message is
# printed instead of sent (a dry run). DRAINME_ALERT_APP names another container to watch, for a drill.
set -eu

DIR=$(cd "$(dirname "$0")/../.." && pwd)
SECRET=${DRAINME_TELEGRAM_FILE:-$DIR/secrets/drainme_telegram}
APP=${DRAINME_ALERT_APP:-drainme-app}

send() {
	if [ ! -r "$SECRET" ]; then
		printf 'telegram dry run (no readable %s):\n%s\n' "$SECRET" "$1"
		return 0
	fi
	token=$(sed -n 1p "$SECRET")
	chat=$(sed -n 2p "$SECRET")
	# the token reaches curl on stdin, never on a command line where ps would show it
	code=$(printf 'url = "https://api.telegram.org/bot%s/sendMessage"\n' "$token" |
		curl -sS -m 20 --config - --data-urlencode "chat_id=$chat" --data-urlencode "text=$1" -o /dev/null -w '%{http_code}') || code=000
	[ "$code" = 200 ] || echo "drainme-alert: telegram answered $code" >&2
}

watch() {
	last=0
	# a restart of this script (or of the container) re-reads the last half minute, and the 30-minute gap below
	# keeps a halt from being announced twice
	docker logs -f --since 30s "$APP" 2>&1 | while IFS= read -r line; do
		case $line in
		*'PAYOUTS HALTED'* | *'HALTED: the vault'*)
			now=$(date +%s)
			if [ $((now - last)) -ge 1800 ]; then
				last=$now
				send "drainme: payouts halted, funds intact. Run drainme status. $(printf '%s' "$line" | cut -c1-600)"
			fi
			;;
		esac
	done
}

summary() {
	status=$(docker compose -f "$DIR/compose.yaml" exec -T app /drainme status 2>&1) || true
	logs=$(docker logs --since 24h "$APP" 2>&1) || logs=
	paid=$(printf '%s\n' "$logs" | grep -c '"payout confirmed"') || paid=0
	halts=$(printf '%s\n' "$logs" | grep -c 'HALTED') || halts=0
	backups=$(journalctl --user -u drainme-backup --since -24h -o cat 2>/dev/null | grep -c '^Backed up') || backups=0
	send "drainme, $(date +%F)
payouts in the last 24 h: $paid
halts in the last 24 h: $halts
backups taken: $backups

$(printf '%s\n' "$status" | grep -E '^(ledger|owed|paid|in flight|books|waiting|threshold)' || printf '%s\n' "$status" | head -5)"
}

case ${1:-} in
watch) watch ;;
summary) summary ;;
test) send "drainme alerts are set up. You will hear about halts, and get a summary at 21:00." ;;
*)
	echo "usage: drainme-alert.sh watch | summary | test" >&2
	exit 2
	;;
esac
