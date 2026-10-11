#!/bin/sh
# Installs drainme's host timers and the alert watcher as systemd user units (D104). trap runs this; it changes
# nothing outside ~/.config/systemd/user. Rerun it after moving the project, since the units name its path.
set -eu
DIR=$(cd "$(dirname "$0")/../.." && pwd)
UNITS=${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user
mkdir -p "$UNITS"
for u in drainme-backup.service drainme-backup.timer drainme-alert.service drainme-summary.service drainme-summary.timer; do
	sed "s|@DIR@|$DIR|g" "$DIR/scripts/ops/$u" >"$UNITS/$u"
done
systemctl --user daemon-reload
systemctl --user enable --now drainme-backup.timer drainme-summary.timer drainme-alert.service
systemctl --user list-timers 'drainme-*' --no-pager
if [ "$(loginctl show-user "$USER" -p Linger --value 2>/dev/null)" != yes ]; then
	echo
	echo "User services stop when you log out. To keep them running: sudo loginctl enable-linger $USER"
fi
echo "Check the bot with: $DIR/scripts/ops/drainme-alert.sh test"
