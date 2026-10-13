#!/usr/bin/env bash
# Refresh the challenger flat-panel once per calendar month.

set -euo pipefail

CODE_ROOT=${CODE_ROOT:-/home/htpc/backtrader}
PANEL=${PANEL:-$CODE_ROOT/config-common/indicator_panel/flat_panel_live.csv}
STATE_DIR=${BT_SCHEDULED_STATE_DIR:-$HOME/.local/state/backtrader}
LOCK_DIR=$STATE_DIR/locks
YEAR=$(date +%Y)
MONTH=$((10#$(date +%m)))

mkdir -p "$LOCK_DIR"
exec 9>"$LOCK_DIR/refresh-indicator-panel.lock"
flock -n 9 || {
    echo "[$(date '+%F %T %Z')] SKIP refresh-indicator-panel: another refresh is active"
    exit 0
}

if [[ -r "$PANEL" ]] && awk -F';' -v year="$YEAR" -v month="$MONTH" '
    NR > 1 && $1 == year && $2 == month { found=1; exit }
    END { exit !found }
' "$PANEL"; then
    echo "[$(date '+%F %T %Z')] SKIP refresh-indicator-panel: month=$YEAR-$(printf '%02d' "$MONTH") already present"
    exit 0
fi

echo "[$(date '+%F %T %Z')] RUN refresh-indicator-panel: missing month=$YEAR-$(printf '%02d' "$MONTH")"
exec "$CODE_ROOT/scripts/refresh-indicator-panel.sh"
