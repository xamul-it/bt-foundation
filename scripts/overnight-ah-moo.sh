#!/usr/bin/env bash
# OvernightAH — MOO Exit (~09:25 ET, lunedì-venerdì)
# Sottomette ordini MOO (OPG) per tutte le posizioni long aperte.

set -euo pipefail

BACK_DIR="/home/htpc/backtrader"
BT_CORE="$BACK_DIR/bt-core"

source "$BT_CORE/.venv/bin/activate"
set -a; source "$BACK_DIR/env/psim-b"; set +a

exec python "$BACK_DIR/bin/overnight_ah/submit_moo.py" "$@"
