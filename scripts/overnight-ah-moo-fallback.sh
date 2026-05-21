#!/usr/bin/env bash
# OvernightAH — fallback post-open (~09:35 ET, lunedì-venerdì)
# Chiude a mercato le posizioni long rimaste aperte se non esiste già un sell pendente.

set -euo pipefail

BACK_DIR="/home/htpc/backtrader"
BT_CORE="$BACK_DIR/bt-core"

source "$BT_CORE/.venv/bin/activate"
set -a; source "$BACK_DIR/env/psim-b"; set +a

exec python "$BACK_DIR/bin/overnight_ah/submit_moo.py" --fallback-market "$@"
