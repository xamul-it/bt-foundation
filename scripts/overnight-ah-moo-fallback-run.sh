#!/usr/bin/env bash
# OvernightAH — fallback post-open parametrizzato per ambiente.

set -euo pipefail

BACK_DIR="/home/htpc/backtrader"
BT_CORE="$BACK_DIR/bt-core"
PSIM_ENV="${PSIM_ENV:-overnight-ah-auc.key}"
ENTRY_TIF="${ENTRY_TIF:-cls}"

source "$BT_CORE/.venv/bin/activate"
set -a; source "$BACK_DIR/env/$PSIM_ENV"; set +a

exec python "$BACK_DIR/bin/overnight_ah/submit_moo.py" --fallback-market --all-longs --cancel-pending-sells --entry-tif "$ENTRY_TIF" "$@"
