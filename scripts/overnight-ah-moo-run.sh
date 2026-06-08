#!/usr/bin/env bash
# OvernightAH — MOO exit parametrizzato per ambiente.
# Parametri via env:
#   PSIM_ENV=overnight-ah-no.key|overnight-ah-auc.key
#   ENTRY_TIF=gtc|cls|any

set -euo pipefail

BACK_DIR="/home/htpc/backtrader"
BT_CORE="$BACK_DIR/bt-core"
PSIM_ENV="${PSIM_ENV:-overnight-ah-auc.key}"
ENTRY_TIF="${ENTRY_TIF:-cls}"

source "$BT_CORE/.venv/bin/activate"
set -a; source "$BACK_DIR/env/$PSIM_ENV"; set +a

exec python "$BACK_DIR/bin/overnight_ah/submit_moo.py" --all-longs --entry-tif "$ENTRY_TIF" "$@"
