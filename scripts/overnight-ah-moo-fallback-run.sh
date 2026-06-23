#!/usr/bin/env bash
# OvernightAH — fallback post-open parametrizzato per ambiente.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACK_DIR="${BACK_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
BT_CORE="${BT_CORE:-$BACK_DIR/bt-core}"
ENV_DIR="${ENV_DIR:-$BACK_DIR/env}"
PSIM_ENV="${PSIM_ENV:-overnight-ah-auc.key}"
ENTRY_TIF="${ENTRY_TIF:-cls}"

source "$BT_CORE/.venv/bin/activate"
set -a; source "$ENV_DIR/$PSIM_ENV"; set +a

exec python "$BACK_DIR/bin/overnight_ah/submit_moo.py" --fallback-market --all-longs --cancel-pending-sells --entry-tif "$ENTRY_TIF" "$@"
