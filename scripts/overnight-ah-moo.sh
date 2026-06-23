#!/usr/bin/env bash
# OvernightAH — MOO Exit (~09:25 ET, lunedì-venerdì)
# Sottomette ordini MOO (OPG) solo per le posizioni aperte da OvernightAH.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACK_DIR="${BACK_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
BT_CORE="${BT_CORE:-$BACK_DIR/bt-core}"
ENV_DIR="${ENV_DIR:-$BACK_DIR/env}"

source "$BT_CORE/.venv/bin/activate"
set -a; source "$ENV_DIR/overnight-ah-auc.key"; set +a

exec python "$BACK_DIR/bin/overnight_ah/submit_moo.py" --entry-tif cls "$@"
