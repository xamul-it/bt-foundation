#!/usr/bin/env bash
# OvernightAH — fallback post-open (~09:35 ET, lunedì-venerdì)
# Chiude a mercato le posizioni long rimaste aperte se non esiste già un sell pendente.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACK_DIR="${BACK_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
BT_CORE="${BT_CORE:-$BACK_DIR/bt-core}"
ENV_DIR="${ENV_DIR:-$BACK_DIR/env}"

source "$BT_CORE/.venv/bin/activate"
set -a; source "$ENV_DIR/overnight-ah-auc.key"; set +a

exec python "$BACK_DIR/bin/overnight_ah/submit_moo.py" --fallback-market --entry-tif cls "$@"
