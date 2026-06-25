#!/usr/bin/env bash
# auc: fallback post-asta per entry CLS expired/parziali.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACK_DIR="${BACK_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
BT_CORE="${BT_CORE:-$BACK_DIR/bt-core}"
ENV_DIR="${ENV_DIR:-$BACK_DIR/env}"

source "$BT_CORE/.venv/bin/activate"
set -a; source "$ENV_DIR/overnight-ah-auc.key"; set +a

exec python "$BACK_DIR/bin/overnight_ah/auction_fallback.py" "$@"
