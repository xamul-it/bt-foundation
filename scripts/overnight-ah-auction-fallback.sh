#!/usr/bin/env bash
# OvernightAH — auction fallback (~16:01 ET)
# Reinvia residui di ordini auction expired: CLS entry come limit AH,
# OPG exit come market RTH.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACK_DIR="${BACK_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
BT_CORE="${BT_CORE:-$BACK_DIR/bt-core}"
ENV_DIR="${ENV_DIR:-$BACK_DIR/env}"

source "$BT_CORE/.venv/bin/activate"
set -a; source "$ENV_DIR/overnight-ah-auc.key"; set +a

exec python "$BACK_DIR/bin/auction_fallback.py" "$@"
