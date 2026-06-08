#!/usr/bin/env bash
# OvernightAH — auction fallback (~16:01 ET)
# Reinvia residui di ordini auction expired: CLS entry come limit AH,
# OPG exit come market RTH.

set -euo pipefail

BACK_DIR="/home/htpc/backtrader"
BT_CORE="$BACK_DIR/bt-core"

source "$BT_CORE/.venv/bin/activate"
set -a; source "$BACK_DIR/env/overnight-ah-auc.key"; set +a

exec python "$BACK_DIR/bin/overnight_ah/auction_fallback.py" "$@"
