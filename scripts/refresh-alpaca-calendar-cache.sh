#!/usr/bin/env bash
# Refresh only the shared Alpaca trading calendar; never download Yahoo data.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BT_CORE=$ROOT/bt-core
ACCOUNT_ENV=${ACCOUNT_ENV:-$HOME/.config/backtrader/accounts/development.env}

[[ -r "$ACCOUNT_ENV" ]] || { echo "Account environment not readable: $ACCOUNT_ENV" >&2; exit 66; }
[[ -x "$BT_CORE/.venv/bin/python" ]] || { echo "Missing Python environment: $BT_CORE/.venv/bin/python" >&2; exit 66; }

set -a
# shellcheck source=/dev/null
source "$ACCOUNT_ENV"
set +a

exec "$BT_CORE/.venv/bin/python" "$ROOT/bin/refresh_alpaca_calendar_cache.py" "$@"
