#!/usr/bin/env bash
set -euo pipefail

BT_CORE=${BT_CORE:-$CODE_ROOT/bt-core}
TICKER=${TICKER:?TICKER is required}
STRAT=${STRAT:?STRAT is required}
DATA_PROVIDER=${DATA_PROVIDER:-yahoo}
ALPACA_FEED=${ALPACA_FEED:-sip}
FROM_DAYS=${FROM_DAYS:-120}
MARGIN_LEVERAGE=${MARGIN_LEVERAGE:-1}
STRATARGS=${STRATARGS:?STRATARGS is required}

FROMDATE=$(date -d "$FROM_DAYS days ago" '+%Y-%m-%d')
LOAD_CMD=(python load_tickers.py --ticker="$TICKER" --provider "$DATA_PROVIDER" --alpaca-feed "$ALPACA_FEED" --timeframe=d --incremental)
RUN_CMD=(python btmain.py --strat "$STRAT" --ticker "$TICKER" --stratargs "$STRATARGS" --timeframe daily --provider "$DATA_PROVIDER" --alpaca-feed "$ALPACA_FEED" --fromdate "$FROMDATE" --mode "$TRADING_MODE" --margin-leverage "$MARGIN_LEVERAGE" --commission none)
[[ -z "${RUN_ID:-}" ]] || RUN_CMD+=(--id "$RUN_ID")

if [[ "${SCHEDULED_DRY_RUN:-0}" == 1 ]]; then
    printf 'cd %q\n' "$BT_CORE"
    printf 'load: '; printf '%q ' "${LOAD_CMD[@]}"; printf '\n'
    printf 'run:  '; printf '%q ' "${RUN_CMD[@]}"; printf '\n'
    exit 0
fi

# shellcheck source=/dev/null
source "$BT_CORE/.venv/bin/activate"
set -a; source "$ACCOUNT_ENV"; set +a
cd "$BT_CORE"
timeout "${LOAD_TIMEOUT_SEC:-900}" "${LOAD_CMD[@]}"
"${RUN_CMD[@]}"
