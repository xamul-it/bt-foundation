#!/usr/bin/env bash
set -euo pipefail

BT_CORE=${BT_CORE:-$CODE_ROOT/bt-core}
TICKER=${TICKER:?TICKER is required}
STRAT=${STRAT:?STRAT is required}
DATA_PROVIDER=${DATA_PROVIDER:-yahoo}
ALPACA_FEED=${ALPACA_FEED:-sip}
FROM_DAYS=${FROM_DAYS:-120}
MARGIN_LEVERAGE=${MARGIN_LEVERAGE:-1}
COMMISSION=${COMMISSION:-none}
STRATARGS=${STRATARGS:?STRATARGS is required}
REFRESH_MARKET_DATA=${REFRESH_MARKET_DATA:-0}

FROMDATE=$(date -d "$FROM_DAYS days ago" '+%Y-%m-%d')
# The entry decision must never depend on today's incomplete daily candle.
# Calendar-day cutoff is intentional: on Monday (or after a holiday) the
# feed naturally ends at the latest earlier trading session.
DATA_CUTOFF=${DATA_CUTOFF:-$(date -d 'yesterday' '+%Y-%m-%d')}
LOAD_CMD=(python load_tickers.py --ticker="$TICKER" --provider "$DATA_PROVIDER" --alpaca-feed "$ALPACA_FEED" --timeframe=d --todate "$DATA_CUTOFF" --incremental)
RUN_CMD=(python btmain.py --strat "$STRAT" --ticker "$TICKER" --stratargs "$STRATARGS" --timeframe daily --provider "$DATA_PROVIDER" --alpaca-feed "$ALPACA_FEED" --fromdate "$FROMDATE" --todate "$DATA_CUTOFF" --mode "$TRADING_MODE" --margin-leverage "$MARGIN_LEVERAGE" --commission "$COMMISSION")
[[ -z "${RUN_ID:-}" ]] || RUN_CMD+=(--id "$RUN_ID")

if [[ "${SCHEDULED_DRY_RUN:-0}" == 1 ]]; then
    printf 'cd %q\n' "$BT_CORE"
    if [[ "$REFRESH_MARKET_DATA" == 1 ]]; then
        printf 'load: '; printf '%q ' "${LOAD_CMD[@]}"; printf '\n'
    else
        printf 'load: skipped (REFRESH_MARKET_DATA=%q) cutoff=%q\n' "$REFRESH_MARKET_DATA" "$DATA_CUTOFF"
    fi
    printf 'run:  '; printf '%q ' "${RUN_CMD[@]}"; printf '\n'
    exit 0
fi

# shellcheck source=/dev/null
source "$BT_CORE/.venv/bin/activate"
set -a; source "$ACCOUNT_ENV"; set +a
cd "$BT_CORE"
if [[ "$REFRESH_MARKET_DATA" == 1 ]]; then
    timeout "${LOAD_TIMEOUT_SEC:-900}" "${LOAD_CMD[@]}"
fi
"$CODE_ROOT/scripts/snapshot-scheduled-data.sh" "$PROFILE" "$(date '+%Y-%m-%d')" "$CODE_ROOT"
"${RUN_CMD[@]}"
