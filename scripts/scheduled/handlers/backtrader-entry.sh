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
RUN_ID=${RUN_ID:-scheduled_${PROFILE}}
STRAT_MODULE=${STRAT%.*}
STRAT_CLASS=${STRAT##*.}
STRAT_DIR=${STRAT_MODULE,,}

FROMDATE=$(date -d "$FROM_DAYS days ago" '+%Y-%m-%d')
# The entry decision must never depend on today's incomplete daily candle.
# Calendar-day cutoff is intentional: on Monday (or after a holiday) the
# feed naturally ends at the latest earlier trading session.
DATA_CUTOFF=${DATA_CUTOFF:-$(date -d 'yesterday' '+%Y-%m-%d')}
LOAD_CMD=(python load_tickers.py --ticker="$TICKER" --provider "$DATA_PROVIDER" --alpaca-feed "$ALPACA_FEED" --timeframe=d --todate "$DATA_CUTOFF" --incremental)
RUN_CMD=(python btmain.py --strat "$STRAT" --ticker "$TICKER" --stratargs "$STRATARGS" --timeframe daily --provider "$DATA_PROVIDER" --alpaca-feed "$ALPACA_FEED" --fromdate "$FROMDATE" --todate "$DATA_CUTOFF" --mode "$TRADING_MODE" --margin-leverage "$MARGIN_LEVERAGE" --commission "$COMMISSION" --id "$RUN_ID")

if [[ "${SCHEDULED_DRY_RUN:-0}" == 1 ]]; then
    printf 'cd %q\n' "$BT_CORE"
    if [[ "$REFRESH_MARKET_DATA" == 1 ]]; then
        printf 'load: '; printf '%q ' "${LOAD_CMD[@]}"; printf '\n'
    else
        printf 'load: skipped (REFRESH_MARKET_DATA=%q) cutoff=%q\n' "$REFRESH_MARKET_DATA" "$DATA_CUTOFF"
    fi
    printf 'run:  '; printf '%q ' "${RUN_CMD[@]}"; printf '\n'
    printf 'backtest decision: python btmain.py --strat %q --ticker %q --stratargs %q --timeframe daily --provider %q --alpaca-feed %q --fromdate %q --todate %q --mode backtest --margin-leverage %q --commission %q --id %q\n' \
        "$STRAT" "$TICKER" "$STRATARGS decision_date_override='$(date '+%Y-%m-%d')'" "$DATA_PROVIDER" "$ALPACA_FEED" "$FROMDATE" "$DATA_CUTOFF" "$MARGIN_LEVERAGE" "$COMMISSION" "${RUN_ID}_backtest"
    printf 'backtest settlement: python %q --profile %q --trading-date %q --trades-path %q\n' \
        "$CODE_ROOT/bin/watchtower_record_backtest_settlement.py" "$PROFILE" "$(date '+%Y-%m-%d')" "$BT_CORE/out/$STRAT_DIR/$STRAT_CLASS/${RUN_ID}_backtest/trades.json"
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

TRADING_DATE=$(date '+%Y-%m-%d')
RECORDER=("$CODE_ROOT/bin/watchtower_record_entry_decision.py" --profile "$PROFILE" --trading-date "$TRADING_DATE")
BACKTEST_SETTLEMENT_RECORDER=(python "$CODE_ROOT/bin/watchtower_record_backtest_settlement.py" --profile "$PROFILE" --trading-date "$TRADING_DATE")
TELEMETRY_FAILED=0

record_receipt() {
    local source=$1
    shift
    if ! "${RECORDER[@]}" --source "$source" "$@"; then
        TELEMETRY_FAILED=1
        echo "WATCHTOWER_TELEMETRY_FAILED source=$source args=$* (continuing scheduled strategy runs)" >&2
    fi
}

record_failed() {
    record_receipt "$1" --failed "$2"
}

record_backtest_settlement() {
    local trades_path=$1
    if ! "${BACKTEST_SETTLEMENT_RECORDER[@]}" --trades-path "$trades_path"; then
        TELEMETRY_FAILED=1
        echo "WATCHTOWER_TELEMETRY_FAILED source=backtest_settlement path=$trades_path (continuing scheduled strategy runs)" >&2
    fi
}

# Watchtower is observability, not a trading precondition.  Every recorder
# invocation is deliberately best-effort so a DB outage cannot skip either
# the Alpaca decision or the independent Backtest decision.
record_receipt alpaca --running
ALPACA_RC=0
if "${RUN_CMD[@]}"; then
    ALPACA_TODAY="$BT_CORE/out/$STRAT_DIR/$STRAT_CLASS/$RUN_ID/today.json"
    if [[ -f "$ALPACA_TODAY" ]]; then
        record_receipt alpaca --today-path "$ALPACA_TODAY"
    else
        record_failed alpaca "today.json missing after completed scheduled run: $ALPACA_TODAY"
        ALPACA_RC=1
    fi
else
    ALPACA_RC=$?
    record_failed alpaca "scheduled Alpaca run exited with status $ALPACA_RC"
fi

# This is the independent scheduled Backtest decision.  It consumes the same
# t-1 snapshot and writes only its own entry decision; it is not a replay.
BT_RUN_ID="${RUN_ID}_backtest"
BT_STRATARGS="$STRATARGS decision_date_override='$TRADING_DATE'"
BACKTEST_CMD=(python btmain.py --strat "$STRAT" --ticker "$TICKER" --stratargs "$BT_STRATARGS" --timeframe daily --provider "$DATA_PROVIDER" --alpaca-feed "$ALPACA_FEED" --fromdate "$FROMDATE" --todate "$DATA_CUTOFF" --mode backtest --margin-leverage "$MARGIN_LEVERAGE" --commission "$COMMISSION" --id "$BT_RUN_ID")
record_receipt backtest --running
BT_RC=0
if "${BACKTEST_CMD[@]}"; then
    BT_TODAY="$BT_CORE/out/$STRAT_DIR/$STRAT_CLASS/$BT_RUN_ID/today.json"
    if [[ -f "$BT_TODAY" ]]; then
        record_receipt backtest --today-path "$BT_TODAY"
        BT_TRADES="$BT_CORE/out/$STRAT_DIR/$STRAT_CLASS/$BT_RUN_ID/trades.json"
        if [[ -f "$BT_TRADES" ]]; then
            record_backtest_settlement "$BT_TRADES"
        else
            # An empty/missing trades artifact is normal when no position has
            # closed.  The entry remains visibly pending in the database.
            echo "WATCHTOWER_BACKTEST_SETTLEMENT_PENDING trades.json missing: $BT_TRADES" >&2
        fi
    else
        record_failed backtest "today.json missing after completed decision run: $BT_TODAY"
        BT_RC=1
    fi
else
    BT_RC=$?
    record_failed backtest "scheduled Backtest decision run exited with status $BT_RC"
fi

if (( TELEMETRY_FAILED != 0 )); then
    echo "WATCHTOWER_TELEMETRY_DEGRADED profile=$PROFILE: strategy outcomes may be absent from dashboard" >&2
fi

# A strategy/run-contract failure remains visible to the scheduler, but only
# after both independent decision runs were attempted.
(( ALPACA_RC == 0 && BT_RC == 0 ))
