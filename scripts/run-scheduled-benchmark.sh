#!/usr/bin/env bash
# Run the canonical full-history backtest for a scheduled paper profile.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: run-scheduled-benchmark.sh [--dry-run] PROFILE [--fromdate YYYY-MM-DD] [--todate YYYY-MM-DD]

Runs the canonical scheduled benchmark (ID: scheduled_benchmark) using the
profile's versioned strategy configuration. PROFILE must be development or
challenger. Omitting --todate uses the latest session available in local data.
EOF
}

ACTION=run
if [[ "${1:-}" == "--dry-run" ]]; then
    ACTION=dry-run
    shift
elif [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

[[ $# -ge 1 ]] || { usage >&2; exit 64; }
PROFILE=$1
shift
case "$PROFILE" in
    development|challenger) ;;
    *) echo "Canonical scheduled benchmarks are supported only for development or challenger (got: $PROFILE)" >&2; exit 64 ;;
esac

FROMDATE=2000-01-01
TODATE=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --fromdate) [[ $# -ge 2 ]] || { usage >&2; exit 64; }; FROMDATE=$2; shift 2 ;;
        --todate) [[ $# -ge 2 ]] || { usage >&2; exit 64; }; TODATE=$2; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; usage >&2; exit 64 ;;
    esac
done

CONFIG_DIR=${BT_SCHEDULED_CONFIG_DIR:-$HOME/.config/backtrader/scheduled}
PROFILE_FILE=$CONFIG_DIR/$PROFILE.env
[[ -r "$PROFILE_FILE" ]] || {
    echo "Profile not found or unreadable: $PROFILE_FILE" >&2
    exit 66
}

set -a
# shellcheck source=/dev/null
source "$PROFILE_FILE"
set +a

[[ "${ROLE:-}" == "$PROFILE" ]] || { echo "ROLE must match profile in $PROFILE_FILE" >&2; exit 78; }
[[ "${TRADING_MODE:-}" == paper ]] || { echo "Profile $PROFILE must use TRADING_MODE=paper" >&2; exit 78; }
[[ -n "${CODE_ROOT:-}" && -n "${STRATEGY_CONFIG:-}" ]] || {
    echo "CODE_ROOT and STRATEGY_CONFIG are required in $PROFILE_FILE" >&2
    exit 78
}

CODE_ROOT=$(realpath -e "$CODE_ROOT")
if [[ "$STRATEGY_CONFIG" != /* ]]; then
    STRATEGY_CONFIG=$CODE_ROOT/$STRATEGY_CONFIG
fi
[[ -r "$STRATEGY_CONFIG" ]] || { echo "Missing strategy config: $STRATEGY_CONFIG" >&2; exit 66; }

set -a
# shellcheck source=/dev/null
source "$STRATEGY_CONFIG"
set +a

required=(STRAT TICKER STRATARGS DATA_PROVIDER ALPACA_FEED MARGIN_LEVERAGE COMMISSION)
for name in "${required[@]}"; do
    [[ -n "${!name:-}" ]] || { echo "Missing $name in $STRATEGY_CONFIG" >&2; exit 78; }
done

BT_CORE=${BT_CORE:-$CODE_ROOT/bt-core}
BT_CORE=$(realpath -e "$BT_CORE")
STRAT_MODULE=${STRAT%.*}
STRAT_CLASS=${STRAT##*.}
STRAT_DIR=${STRAT_MODULE,,}
RESULTS_DIR=$BT_CORE/out/$STRAT_DIR/$STRAT_CLASS/scheduled_benchmark
CMD=(python btmain.py --strat "$STRAT" --ticker "$TICKER" --stratargs "$STRATARGS" --timeframe daily --provider "$DATA_PROVIDER" --alpaca-feed "$ALPACA_FEED" --fromdate "$FROMDATE" --mode backtest --margin-leverage "$MARGIN_LEVERAGE" --commission "$COMMISSION" --id scheduled_benchmark)
if [[ -n "$TODATE" ]]; then
    CMD+=(--todate "$TODATE")
fi

if [[ "$ACTION" == dry-run ]]; then
    printf 'profile=%s strategy_config=%q run_id=scheduled_benchmark results_dir=%q\n' "$PROFILE" "$STRATEGY_CONFIG" "$RESULTS_DIR"
    printf 'cd %q\nrun: ' "$BT_CORE"
    printf '%q ' "${CMD[@]}"
    printf '\n'
    exit 0
fi

[[ -r "$BT_CORE/.venv/bin/activate" ]] || { echo "Missing virtual environment: $BT_CORE/.venv/bin/activate" >&2; exit 66; }
source "$BT_CORE/.venv/bin/activate"
cd "$BT_CORE"
"${CMD[@]}"
printf 'Results directory: %s\n' "$RESULTS_DIR"
