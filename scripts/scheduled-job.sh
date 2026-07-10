#!/usr/bin/env bash
# Generic entry point for scheduled trading jobs.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scheduled-job.sh [--check|--dry-run] PROFILE PHASE [-- handler args]

PROFILE is loaded from $BT_SCHEDULED_CONFIG_DIR/PROFILE.env.
PHASE is one of: entry, exit, exit-fallback.
EOF
}

ACTION=run
case "${1:-}" in
    --check) ACTION=check; shift ;;
    --dry-run) ACTION=dry-run; shift ;;
    -h|--help) usage; exit 0 ;;
esac

[[ $# -ge 2 ]] || { usage >&2; exit 64; }
PROFILE=$1
PHASE=$2
shift 2
[[ "${1:-}" != -- ]] || shift

[[ "$PROFILE" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || {
    echo "Invalid profile name: $PROFILE" >&2
    exit 64
}
case "$PHASE" in
    entry|exit|exit-fallback) ;;
    *) echo "Invalid phase: $PHASE" >&2; exit 64 ;;
esac

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

required=(ROLE TRADING_MODE CODE_ROOT ACCOUNT_ENV STRATEGY_CONFIG)
for name in "${required[@]}"; do
    [[ -n "${!name:-}" ]] || { echo "Missing $name in $PROFILE_FILE" >&2; exit 78; }
done

[[ "$ROLE" == "$PROFILE" ]] || {
    echo "ROLE=$ROLE does not match profile $PROFILE" >&2
    exit 78
}
[[ "$TRADING_MODE" == paper || "$TRADING_MODE" == live ]] || {
    echo "TRADING_MODE must be paper or live" >&2
    exit 78
}

CODE_ROOT=$(realpath -e "$CODE_ROOT")
if [[ "$STRATEGY_CONFIG" != /* ]]; then
    STRATEGY_CONFIG=$CODE_ROOT/$STRATEGY_CONFIG
fi
[[ -r "$STRATEGY_CONFIG" ]] || { echo "Missing strategy config: $STRATEGY_CONFIG" >&2; exit 66; }
[[ -r "$ACCOUNT_ENV" ]] || { echo "Missing account env: $ACCOUNT_ENV" >&2; exit 66; }

if [[ "$ROLE" == live ]]; then
    [[ "$TRADING_MODE" == live ]] || { echo "The live profile must use TRADING_MODE=live" >&2; exit 78; }
    LIVE_CODE_ROOT=${BT_LIVE_CODE_ROOT:-/home/htpc/backtrader-prod}
    [[ "$CODE_ROOT" == "$(realpath -m "$LIVE_CODE_ROOT")" ]] || {
        echo "The live profile must use $LIVE_CODE_ROOT (got $CODE_ROOT)" >&2
        exit 78
    }
    branch=$(git -C "$CODE_ROOT" branch --show-current)
    [[ "$branch" == prod ]] || { echo "Live checkout is on '$branch', expected 'prod'" >&2; exit 78; }
else
    [[ "$TRADING_MODE" == paper ]] || {
        echo "Non-live profile $ROLE cannot use TRADING_MODE=$TRADING_MODE" >&2
        exit 78
    }
fi

set -a
# shellcheck source=/dev/null
source "$STRATEGY_CONFIG"
set +a

case "$PHASE" in
    entry) HANDLER=${ENTRY_HANDLER:-} ;;
    exit) HANDLER=${EXIT_HANDLER:-} ;;
    exit-fallback) HANDLER=${EXIT_FALLBACK_HANDLER:-} ;;
esac
[[ "$HANDLER" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || {
    echo "No valid handler configured for phase $PHASE" >&2
    exit 78
}
HANDLER_PATH=$CODE_ROOT/scripts/scheduled/handlers/$HANDLER.sh
[[ -x "$HANDLER_PATH" ]] || { echo "Handler not executable: $HANDLER_PATH" >&2; exit 66; }

export PROFILE ROLE PHASE CODE_ROOT ACCOUNT_ENV STRATEGY_CONFIG TRADING_MODE
export SCHEDULED_DRY_RUN=0
[[ "$ACTION" == dry-run ]] && export SCHEDULED_DRY_RUN=1

commit=$(git -C "$CODE_ROOT" rev-parse --short=12 HEAD)
core_commit=$(git -C "$CODE_ROOT/bt-core" rev-parse --short=12 HEAD 2>/dev/null || echo unavailable)
echo "profile=$PROFILE phase=$PHASE mode=$TRADING_MODE commit=$commit bt_core_commit=$core_commit"

if [[ "$ACTION" == check ]]; then
    echo "configuration valid"
    exit 0
fi

STATE_DIR=${BT_SCHEDULED_STATE_DIR:-$HOME/.local/state/backtrader}
LOCK_DIR=$STATE_DIR/locks
LOG_DIR=${LOG_DIR:-$STATE_DIR/logs/$PROFILE}
mkdir -p "$LOCK_DIR" "$LOG_DIR"
exec 9>"$LOCK_DIR/$PROFILE.lock"
flock -n 9 || { echo "Another scheduled job is active for profile $PROFILE" >&2; exit 75; }

LOG_FILE=$LOG_DIR/$(date +%F).log
exec > >(tee -a "$LOG_FILE") 2>&1
echo "[$(date '+%F %T %Z')] START profile=$PROFILE phase=$PHASE"
"$HANDLER_PATH" "$@"
echo "[$(date '+%F %T %Z')] END profile=$PROFILE phase=$PHASE"
