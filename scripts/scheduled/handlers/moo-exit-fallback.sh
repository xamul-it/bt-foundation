#!/usr/bin/env bash
set -euo pipefail

BT_CORE=${BT_CORE:-$CODE_ROOT/bt-core}
STRAT_MODULE=${STRAT%.*}
STRAT_CLASS=${STRAT##*.}
STRAT_DIR=${STRAT_MODULE,,}
RUN_ID=${RUN_ID:-scheduled_${PROFILE}}
POLICY_FILE="$BT_CORE/out/$STRAT_DIR/$STRAT_CLASS/$RUN_ID/today.json"

CMD=(python "$CODE_ROOT/bin/submit_moo.py" --fallback-market --all-longs --cancel-pending-sells --entry-tif "${ENTRY_TIF:-gtc}" --opg-failed-policy-file "$POLICY_FILE")
CMD+=("$@")
if [[ "${SCHEDULED_DRY_RUN:-0}" == 1 ]]; then
    printf 'run: '; printf '%q ' "${CMD[@]}"; printf '\n'
    exit 0
fi

source "$CODE_ROOT/bt-core/.venv/bin/activate"
set -a; source "$ACCOUNT_ENV"; set +a
exec "${CMD[@]}"
