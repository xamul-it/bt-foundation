#!/usr/bin/env bash
set -euo pipefail

CMD=(python "$CODE_ROOT/bin/submit_moo.py" --all-longs --entry-tif "${ENTRY_TIF:-gtc}")
CMD+=("$@")
if [[ "${SCHEDULED_DRY_RUN:-0}" == 1 ]]; then
    printf 'run: '; printf '%q ' "${CMD[@]}"; printf '\n'
    exit 0
fi

source "$CODE_ROOT/bt-core/.venv/bin/activate"
set -a; source "$ACCOUNT_ENV"; set +a
exec "${CMD[@]}"
