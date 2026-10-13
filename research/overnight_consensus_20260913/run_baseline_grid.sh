#!/usr/bin/env bash
set -euo pipefail

if (($# != 2)); then
  printf 'usage: %s development|challenger selection_max\n' "$0" >&2
  exit 2
fi

profile=$1
selection_max=$2
case "$profile" in
  development|challenger) ;;
  *) printf 'unknown profile: %s\n' "$profile" >&2; exit 2 ;;
esac
if ! [[ "$selection_max" =~ ^[0-9]+$ ]] || ((selection_max < 1)); then
  printf 'selection_max must be a positive integer\n' >&2
  exit 2
fi

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root_dir/bt-core"
source "../config-common/scheduled/strategies/overnight-ah-${profile}.env"

min_concurrent=3
if ((selection_max < 3)); then min_concurrent=$selection_max; fi
run_id="${profile}_waterfall_cap10k_slots${selection_max}_2000_20260910"
.venv/bin/python btmain.py \
  --strat "$STRAT" --ticker "$TICKER" \
  --stratargs "$STRATARGS max_notional_per_trade=10000 max_concurrent=${selection_max} min_concurrent=${min_concurrent} auction=True" \
  --timeframe daily --provider "$DATA_PROVIDER" \
  --fromdate 2000-01-03 --todate 2026-09-09 \
  --margin-leverage 1 --commission alpaca --cash 100000 \
  --id "$run_id" > "../out/${run_id}.log" 2>&1
printf '%s complete\n' "$run_id"
