#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT_DIR/bt-core"
source ../config-common/scheduled/strategies/overnight-ah-challenger.env

if (($#)); then
  values=("$@")
else
  values=({1..20} 30 50 102)
fi

for selection_top_n in "${values[@]}"; do
  run_id="consensus_fullpool_select${selection_top_n}_2000_20260910"
  .venv/bin/python btmain.py \
    --strat overnight_ah_consensus.OvernightAHConsensus \
    --ticker "$TICKER" \
    --stratargs "$STRATARGS monthly_universe_top_n=10000 selection_top_n=${selection_top_n} max_concurrent=20 min_concurrent=3 max_notional_per_trade=10000 ah_lag1_threshold=0 auction=True" \
    --timeframe daily --provider "$DATA_PROVIDER" \
    --fromdate 2000-01-03 --todate 2026-09-09 \
    --margin-leverage 1 --commission alpaca --cash 100000 \
    --id "$run_id" > "../out/${run_id}.log" 2>&1
  printf '%s complete\n' "$run_id"
done
