#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# HMA convergence validation: period=8 + sl_pct=0.57% (p95 MAE)
#
# Step 1 (questo script): esegue pandas sim e mostra metriche
# Step 2 (manuale): esegue BT backtest con stessi parametri
# Step 3: confronta i risultati con compare.py
#
# Uso:
#   bash bt-strategy-test/HMA/validate_sl.sh
#   bash bt-strategy-test/HMA/validate_sl.sh --from 2025-01-01 --to 2025-03-31
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE="$(cd "$SCRIPT_DIR/../.." && pwd)"
BT_CORE="$WORKSPACE/bt-core"

# Parametri
PERIOD=8
EXITBAR=6
SL_PCT=0.0057      # p95 MAE = 0.57%
FROM_DATE="${2:-}"
TO_DATE="${4:-}"
EXTRA_ARGS=""
[ -n "$FROM_DATE" ] && EXTRA_ARGS="$EXTRA_ARGS --from $FROM_DATE"
[ -n "$TO_DATE"   ] && EXTRA_ARGS="$EXTRA_ARGS --to $TO_DATE"

# Attiva venv
source "$BT_CORE/.venv/bin/activate"
cd "$WORKSPACE"

echo "════════════════════════════════════════════════════════════════════════"
echo "  HMA Validate SL  period=$PERIOD  exitbar=$EXITBAR  sl_pct=$SL_PCT"
echo "  sl_on_close=True (replica bt-core)"
echo "════════════════════════════════════════════════════════════════════════"
echo ""

# ── Step 1: Pandas sim ────────────────────────────────────────────────────────
echo "── Step 1: Pandas simulation ────────────────────────────────────────────"
python bt-strategy-test/HMA/sim.py \
    --period $PERIOD \
    --exitbar $EXITBAR \
    --sl $SL_PCT \
    --sl-on-close \
    --out /tmp/hma_pandas_sl.csv \
    $EXTRA_ARGS

echo ""
echo "── Step 2: BT backtest (eseguire manualmente) ───────────────────────────"
echo ""
echo "  cd $BT_CORE"
echo "  source .venv/bin/activate"
echo "  set -a; source $WORKSPACE/env/pa2; set +a"
echo ""
BT_STRATARGS="period=$PERIOD inverted=True exitbar=$EXITBAR sl_pct=$SL_PCT"
BT_CMD="python btmain.py --strat=intraday.HMA --ticker=HMA_top9.json"
BT_CMD="$BT_CMD --timeframe=minutes --provider=alpaca --mode=backtest"
BT_CMD="$BT_CMD --stratargs=\"$BT_STRATARGS\""
[ -n "$FROM_DATE" ] && BT_CMD="$BT_CMD --fromdate=$FROM_DATE"
[ -n "$TO_DATE"   ] && BT_CMD="$BT_CMD --todate=$TO_DATE"
echo "  $BT_CMD"
echo ""
echo "── Step 3: Confronto (dopo aver eseguito il BT) ─────────────────────────"
echo ""
echo "  python bt-strategy-test/HMA/compare.py --bt-dir $BT_CORE/out/intraday/HMA/<run_id>"
echo ""
echo "  Target convergenza: delta trade count <= 6%  (baseline senza SL era -5.3%)"
echo "  Atteso lieve aumento divergenza dovuto a close vs low/high SL check"
