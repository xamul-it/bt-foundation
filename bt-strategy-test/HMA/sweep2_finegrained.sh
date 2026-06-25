#!/bin/bash
# ============================================================
# HMA Sweep 2 — Fine-grained intorno al best di Sweep1
# Base: period=14, exitbar=6, inverted=True
# Output: out/intraday/HMA/tuning3/
# Analisi: python3 out/intraday/HMA/tuning3/analyze_sweep.py
# ============================================================
cd "$(dirname "$0")/../../bt-core"
source .venv/bin/activate
set -a; source /home/htpc/backtrader/env/pa2; set +a

BASE="--strat intraday.HMA --ticker HMA_top9.json \
      --fromdate 2025-01-01 --todate 2026-03-06 \
      --provider alpaca --timeframe minutes --commission none"

PROGRESS_LOG="logs/hma_sweep2_progress.log"
RUN_LOG="logs/hma_sweep2.log"
mkdir -p logs

echo "=== HMA Sweep2 fine-grained — $(date) ===" | tee "$PROGRESS_LOG"

run() {
    local label=$1; local args=$2
    echo "[$(date +%H:%M:%S)] Starting $label" | tee -a "$PROGRESS_LOG"
    python btmain.py $BASE --stratargs "$args" --id "tuning3/$label" >> "$RUN_LOG" 2>&1
    echo "[$(date +%H:%M:%S)] Done    $label — exit $?" | tee -a "$PROGRESS_LOG"
}

# period 13-15, exitbar 5-7, intorno al best p14_eb6
run "p13_eb5"  "period=13 exitbar=5 inverted=True"
run "p13_eb6"  "period=13 exitbar=6 inverted=True"
run "p13_eb7"  "period=13 exitbar=7 inverted=True"
run "p14_eb5"  "period=14 exitbar=5 inverted=True"
run "p14_eb7"  "period=14 exitbar=7 inverted=True"
run "p15_eb5"  "period=15 exitbar=5 inverted=True"
run "p15_eb6"  "period=15 exitbar=6 inverted=True"
run "p15_eb7"  "period=15 exitbar=7 inverted=True"

echo "=== Sweep2 Done — $(date) ===" | tee -a "$PROGRESS_LOG"
echo "Risultati: python3 out/intraday/HMA/tuning3/analyze_sweep.py"
