#!/bin/bash
# ============================================================
# HMA Sweep 1 — Grid search su period e exitbar
# Base: inverted=True, use_calendar=True, commission=none
# Output: out/intraday/HMA/tuning2/
# Analisi: python3 out/intraday/HMA/tuning2/analyze_sweep.py
# ============================================================
cd "$(dirname "$0")/../../bt-core"
source .venv/bin/activate
set -a; source /home/htpc/backtrader/env/pa2; set +a

BASE="--strat intraday.HMA --ticker HMA_top9.json \
      --fromdate 2025-01-01 --todate 2026-03-06 \
      --provider alpaca --timeframe minutes --commission none"

PROGRESS_LOG="logs/hma_sweep1_progress.log"
RUN_LOG="logs/hma_sweep1.log"
mkdir -p logs

echo "=== HMA Sweep1 period/exitbar — $(date) ===" | tee "$PROGRESS_LOG"

run() {
    local label=$1; local args=$2
    echo "[$(date +%H:%M:%S)] Starting $label" | tee -a "$PROGRESS_LOG"
    python btmain.py $BASE --stratargs "$args" --id "tuning2/$label" >> "$RUN_LOG" 2>&1
    echo "[$(date +%H:%M:%S)] Done    $label — exit $?" | tee -a "$PROGRESS_LOG"
}

run "p16_eb6"   "period=16 exitbar=6  inverted=True"
run "p14_eb6"   "period=14 exitbar=6  inverted=True"
run "p12_eb6"   "period=12 exitbar=6  inverted=True"
run "p20_eb6"   "period=20 exitbar=6  inverted=True"
run "p16_eb4"   "period=16 exitbar=4  inverted=True"
run "p16_eb8"   "period=16 exitbar=8  inverted=True"
run "p16_eb3"   "period=16 exitbar=3  inverted=True"
run "p16_ebn1"  "period=16 exitbar=-1 inverted=True"
run "p14_eb4"   "period=14 exitbar=4  inverted=True"
run "p14_ebn1"  "period=14 exitbar=-1 inverted=True"
run "p10_eb6"   "period=10 exitbar=6  inverted=True"
run "p10_eb4"   "period=10 exitbar=4  inverted=True"

echo "=== Sweep1 Done — $(date) ===" | tee -a "$PROGRESS_LOG"
echo "Risultati: python3 out/intraday/HMA/tuning2/analyze_sweep.py"
