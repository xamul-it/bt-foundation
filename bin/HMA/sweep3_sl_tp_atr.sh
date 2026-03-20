#!/bin/bash
# ============================================================
# HMA Sweep 3 — Stop Loss / Take Profit / ATR filter
# Base fissa: period=14, exitbar=6, inverted=True
# Output: out/intraday/HMA/tuning4/
# Analisi: python3 out/intraday/HMA/tuning4/analyze_sweep.py
# ============================================================
cd "$(dirname "$0")/../../bt-core"
source .venv/bin/activate
set -a; source /home/htpc/backtrader/env/pa2; set +a

BASE="--strat intraday.HMA --ticker HMA_top9.json \
      --fromdate 2025-01-01 --todate 2026-03-06 \
      --provider alpaca --timeframe minutes --commission none"
FIXED="period=14 exitbar=6 inverted=True"

PROGRESS_LOG="logs/hma_sweep3_progress.log"
RUN_LOG="logs/hma_sweep3.log"
mkdir -p logs

echo "=== HMA Sweep3 SL/TP/ATR — $(date) ===" | tee "$PROGRESS_LOG"

run() {
    local label=$1; local extra=$2
    echo "[$(date +%H:%M:%S)] Starting $label" | tee -a "$PROGRESS_LOG"
    python btmain.py $BASE --stratargs "$FIXED $extra" --id "tuning4/$label" >> "$RUN_LOG" 2>&1
    echo "[$(date +%H:%M:%S)] Done    $label — exit $?" | tee -a "$PROGRESS_LOG"
}

# --- Solo SL ---
run "sl003"        "sl_pct=0.003"
run "sl005"        "sl_pct=0.005"
run "sl010"        "sl_pct=0.010"
run "sl015"        "sl_pct=0.015"

# --- Solo TP ---
run "tp005"        "tp_pct=0.005"
run "tp010"        "tp_pct=0.010"
run "tp020"        "tp_pct=0.020"

# --- SL + TP combinati ---
run "sl005_tp010"  "sl_pct=0.005 tp_pct=0.010"
run "sl005_tp020"  "sl_pct=0.005 tp_pct=0.020"
run "sl010_tp020"  "sl_pct=0.010 tp_pct=0.020"
run "sl003_tp010"  "sl_pct=0.003 tp_pct=0.010"

# --- Solo ATR filter (volatilità minima richiesta per entrare) ---
run "atr002"       "atr_min_pct=0.002"
run "atr003"       "atr_min_pct=0.003"
run "atr005"       "atr_min_pct=0.005"

# --- SL + ATR ---
run "sl005_atr003" "sl_pct=0.005 atr_min_pct=0.003"
run "sl005_atr005" "sl_pct=0.005 atr_min_pct=0.005"

echo "=== Sweep3 Done — $(date) ===" | tee -a "$PROGRESS_LOG"
echo "Risultati: python3 out/intraday/HMA/tuning4/analyze_sweep.py"
