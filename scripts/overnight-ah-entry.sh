#!/usr/bin/env bash
# OvernightAH — entry schedulata.
# Parametri via env:
#   PSIM_ENV=overnight-ah-no.key|overnight-ah-auc.key|live.key|paper
#   AUCTION=True|False
#   TRADING_MODE=paper|live

set -euo pipefail

BACK_DIR="/home/htpc/backtrader"
BT_CORE="$BACK_DIR/bt-core"
PSIM_ENV="${PSIM_ENV:-overnight-ah-auc.key}"
AUCTION="${AUCTION:-True}"
TRADING_MODE="${TRADING_MODE:-paper}"
TICKER="${TICKER:-stable_ah_top10.json}"
MAX_EXPOSURE="${MAX_EXPOSURE:-1}"
MARGIN_LEVERAGE="${MARGIN_LEVERAGE:-1}"
BASE_STRATARGS="max_concurrent=5 min_intraday_vol=0.025 max_intraday_vol=0.045 ah_lag1_threshold=-0.1 max_adv_participation=0.0025 max_exposure=$MAX_EXPOSURE min_price=0 min_adv=100000000"
STRATARGS="${STRATARGS:-$BASE_STRATARGS auction=$AUCTION ${STRATARGS_EXTRA:-}}"

log() { echo "[$(date '+%F %T %Z')] [$PSIM_ENV mode=$TRADING_MODE auction=$AUCTION] $*"; }

source "$BT_CORE/.venv/bin/activate"
set -a; source "$BACK_DIR/env/$PSIM_ENV"; set +a

cd "$BT_CORE"

T0=$(date +%s%3N)
log "START overnight-ah-entry"

log "STEP 1: download daily bar Alpaca"
T1=$(date +%s%3N)
python load_tickers.py \
    --ticker="$TICKER" \
    --provider alpaca \
    --timeframe=d \
    --incremental
log "STEP 1 done in $(( $(date +%s%3N) - T1 ))ms"

log "STEP 2: run strategia entry"
T2=$(date +%s%3N)
FROMDATE=$(date -d '30 days ago' '+%Y-%m-%d')
log "STRATARGS: $STRATARGS"
python btmain.py \
    --strat overnight_ah.OvernightAH \
    --ticker "$TICKER" \
    --stratargs "$STRATARGS" \
    --timeframe daily \
    --provider alpaca \
    --fromdate "$FROMDATE" \
    --mode "$TRADING_MODE" \
    --margin-leverage "$MARGIN_LEVERAGE" \
    --commission none
log "STEP 2 done in $(( $(date +%s%3N) - T2 ))ms"

log "END overnight-ah-entry — totale $(( $(date +%s%3N) - T0 ))ms"
