#!/usr/bin/env bash
# OvernightAH — MOC Entry (~15:45 ET, lunedì-venerdì)
# 1) Scarica/aggiorna daily bar Alpaca (inclusa barra di oggi parziale)
# 2) Applica filtri e sottomette ordini MOC via btmain

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACK_DIR="${BACK_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
BT_CORE="${BT_CORE:-$BACK_DIR/bt-core}"
ENV_DIR="${ENV_DIR:-$BACK_DIR/env}"
TICKER="stable_ah_top10.json"
STRAT="${STRAT:-overnight_ah.OvernightAH}"
STRATARGS="max_concurrent=5 min_intraday_vol=0.025 max_intraday_vol=0.045 intraday_vol_filter_side='any' ah_lag1_threshold=-0.1 max_adv_participation=0.0025 max_exposure=1 min_price=0 min_adv=100000000"

log() { echo "[$(date '+%F %T %Z')] $*"; }

source "$BT_CORE/.venv/bin/activate"
set -a; source "$ENV_DIR/overnight-ah-auc.key"; set +a

cd "$BT_CORE"

T0=$(date +%s%3N)
log "START overnight-ah-moc"

log "STEP 1: download daily bar Alpaca"
T1=$(date +%s%3N)
python load_tickers.py \
    --ticker="$TICKER" \
    --provider alpaca \
    --timeframe=d \
    --incremental
log "STEP 1 done in $(( $(date +%s%3N) - T1 ))ms"

log "STEP 2: run strategia (MOC entry)"
T2=$(date +%s%3N)
FROMDATE=$(date -d '30 days ago' '+%Y-%m-%d')
log "STRATARGS: $STRATARGS"
python btmain.py \
    --strat "$STRAT" \
    --ticker "$TICKER" \
    --stratargs "$STRATARGS" \
    --timeframe daily \
    --provider alpaca \
    --fromdate "$FROMDATE" \
    --mode paper \
    --commission none
log "STEP 2 done in $(( $(date +%s%3N) - T2 ))ms"

log "END overnight-ah-moc — totale $(( $(date +%s%3N) - T0 ))ms"
