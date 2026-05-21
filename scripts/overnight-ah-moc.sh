#!/usr/bin/env bash
# OvernightAH — MOC Entry (~15:45 ET, lunedì-venerdì)
# 1) Scarica/aggiorna daily bar Alpaca (inclusa barra di oggi parziale)
# 2) Applica filtri e sottomette ordini MOC via btmain

set -euo pipefail

BACK_DIR="/home/htpc/backtrader"
BT_CORE="$BACK_DIR/bt-core"
TICKER="stable_ah_top10.json"
STRATARGS="max_concurrent=5 min_intraday_vol=0.025 max_intraday_vol=0.045 ah_lag1_threshold=-0.1 max_adv_participation=0.0025 max_exposure=2 min_price=0 min_adv=100000000"
UNIVERSE_FILTER="$BACK_DIR/bin/overnight_ah/out/monthly_universe_lists/monthly_lists_total_top10_trades20_keep20_enter10.csv"

log() { echo "[$(date '+%F %T %Z')] $*"; }

source "$BT_CORE/.venv/bin/activate"
set -a; source "$BACK_DIR/env/psim-b"; set +a

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
if [[ -f "$UNIVERSE_FILTER" ]]; then
    STRATARGS="$STRATARGS universe_filter_file='$UNIVERSE_FILTER' universe_filter_delay_months=1"
else
    log "WARN: universe filter non trovato: $UNIVERSE_FILTER"
fi
log "STRATARGS: $STRATARGS"
python btmain.py \
    --strat overnight_ah.OvernightAH \
    --ticker "$TICKER" \
    --stratargs "$STRATARGS" \
    --timeframe daily \
    --provider alpaca \
    --fromdate "$FROMDATE" \
    --mode paper \
    --commission none
log "STEP 2 done in $(( $(date +%s%3N) - T2 ))ms"

log "END overnight-ah-moc — totale $(( $(date +%s%3N) - T0 ))ms"
