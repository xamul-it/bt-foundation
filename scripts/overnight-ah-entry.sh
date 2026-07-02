#!/usr/bin/env bash
# OvernightAH — entry schedulata.
# Parametri via env:
#   PSIM_ENV=overnight-ah-no.key|overnight-ah-auc.key|live.key|paper
#   AUCTION=True|False
#   TRADING_MODE=paper|live

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACK_DIR="${BACK_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
BT_CORE="${BT_CORE:-$BACK_DIR/bt-core}"
ENV_DIR="${ENV_DIR:-$BACK_DIR/env}"
PSIM_ENV="${PSIM_ENV:-overnight-ah-auc.key}"
AUCTION="${AUCTION:-True}"
TRADING_MODE="${TRADING_MODE:-paper}"
TICKER="${TICKER:-stable_ah_top10.json}"
STRAT="${STRAT:-overnight_ah_live.OvernightAH}"
DATA_PROVIDER="${DATA_PROVIDER:-yahoo}"
ALPACA_FEED="${ALPACA_FEED:-sip}"
MAX_EXPOSURE="${MAX_EXPOSURE:-1}"
MARGIN_LEVERAGE="${MARGIN_LEVERAGE:-1}"
RUN_ID="${RUN_ID:-}"
BASE_STRATARGS="max_concurrent=5 min_intraday_vol=0.025 max_intraday_vol=0.045 intraday_vol_filter_side='any' ah_lag1_threshold=-0.1 max_adv_participation=0.0025 max_exposure=$MAX_EXPOSURE min_price=0 min_adv=100000000 post_up_cooldown_threshold=0.05 post_up_cooldown_days=5 risk_overlay_mode='off' risk_overlay_lookback=10 risk_overlay_threshold=-0.10 risk_overlay_stress_exposure=1.25"
STRATARGS="${STRATARGS:-$BASE_STRATARGS auction=$AUCTION ${STRATARGS_EXTRA:-}}"

log() { echo "[$(date '+%F %T %Z')] [$PSIM_ENV mode=$TRADING_MODE auction=$AUCTION] $*"; }

source "$BT_CORE/.venv/bin/activate"
set -a; source "$ENV_DIR/$PSIM_ENV"; set +a

cd "$BT_CORE"

T0=$(date +%s%3N)
log "START overnight-ah-entry"

log "STEP 1: download daily bar $DATA_PROVIDER"
T1=$(date +%s%3N)
timeout "${LOAD_TIMEOUT_SEC:-900}" python load_tickers.py \
    --ticker="$TICKER" \
    --provider "$DATA_PROVIDER" \
    --alpaca-feed "$ALPACA_FEED" \
    --timeframe=d \
    --incremental
log "STEP 1 done in $(( $(date +%s%3N) - T1 ))ms"

log "STEP 2: run strategia entry"
T2=$(date +%s%3N)
# Finestra ampia per weekend lunghi/festività e indicatori rolling usati sulla
# barra precedente, ma ancora sostenibile nei run con broker paper/live.
FROM_DAYS="${FROM_DAYS:-120}"
FROMDATE=$(date -d "$FROM_DAYS days ago" '+%Y-%m-%d')
log "STRATARGS: $STRATARGS"
CMD=(
    python btmain.py
    --strat "$STRAT"
    --ticker "$TICKER"
    --stratargs "$STRATARGS"
    --timeframe daily
    --provider "$DATA_PROVIDER"
    --alpaca-feed "$ALPACA_FEED"
    --fromdate "$FROMDATE"
    --mode "$TRADING_MODE"
    --margin-leverage "$MARGIN_LEVERAGE"
    --commission none
)
if [[ -n "$RUN_ID" ]]; then
    CMD+=(--id "$RUN_ID")
fi
"${CMD[@]}"
log "STEP 2 done in $(( $(date +%s%3N) - T2 ))ms"

log "END overnight-ah-entry — totale $(( $(date +%s%3N) - T0 ))ms"
