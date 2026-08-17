#!/usr/bin/env bash
# Refresh mensile del pannello indicatori "flat_panel" usato da
# OvernightAHFlatComposite (monthly_universe_score_mode='flat_panel').
#
# Rigenera, con i dati grezzi ad oggi, l'intera catena gia' validata negli
# Studi 5/6 (docs/context/ah_context.md) e documentata in
# docs/context/ah_flat_composite_deployment_brief.md:
#
#   config-common/data/d/yahoo_adj/*.csv  (gia' aggiornati ogni giorno da
#       backtrader-entry.sh via load_tickers.py --incremental -- NON tocca
#       questo script)
#     -> daily_panel_full_history.py
#     -> indicator_panel.py
#     -> score_panel.py
#     -> export_flat_panel_csv.py
#     -> config-common/indicator_panel/flat_panel_live.csv (versionato)
#
# Gira una volta al mese (il lookback max tra gli indicatori e' 252 giorni
# di trading, le feature sono ex-ante e calcolate una sola volta a inizio
# mese dalla strategia stessa) -- non serve un refresh piu' frequente.
#
# Output intermedi "bulk" (parquet, rigenerabili da OHLCV grezzo) sotto
# /mnt/Backup/overnight_ah_tuning/live/ -- SEPARATO dalla cartella di
# ricerca originale (senza suffisso /live/), che resta il riferimento
# congelato usato dagli Studi 5/6 per eventuali fidelity check futuri (non
# sovrascriverla).
#
# Non tocca overnight_ah.py di produzione, non scrive in research/out/
# (gitignored), non piazza ordini, non richiede credenziali broker.

set -euo pipefail

CODE_ROOT=${CODE_ROOT:-/home/htpc/backtrader}
RESEARCH_DIR="$CODE_ROOT/bt-strategy-test/overnight-ah/research"
LIVE_DATA_DIR=${LIVE_DATA_DIR:-/mnt/Backup/overnight_ah_tuning/live}
OUT_CSV=${OUT_CSV:-$CODE_ROOT/config-common/indicator_panel/flat_panel_live.csv}

if [[ "${SCHEDULED_DRY_RUN:-0}" == 1 ]]; then
    echo "cd $RESEARCH_DIR"
    echo "python daily_panel_full_history.py --out-dir $LIVE_DATA_DIR/daily_panel_full_history"
    echo "python indicator_panel.py --panel $LIVE_DATA_DIR/daily_panel_full_history/daily_panel.parquet --out-dir $LIVE_DATA_DIR/indicator_panel"
    echo "python score_panel.py --panel $LIVE_DATA_DIR/daily_panel_full_history/daily_panel.parquet --out-dir $LIVE_DATA_DIR/score_panel"
    echo "python export_flat_panel_csv.py --data-dir $LIVE_DATA_DIR --out $OUT_CSV"
    exit 0
fi

# shellcheck source=/dev/null
source "$CODE_ROOT/bt-core/.venv/bin/activate"
cd "$RESEARCH_DIR"

echo "[$(date '+%F %T %Z')] START refresh-indicator-panel"

python daily_panel_full_history.py \
    --out-dir "$LIVE_DATA_DIR/daily_panel_full_history"

python indicator_panel.py \
    --panel "$LIVE_DATA_DIR/daily_panel_full_history/daily_panel.parquet" \
    --out-dir "$LIVE_DATA_DIR/indicator_panel"

python score_panel.py \
    --panel "$LIVE_DATA_DIR/daily_panel_full_history/daily_panel.parquet" \
    --out-dir "$LIVE_DATA_DIR/score_panel"

mkdir -p "$(dirname "$OUT_CSV")"
python export_flat_panel_csv.py \
    --data-dir "$LIVE_DATA_DIR" \
    --out "$OUT_CSV"

echo "[$(date '+%F %T %Z')] END refresh-indicator-panel: wrote $OUT_CSV"
