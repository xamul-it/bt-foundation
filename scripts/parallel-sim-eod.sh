#!/bin/bash
# EOD Analysis: confronto paper vs simulazione
# Genera HTML report e lo salva in out/<strategy>/

BACK_DIR=/home/htpc/backtrader

source $BACK_DIR/backtrader/backtrader/bin/activate
source $BACK_DIR/.parallel-sim-env
cd $BACK_DIR/backtrader

DATE=$(date +%Y%m%d)
OUTPUT="${PSIM_OUT_DIR}/eod_report_${DATE}.html"

echo "[EOD] $(date '+%H:%M:%S') Avvio analisi..."
echo "[EOD] Paper dir : ${PSIM_PAPER_DIR}"
echo "[EOD] Sim dir   : ${PSIM_SIM_DIR}"
echo "[EOD] Output    : ${OUTPUT}"

python bin/parallel_sim/eod_analysis.py \
    --paper "${PSIM_PAPER_DIR}" \
    --sim   "${PSIM_SIM_DIR}" \
    --output "${OUTPUT}" \
    --tolerance "${PSIM_TOLERANCE:-300}"

echo "[EOD] $(date '+%H:%M:%S') Completato: ${OUTPUT}"
