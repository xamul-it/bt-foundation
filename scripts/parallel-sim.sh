#!/bin/bash
# Parallel Paper + Simulation launcher
# Legge la configurazione da /home/htpc/backtrader/.parallel-sim-env

BACK_DIR=/home/htpc/backtrader

source $BACK_DIR/backtrader/backtrader/bin/activate
source $BACK_DIR/env/psim
cd $BACK_DIR/backtrader

exec python bin/parallel_sim/run_parallel.py \
    --strat="${PSIM_STRAT}" \
    --ticker="${PSIM_TICKER}" \
    --stratargs="${PSIM_STRATARGS}" \
    --timeframe="${PSIM_TIMEFRAME:-minutes}" \
    --cash="${PSIM_CASH:-100000}" \
    --commission="${PSIM_COMMISSION:-none}" \
    ${PSIM_DEBUG:+--debug} \
    ${PSIM_AUDIT:+--audit-full} \
    "$@"
