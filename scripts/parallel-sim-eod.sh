#!/usr/bin/env bash
# EOD Analysis: confronto paper vs simulazione
# Flusso richiesto:
#   1) stop zmq-proxy (che trascina giu' logger + parallel-sim)
#   2) genera report EOD
#   3) start zmq-proxy (che rilancia logger + parallel-sim)

set -euo pipefail

BACK_DIR="/home/htpc/backtrader"
ENV_FILE="$BACK_DIR/env/psim"
PYTHON_BIN="$BACK_DIR/bt-core/.venv/bin/python"
EOD_SCRIPT="$BACK_DIR/bin/parallel_sim/eod_analysis.py"
ZMQ_PROXY_SERVICE="zmq-proxy.service"

log() {
  echo "[EOD] $(date '+%F %T') $*"
}

if [[ -r "$ENV_FILE" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +a
fi

DATE="$(date +%Y%m%d)"
OUTPUT="${PSIM_OUT_DIR}/eod_report_${DATE}.html"

mkdir -p "$(dirname "$OUTPUT")"

restart_proxy=0
cleanup() {
  if [[ "$restart_proxy" -eq 1 ]]; then
    log "Riavvio ${ZMQ_PROXY_SERVICE}..."
    if systemctl --user start "$ZMQ_PROXY_SERVICE"; then
      log "Riavvio ${ZMQ_PROXY_SERVICE} completato"
    else
      log "ERRORE: riavvio ${ZMQ_PROXY_SERVICE} fallito"
      exit 1
    fi
  fi
}
trap cleanup EXIT

log "Stop ${ZMQ_PROXY_SERVICE} (ferma anche logger+parallel-sim)..."
systemctl --user stop "$ZMQ_PROXY_SERVICE"
restart_proxy=1

for i in {1..30}; do
  state="$(systemctl is-active "$ZMQ_PROXY_SERVICE" || true)"
  [[ "$state" == "inactive" || "$state" == "failed" ]] && break
  sleep 1
done
log "Stato ${ZMQ_PROXY_SERVICE} dopo stop: $(systemctl is-active "$ZMQ_PROXY_SERVICE" || true)"
log "Stato parallel-sim.service: $(systemctl is-active parallel-sim.service || true)"
log "Stato zmq-logger.service: $(systemctl is-active zmq-logger.service || true)"

log "Avvio analisi..."
log "Paper dir : ${PSIM_PAPER_DIR}"
log "Sim dir   : ${PSIM_SIM_DIR}"
log "Output    : ${OUTPUT}"

"$PYTHON_BIN" "$EOD_SCRIPT" \
  --paper "${PSIM_PAPER_DIR}" \
  --sim "${PSIM_SIM_DIR}" \
  --output "${OUTPUT}" \
  --tolerance "${PSIM_TOLERANCE:-300}"

log "Completato: ${OUTPUT}"
