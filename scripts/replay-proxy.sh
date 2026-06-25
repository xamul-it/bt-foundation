#!/bin/bash
# Replay ZMQ Proxy
# Uso: ./replay-proxy.sh 2026-01-12 [--speed 60] [--log-dir out/dump]

BACK_DIR=/home/htpc/backtrader
DATE=${1:?"Uso: $0 YYYY-MM-DD [--speed N] [--log-dir DIR]"}
shift  # Rimuovi la data, il resto va al proxy

source $BACK_DIR/bt-gateway/.venv/bin/activate
cd $BACK_DIR/bt-gateway

echo "[replay-proxy] Data: $DATE, args: $@"
exec python -m bt_alpaca_zmq.replay_zmq_proxy \
    --date "$DATE" \
    "$@"
