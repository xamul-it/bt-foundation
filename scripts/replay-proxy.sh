#!/bin/bash
# Replay ZMQ Proxy
# Uso: ./replay-proxy.sh 2026-01-12 [--speed 60] [--log-dir out/dump]

BACK_DIR=/home/htpc/backtrader
DATE=${1:?"Uso: $0 YYYY-MM-DD [--speed N] [--log-dir DIR]"}
shift  # Rimuovi la data, il resto va al proxy

source $BACK_DIR/backtrader/backtrader/bin/activate
cd $BACK_DIR/backtrader

echo "[replay-proxy] Data: $DATE, args: $@"
exec python replay_zmq_proxy.py \
    --date "$DATE" \
    "$@"
