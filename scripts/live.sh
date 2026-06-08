#!/usr/bin/env bash
# Live: chiude entry market/GTC recenti con MOO OPG.

set -euo pipefail

export PSIM_ENV=live.key
export ENTRY_TIF=gtc

exec /home/htpc/backtrader/scripts/overnight-ah-moo-run.sh --wait-window "$@"
