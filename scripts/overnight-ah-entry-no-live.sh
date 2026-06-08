#!/usr/bin/env bash
# Live: entry market standard prima della close, senza auction.

set -euo pipefail

export PSIM_ENV=live.key
export AUCTION=False
export TRADING_MODE=live

exec /home/htpc/backtrader/scripts/overnight-ah-entry.sh "$@"
