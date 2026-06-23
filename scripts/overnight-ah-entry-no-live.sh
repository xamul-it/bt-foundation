#!/usr/bin/env bash
# Live: entry market standard prima della close, senza auction.

set -euo pipefail

export PSIM_ENV=live.key
export AUCTION=False
export TRADING_MODE=live
export MAX_EXPOSURE=2
export MARGIN_LEVERAGE=2

exec /home/htpc/backtrader/scripts/overnight-ah-entry.sh "$@"
