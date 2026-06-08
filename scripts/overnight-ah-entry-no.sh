#!/usr/bin/env bash
# no: entry market standard prima della close, senza auction.

set -euo pipefail

export PSIM_ENV=overnight-ah-no.key
export AUCTION=False
export TRADING_MODE=paper
export MAX_EXPOSURE=2
export MARGIN_LEVERAGE=2

exec /home/htpc/backtrader/scripts/overnight-ah-entry.sh "$@"
