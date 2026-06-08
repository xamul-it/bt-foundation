#!/usr/bin/env bash
# auc: entry auction CLS.

set -euo pipefail

export PSIM_ENV=overnight-ah-auc.key
export AUCTION=True
export MAX_EXPOSURE=3
export MARGIN_LEVERAGE=3

exec /home/htpc/backtrader/scripts/overnight-ah-entry.sh "$@"
