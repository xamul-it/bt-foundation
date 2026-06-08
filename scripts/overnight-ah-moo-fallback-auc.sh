#!/usr/bin/env bash
# auc: fallback post-open per entry CLS recenti.

set -euo pipefail

export PSIM_ENV=overnight-ah-auc.key
export ENTRY_TIF=cls

exec /home/htpc/backtrader/scripts/overnight-ah-moo-fallback-run.sh "$@"
