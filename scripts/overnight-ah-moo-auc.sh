#!/usr/bin/env bash
# auc: chiude entry CLS recenti con MOO OPG.

set -euo pipefail

export PSIM_ENV=overnight-ah-auc.key
export ENTRY_TIF=cls

exec /home/htpc/backtrader/scripts/overnight-ah-moo-run.sh "$@"
