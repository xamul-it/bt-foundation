#!/usr/bin/env bash
# no: fallback post-open per entry market/GTC recenti.

set -euo pipefail

export PSIM_ENV=overnight-ah-no.key
export ENTRY_TIF=gtc

exec /home/htpc/backtrader/scripts/overnight-ah-moo-fallback-run.sh "$@"
