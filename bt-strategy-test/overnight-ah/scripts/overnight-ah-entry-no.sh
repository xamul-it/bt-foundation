#!/usr/bin/env bash
# no: entry market standard prima della close, senza auction.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PSIM_ENV=overnight-ah-no.key
export AUCTION=False
export TRADING_MODE=paper
export MAX_EXPOSURE=2
export MARGIN_LEVERAGE=2

exec "$SCRIPT_DIR/overnight-ah-entry.sh" "$@"
