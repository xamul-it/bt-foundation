#!/usr/bin/env bash
# auc: entry auction CLS.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PSIM_ENV=overnight-ah-auc.key
export AUCTION=True
export MAX_EXPOSURE=3
export MARGIN_LEVERAGE=3

exec "$SCRIPT_DIR/overnight-ah-entry.sh" "$@"
