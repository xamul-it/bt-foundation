#!/usr/bin/env bash
# auc: fallback post-open per entry CLS recenti.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PSIM_ENV=overnight-ah-auc.key
export ENTRY_TIF=cls

exec "$SCRIPT_DIR/overnight-ah-moo-fallback-run.sh" "$@"
