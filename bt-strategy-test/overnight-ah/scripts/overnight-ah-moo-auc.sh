#!/usr/bin/env bash
# auc: chiude entry CLS recenti con MOO OPG.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PSIM_ENV=overnight-ah-auc.key
export ENTRY_TIF=cls

exec "$SCRIPT_DIR/overnight-ah-moo-run.sh" "$@"
