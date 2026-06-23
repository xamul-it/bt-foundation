#!/usr/bin/env bash
# Live: chiude entry market/GTC recenti con MOO OPG.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PSIM_ENV=live.key
export ENTRY_TIF=gtc

exec "$SCRIPT_DIR/overnight-ah-moo-run.sh" --wait-window "$@"
