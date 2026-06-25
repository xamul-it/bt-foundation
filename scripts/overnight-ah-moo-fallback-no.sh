#!/usr/bin/env bash
# no: fallback post-open per entry market/GTC recenti.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PSIM_ENV=overnight-ah-no.key
export ENTRY_TIF=gtc

exec "$SCRIPT_DIR/overnight-ah-moo-fallback-run.sh" "$@"
