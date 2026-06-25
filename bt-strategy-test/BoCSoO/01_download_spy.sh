#!/usr/bin/env bash
# =============================================================================
# 01_download_spy.sh — Scarica SPY come benchmark per la decomposizione AH/RTH
# =============================================================================
# Usa load_tickers.py (bt-core) per scaricare SPY da Yahoo Finance e salvarlo
# in data/d/yahoo/ nel formato standard del workspace (stesso degli altri asset).
#
# SPY è necessario come benchmark per:
#   - calcolo del beta in 02_decompose.py
#   - riferimento visivo nei grafici equity curve
#
# Usage: bash 01_download_spy.sh [--from YYYY-MM-DD]
# Default from-date: 2000-01-01
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BT_CORE="$REPO_ROOT/bt-core"
FROM_DATE="${1:-2000-01-01}"

echo "→ Attivazione venv..."
cd "$BT_CORE"
source .venv/bin/activate

echo "→ Download SPY da Yahoo Finance (from $FROM_DATE)..."
python load_tickers.py \
    --ticker SPY \
    --provider yahoo \
    --fromdate "$FROM_DATE"

echo "✓ SPY salvato in data/d/yahoo/SPY.csv"
