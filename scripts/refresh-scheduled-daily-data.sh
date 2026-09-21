#!/usr/bin/env bash
# Refresh the shared raw + adjusted daily cache once, after the US close.
# Entry jobs never perform network I/O and only read through yesterday.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BT_CORE=$ROOT/bt-core
ACCOUNT_ENV=${ACCOUNT_ENV:-$HOME/.config/backtrader/accounts/development.env}
DATA_THROUGH=${DATA_THROUGH:-$(date '+%Y-%m-%d')}

set -a
# Calendar lookup benefits from credentials; Yahoo downloads themselves do
# not use the trading account.
source "$ACCOUNT_ENV"
set +a
source "$BT_CORE/.venv/bin/activate"
cd "$BT_CORE"

python load_tickers.py \
    --ticker=yahoo_adj_research_universe_hedge.json \
    --provider yahoo_adj --alpaca-feed sip --timeframe=d \
    --todate "$DATA_THROUGH" --incremental
python load_tickers.py \
    --ticker=yahoo_adj_research_universe.json \
    --provider yahoo_adj --alpaca-feed sip --timeframe=d \
    --todate "$DATA_THROUGH" --incremental
