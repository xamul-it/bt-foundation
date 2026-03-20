#!/bin/bash
# Process all symbols from alpaca data folder

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../../config/data/m/alpaca"
VENV="$SCRIPT_DIR/../../backtrader/bin/activate"

# Activate virtual environment
source "$VENV"

# Get all CSV files
SYMBOLS=$(ls -1 "$DATA_DIR"/*.csv | xargs -n1 basename | sed 's/.csv//')

echo "=================================="
echo "Processing all symbols from alpaca data"
echo "=================================="
echo "Total symbols: $(echo "$SYMBOLS" | wc -l)"
echo ""

# Process each symbol
for symbol in $SYMBOLS; do
    echo "[$symbol] Processing..."

    # Run 01-fast_backtest.py
    python 01-fast_backtest.py \
        --symbol "$symbol" \
        --input "$DATA_DIR/${symbol}.csv" \
        --output-dir results \
        2>&1 | grep -E "(Processing|Salvati|ERROR|WARNING)" || true

    echo "[$symbol] Done"
    echo ""
done

echo "=================================="
echo "✅ All symbols processed!"
echo "=================================="
