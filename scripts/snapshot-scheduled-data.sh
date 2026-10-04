#!/usr/bin/env bash
# Freeze the exact adjusted feed/config consumed by one scheduled entry.
# Parquet files are hard-linked: creation is cheap, while the loader's
# atomic os.replace on the next refresh leaves this point-in-time inode intact.
set -euo pipefail

[[ $# -eq 3 ]] || { echo "usage: $0 PROFILE TRADING_DATE CODE_ROOT" >&2; exit 64; }
PROFILE=$1
TRADING_DATE=$2
CODE_ROOT=$(realpath -e "$3")
STATE_DIR=${BT_SCHEDULED_STATE_DIR:-$HOME/.local/state/backtrader}
SNAPSHOT_ROOT=$STATE_DIR/market-data-snapshots/$PROFILE/$TRADING_DATE/config-common
SOURCE_CONFIG=$CODE_ROOT/config-common

if [[ -e "$SNAPSHOT_ROOT/.complete" ]]; then
    echo "market-data snapshot already exists: $SNAPSHOT_ROOT"
    exit 0
fi

mkdir -p "$SNAPSHOT_ROOT/data/d/yahoo_adj" "$SNAPSHOT_ROOT/tickers" "$SNAPSHOT_ROOT/indicator_panel"
for source in "$SOURCE_CONFIG"/data/d/yahoo_adj/*.parquet; do
    [[ -e "$source" ]] || continue
    target=$SNAPSHOT_ROOT/data/d/yahoo_adj/$(basename "$source")
    ln "$source" "$target" 2>/dev/null || cp -a "$source" "$target"
done
for source in "$SOURCE_CONFIG"/tickers/*; do
    [[ -f "$source" ]] || continue
    cp -a "$source" "$SNAPSHOT_ROOT/tickers/"
done
for source in "$SOURCE_CONFIG"/indicator_panel/*; do
    [[ -f "$source" ]] || continue
    cp -a "$source" "$SNAPSHOT_ROOT/indicator_panel/"
done
touch "$SNAPSHOT_ROOT/.complete"
echo "market-data snapshot created: $SNAPSHOT_ROOT"
