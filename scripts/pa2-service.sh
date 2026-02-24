#!/bin/bash
BACK_DIR=/home/htpc/backtrader

source $BACK_DIR/backtrader/backtrader/bin/activate
source $BACK_DIR/.PA2
cd $BACK_DIR/backtrader
$BACK_DIR/backtrader/backtrader/bin/python $BACK_DIR/backtrader/btmain.py --strat=intraday.HMA --ticke="HMA_top9.json"  --timeframe=minutes --live --alpaca-mode=proxy --provider=alpaca --mode=paper --stratargs="period=16 inverted=True exitbar=6" --debug --audit-full
