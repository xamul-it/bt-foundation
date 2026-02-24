#!/bin/bash
BACK_DIR=/home/htpc/backtrader

source $BACK_DIR/backtrader/backtrader/bin/activate
source $BACK_DIR/.PA2
cd $BACK_DIR/backtrader 
python $BACK_DIR/backtrader/btmain.py --strat=daily.HMA --debug --ticker="NASDAQ10.json"  --timeframe=minutes --live --alpaca-mode=proxy --provider=alpaca --mode=shadow --stratargs="period=16 inverted=True"
