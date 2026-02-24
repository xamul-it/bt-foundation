#!/bin/bash
BACK_DIR=/home/htpc/backtrader

source $BACK_DIR/backtrader/backtrader/bin/activate
source $BACK_DIR/.PA1
cd $BACK_DIR/backtrader 
TICKER="NASDAQ_100_US.json"

python ./load_tickers.py --ticker="$TICKER" --provider yahoo --timeframe=d
#python ./btmain.py --strat=weekly.RMAStrategy --ticker="$TICKER" --fromdate=2024-01-01  --stratargs="selnum=2 amount=-1 max_volatility=0.04 min_volume=40000 trail_stop=0.02 flatten_on_close=False period=150" --timeframe=daily --provider=yahoo --cash=5000 --mode paper
python ./btmain.py --strat=weekly.RMAStrategy --ticker="$TICKER" --fromdate=2000-01-01  --stratargs="selnum=2 amount=-1 max_volatility=0.04 min_volume=40000 trail_stop=0.02 flatten_on_close=False period=150 regime_filter=False max_amount=20000 tstat_entry_min=0.5" --timeframe=daily --provider=yahoo --cash=20000 --commission none --mode paper
