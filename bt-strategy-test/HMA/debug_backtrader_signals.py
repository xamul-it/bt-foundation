#!/usr/bin/env python3
"""
Debug Backtrader HMA Signal Generation

Minimal reproduction to check why signals aren't being generated.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import backtrader as bt
from indicators.atr_sma import ATR_SMA
import pandas as pd

# Load LCID data for 2025-05-09 (day with known trade)
data_file = Path('config/data/m/alpaca/LCID.csv')
df = pd.read_csv(data_file)
df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
df = df.set_index('timestamp')

# Filter to 2025-05-09
day_data = df[(df.index >= '2025-05-09') & (df.index < '2025-05-10')].copy()

print(f"LCID 2025-05-09: {len(day_data)} bars")
print(f"Time range: {day_data.index[0]} to {day_data.index[-1]}")

# Create Backtrader cerebro
cerebro = bt.Cerebro()

# Add data
data = bt.feeds.PandasData(
    dataname=day_data,
    datetime=None,  # Use index
    open='open',
    high='high',
    low='low',
    close='close',
    volume='volume',
    openinterest=-1
)
cerebro.adddata(data, name='LCID')

# Simple strategy to print HMA values
class DebugHMA(bt.Strategy):
    params = (
        ('period', 16),
        ('atr_period', 14),
        ('atr_min', 0.007),
    )

    def __init__(self):
        self.hma = bt.indicators.HMA(self.data, period=self.p.period)
        self.atr = ATR_SMA(self.data, period=self.p.atr_period)

    def next(self):
        dt = self.data.datetime.datetime()

        # Skip if not enough data
        if len(self.hma) < 2:
            return

        hma_curr = self.hma[0]
        hma_prev = self.hma[-1]
        atr_val = self.atr[0]
        atr_pct = atr_val / self.data.close[0]

        # Check time window (15:00-20:00 UTC)
        hour = dt.hour
        minute = dt.minute
        time_minutes = hour * 60 + minute

        trading_start = 15 * 60  # 15:00
        trading_end = 20 * 60    # 20:00
        in_trading = trading_start <= time_minutes <= trading_end

        # Check ATR filter
        atr_pass = atr_pct >= self.p.atr_min

        # Signal detection (inverted mode)
        signal_long = hma_prev > hma_curr   # HMA falling
        signal_short = hma_prev < hma_curr  # HMA rising

        # Print interesting bars
        if in_trading and (signal_long or signal_short):
            print(f"{dt.strftime('%H:%M')}: HMA {hma_prev:.4f} → {hma_curr:.4f}, "
                  f"ATR={atr_pct*100:.3f}% (pass={atr_pass}), "
                  f"LONG={signal_long}, SHORT={signal_short}")

        # Special check for 15:09 (known trade time)
        if hour == 15 and minute == 9:
            print(f"\n⭐ 15:09 CHECK:")
            print(f"  HMA: {hma_prev:.4f} → {hma_curr:.4f} (falling={hma_prev > hma_curr})")
            print(f"  ATR: {atr_pct*100:.3f}% (min={self.p.atr_min*100:.1f}%, pass={atr_pass})")
            print(f"  In trading window: {in_trading}")
            print(f"  LONG signal: {signal_long}")
            print(f"  SHORT signal: {signal_short}\n")

cerebro.addstrategy(DebugHMA)
cerebro.run()
