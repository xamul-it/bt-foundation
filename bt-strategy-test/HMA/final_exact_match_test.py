#!/usr/bin/env python3
"""
FINAL EXACT MATCH TEST
NO Backtrader orders - Manual position tracking ONLY
MUST MATCH Monte Carlo 100%
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import numpy as np
import backtrader as bt
from indicators.atr_sma import ATR_SMA

# Monte Carlo implementation (reference)
def calculate_hma_mc(close, period=16):
    half_period = int(period / 2)
    sqrt_period = int(np.sqrt(period))

    def wma(series, n):
        weights = np.arange(1, n + 1)
        return series.rolling(n).apply(
            lambda x: np.sum(x * weights) / weights.sum() if len(x) == n else np.nan,
            raw=True
        )

    wma_half = wma(close, half_period)
    wma_full = wma(close, period)
    raw_hma = 2 * wma_half - wma_full
    return wma(raw_hma, sqrt_period)

def calculate_atr_mc(df, period=14):
    high, low, close = df['high'], df['low'], df['close']
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return (atr / close) * 100

def monte_carlo_sim(df):
    df = df.copy()
    df['hma'] = calculate_hma_mc(df['close'])
    df['atr_pct'] = calculate_atr_mc(df)
    df['hour'] = df.index.hour
    df = df[(df['hour'] >= 10) & (df['hour'] < 15)]

    trades, position = [], None
    for i in range(1, len(df)):
        bar, prev = df.iloc[i], df.iloc[i-1]

        if pd.isna(bar['hma']) or pd.isna(prev['hma']) or pd.isna(bar['atr_pct']):
            continue
        if bar['atr_pct'] < 0.7:
            continue

        sig_long = prev['hma'] > bar['hma']
        sig_short = prev['hma'] < bar['hma']

        # SL check
        if position:
            if position['side'] == 'long' and bar['low'] <= position['sl']:
                trades.append({**position, 'exit_time': bar.name, 'exit_price': position['sl'],
                              'exit_reason': 'SL', 'pnl_pct': -0.005})
                position = None
                continue
            elif position['side'] == 'short' and bar['high'] >= position['sl']:
                trades.append({**position, 'exit_time': bar.name, 'exit_price': position['sl'],
                              'exit_reason': 'SL', 'pnl_pct': -0.005})
                position = None
                continue

        # Signal logic
        if sig_long:
            if position and position['side'] == 'short':
                pnl = (position['entry_price'] - bar['close']) / position['entry_price']
                trades.append({**position, 'exit_time': bar.name, 'exit_price': bar['close'],
                              'exit_reason': 'signal', 'pnl_pct': pnl})
            if not position or position['side'] == 'short':
                position = {'side': 'long', 'entry_time': bar.name, 'entry_price': bar['close'],
                           'sl': bar['close'] * 0.995}
        elif sig_short:
            if position and position['side'] == 'long':
                pnl = (bar['close'] - position['entry_price']) / position['entry_price']
                trades.append({**position, 'exit_time': bar.name, 'exit_price': bar['close'],
                              'exit_reason': 'signal', 'pnl_pct': pnl})
            if not position or position['side'] == 'long':
                position = {'side': 'short', 'entry_time': bar.name, 'entry_price': bar['close'],
                           'sl': bar['close'] * 1.005}

    # EOD close
    if position:
        last = df.iloc[-1]
        pnl = ((last['close'] - position['entry_price']) / position['entry_price']
               if position['side'] == 'long' else
               (position['entry_price'] - last['close']) / position['entry_price'])
        trades.append({**position, 'exit_time': last.name, 'exit_price': last['close'],
                      'exit_reason': 'EOD', 'pnl_pct': pnl})

    return pd.DataFrame(trades)

# Backtrader with MANUAL tracking (no orders)
class ManualTrackStrategy(bt.Strategy):
    def __init__(self):
        self.hma = bt.indicators.HMA(self.data, period=16)
        self.atr = ATR_SMA(self.data, period=14)
        self.trades_log = []
        self.pos_tracker = None

    def next(self):
        dt = self.data.datetime.datetime()
        if dt.hour < 10 or dt.hour >= 15:
            return

        # EOD close: Close position at end of trading window (14:59) BEFORE other filters
        if dt.hour == 14 and dt.minute == 59 and self.pos_tracker:
            if self.pos_tracker['side'] == 'long':
                pnl = (self.data.close[0] - self.pos_tracker['entry_price']) / self.pos_tracker['entry_price']
            else:
                pnl = (self.pos_tracker['entry_price'] - self.data.close[0]) / self.pos_tracker['entry_price']
            self.trades_log.append({**self.pos_tracker, 'exit_time': dt,
                'exit_price': self.data.close[0], 'exit_reason': 'EOD', 'pnl_pct': pnl})
            self.pos_tracker = None
            return

        if len(self.hma) < 2:
            return

        hma_curr, hma_prev = self.hma[0], self.hma[-1]
        atr_pct = self.atr[0] / self.data.close[0]

        if pd.isna(hma_curr) or pd.isna(hma_prev) or pd.isna(self.atr[0]):
            return
        if atr_pct < 0.007:
            return

        sig_long = hma_prev > hma_curr
        sig_short = hma_prev < hma_curr

        # SL check
        if self.pos_tracker:
            if self.pos_tracker['side'] == 'long' and self.data.low[0] <= self.pos_tracker['sl']:
                self.trades_log.append({**self.pos_tracker, 'exit_time': dt,
                    'exit_price': self.pos_tracker['sl'], 'exit_reason': 'SL', 'pnl_pct': -0.005})
                self.pos_tracker = None
                return
            elif self.pos_tracker['side'] == 'short' and self.data.high[0] >= self.pos_tracker['sl']:
                self.trades_log.append({**self.pos_tracker, 'exit_time': dt,
                    'exit_price': self.pos_tracker['sl'], 'exit_reason': 'SL', 'pnl_pct': -0.005})
                self.pos_tracker = None
                return

        # Signal logic
        if sig_long:
            if self.pos_tracker and self.pos_tracker['side'] == 'short':
                pnl = (self.pos_tracker['entry_price'] - self.data.close[0]) / self.pos_tracker['entry_price']
                self.trades_log.append({**self.pos_tracker, 'exit_time': dt,
                    'exit_price': self.data.close[0], 'exit_reason': 'signal', 'pnl_pct': pnl})
            if not self.pos_tracker or self.pos_tracker['side'] == 'short':
                self.pos_tracker = {'side': 'long', 'entry_time': dt,
                                'entry_price': self.data.close[0],
                                'sl': self.data.close[0] * 0.995}
        elif sig_short:
            if self.pos_tracker and self.pos_tracker['side'] == 'long':
                pnl = (self.data.close[0] - self.pos_tracker['entry_price']) / self.pos_tracker['entry_price']
                self.trades_log.append({**self.pos_tracker, 'exit_time': dt,
                    'exit_price': self.data.close[0], 'exit_reason': 'signal', 'pnl_pct': pnl})
            if not self.pos_tracker or self.pos_tracker['side'] == 'long':
                self.pos_tracker = {'side': 'short', 'entry_time': dt,
                                'entry_price': self.data.close[0],
                                'sl': self.data.close[0] * 1.005}

def main():
    print("=" * 80)
    print("FINAL EXACT MATCH TEST: LCID 2025-05-07")
    print("=" * 80)

    # Load
    df = pd.read_csv('config/data/m/alpaca/LCID.csv')
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df = df.set_index('timestamp')
    df = df[(df.index >= '2025-05-07') & (df.index < '2025-05-08')]

    print(f"Bars: {len(df)}, Range: {df.index[0]} to {df.index[-1]}")

    # Monte Carlo
    print("\n" + "=" * 80)
    print("MONTE CARLO")
    print("=" * 80)
    mc = monte_carlo_sim(df)
    print(f"Trades: {len(mc)}\n")
    for i, t in mc.iterrows():
        print(f"{i+1}. {t['side'].upper():5s} {t['entry_time'].strftime('%H:%M')} @ ${t['entry_price']:.2f} → "
              f"{t['exit_time'].strftime('%H:%M')} @ ${t['exit_price']:.2f} PnL:{t['pnl_pct']*100:+.2f}% ({t['exit_reason']})")

    # Backtrader
    print("\n" + "=" * 80)
    print("BACKTRADER (Manual Tracking)")
    print("=" * 80)
    cerebro = bt.Cerebro()
    data = bt.feeds.PandasData(dataname=df, datetime=None, open='open', high='high',
                               low='low', close='close', volume='volume', openinterest=-1)
    cerebro.adddata(data)
    cerebro.addstrategy(ManualTrackStrategy)
    strats = cerebro.run()
    bt_trades = strats[0].trades_log

    print(f"Trades: {len(bt_trades)}\n")
    for i, t in enumerate(bt_trades):
        print(f"{i+1}. {t['side'].upper():5s} {t['entry_time'].strftime('%H:%M')} @ ${t['entry_price']:.2f} → "
              f"{t['exit_time'].strftime('%H:%M')} @ ${t['exit_price']:.2f} PnL:{t['pnl_pct']*100:+.2f}% ({t['exit_reason']})")

    # Compare
    print("\n" + "=" * 80)
    print("RESULT")
    print("=" * 80)
    print(f"Monte Carlo: {len(mc)} trades")
    print(f"Backtrader:  {len(bt_trades)} trades")

    if len(mc) == len(bt_trades):
        match = True
        for i in range(len(mc)):
            m, b = mc.iloc[i], bt_trades[i]

            # Convert timestamps for comparison (normalize to tz-naive)
            mc_entry = pd.Timestamp(m['entry_time']).tz_localize(None)
            bt_entry = pd.Timestamp(b['entry_time']).tz_localize(None) if hasattr(b['entry_time'], 'tz') else pd.Timestamp(b['entry_time'])
            mc_exit = pd.Timestamp(m['exit_time']).tz_localize(None)
            bt_exit = pd.Timestamp(b['exit_time']).tz_localize(None) if hasattr(b['exit_time'], 'tz') else pd.Timestamp(b['exit_time'])

            if (m['side'] != b['side'] or mc_entry != bt_entry or
                abs(m['entry_price'] - b['entry_price']) > 0.01 or
                mc_exit != bt_exit or m['exit_reason'] != b['exit_reason']):
                match = False
                print(f"\n❌ Trade {i+1} mismatch!")
                print(f"  MC: {m['side']} {mc_entry} @ ${m['entry_price']:.2f} → {mc_exit} @ ${m['exit_price']:.2f} {m['exit_reason']}")
                print(f"  BT: {b['side']} {bt_entry} @ ${b['entry_price']:.2f} → {bt_exit} @ ${b['exit_price']:.2f} {b['exit_reason']}")
                break
        if match:
            print("\n✅ ✅ ✅ PERFECT MATCH! ✅ ✅ ✅")
    else:
        print("\n❌ Count mismatch")

if __name__ == '__main__':
    main()
