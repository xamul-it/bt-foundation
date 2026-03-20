#!/usr/bin/env python3
"""
Test EXACT match: Monte Carlo vs Backtrader
Symbol: LCID
Date: 2025-05-07
Monte Carlo trades: 5 (verified from limit_close.csv)

GOAL: Get IDENTICAL results
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import numpy as np
import backtrader as bt
from indicators.atr_sma import ATR_SMA

# ============================================================================
# MONTE CARLO LOGIC (EXACT COPY)
# ============================================================================

def calculate_hma_mc(close, period=16):
    """Monte Carlo HMA - EXACT implementation"""
    half_period = int(period / 2)
    sqrt_period = int(np.sqrt(period))

    # WMA calculation
    def wma(series, n):
        weights = np.arange(1, n + 1)
        return series.rolling(n).apply(
            lambda x: np.sum(x * weights) / weights.sum() if len(x) == n else np.nan,
            raw=True
        )

    wma_half = wma(close, half_period)
    wma_full = wma(close, period)
    raw_hma = 2 * wma_half - wma_full
    hma = wma(raw_hma, sqrt_period)

    return hma

def calculate_atr_mc(df, period=14):
    """Monte Carlo ATR - EXACT implementation"""
    high = df['high']
    low = df['low']
    close = df['close']

    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()

    return (atr / close) * 100  # Returns percentage (0.787 for 0.787%)

def monte_carlo_simulation(df, period=16, atr_min=0.7, sl_pct=0.005):
    """EXACT Monte Carlo logic"""
    df = df.copy()
    df['hma'] = calculate_hma_mc(df['close'], period)
    df['atr_pct'] = calculate_atr_mc(df, 14)

    # Time filter: hour 10-15 (UTC hours, as per Monte Carlo)
    df['hour'] = df.index.hour
    df = df[(df['hour'] >= 10) & (df['hour'] < 15)]

    trades = []
    position = None

    for i in range(1, len(df)):
        bar = df.iloc[i]
        prev_bar = df.iloc[i-1]

        # Skip if no HMA
        if pd.isna(bar['hma']) or pd.isna(prev_bar['hma']):
            continue

        # ATR filter
        if pd.isna(bar['atr_pct']) or bar['atr_pct'] < atr_min:
            continue

        # HMA signal (inverted mode)
        hma_falling = prev_bar['hma'] > bar['hma']
        hma_rising = prev_bar['hma'] < bar['hma']

        signal_long = hma_falling
        signal_short = hma_rising

        # Check SL if in position
        if position:
            hit_sl = False
            if position['side'] == 'long':
                if bar['low'] <= position['sl_price']:
                    trades.append({
                        'side': 'long',
                        'entry_time': position['entry_time'],
                        'entry_price': position['entry_price'],
                        'exit_time': bar.name,
                        'exit_price': position['sl_price'],
                        'exit_reason': 'SL',
                        'pnl_pct': -0.005
                    })
                    position = None
                    hit_sl = True
            else:  # short
                if bar['high'] >= position['sl_price']:
                    trades.append({
                        'side': 'short',
                        'entry_time': position['entry_time'],
                        'entry_price': position['entry_price'],
                        'exit_time': bar.name,
                        'exit_price': position['sl_price'],
                        'exit_reason': 'SL',
                        'pnl_pct': -0.005
                    })
                    position = None
                    hit_sl = True

            if hit_sl:
                continue

        # New signal (close and reverse or initial entry)
        if signal_long and (position is None or position['side'] == 'short'):
            # Close short if exists
            if position and position['side'] == 'short':
                pnl_pct = (position['entry_price'] - bar['close']) / position['entry_price']
                trades.append({
                    'side': 'short',
                    'entry_time': position['entry_time'],
                    'entry_price': position['entry_price'],
                    'exit_time': bar.name,
                    'exit_price': bar['close'],
                    'exit_reason': 'signal',
                    'pnl_pct': pnl_pct
                })

            # Open long
            position = {
                'side': 'long',
                'entry_time': bar.name,
                'entry_price': bar['close'],
                'sl_price': bar['close'] * (1 - sl_pct)
            }

        elif signal_short and (position is None or position['side'] == 'long'):
            # Close long if exists
            if position and position['side'] == 'long':
                pnl_pct = (bar['close'] - position['entry_price']) / position['entry_price']
                trades.append({
                    'side': 'long',
                    'entry_time': position['entry_time'],
                    'entry_price': position['entry_price'],
                    'exit_time': bar.name,
                    'exit_price': bar['close'],
                    'exit_reason': 'signal',
                    'pnl_pct': pnl_pct
                })

            # Open short
            position = {
                'side': 'short',
                'entry_time': bar.name,
                'entry_price': bar['close'],
                'sl_price': bar['close'] * (1 + sl_pct)
            }

    # Close EOD if position open
    if position:
        last_bar = df.iloc[-1]
        if position['side'] == 'long':
            pnl_pct = (last_bar['close'] - position['entry_price']) / position['entry_price']
        else:
            pnl_pct = (position['entry_price'] - last_bar['close']) / position['entry_price']

        trades.append({
            'side': position['side'],
            'entry_time': position['entry_time'],
            'entry_price': position['entry_price'],
            'exit_time': last_bar.name,
            'exit_price': last_bar['close'],
            'exit_reason': 'EOD',
            'pnl_pct': pnl_pct
        })

    return pd.DataFrame(trades)

# ============================================================================
# BACKTRADER TEST
# ============================================================================

class TestStrategy(bt.Strategy):
    params = (
        ('period', 16),
        ('atr_min', 0.007),  # 0.7%
        ('sl_pct', 0.005),
    )

    def __init__(self):
        self.hma = bt.indicators.HMA(self.data, period=self.p.period)
        self.atr = ATR_SMA(self.data, period=14)
        self.trades_log = []
        self.position_tracker = None

    def next(self):
        dt = self.data.datetime.datetime()

        # Time filter: 10-15 UTC (matching Monte Carlo)
        hour = dt.hour
        if hour < 10 or hour >= 15:
            return

        # Check indicators ready
        if len(self.hma) < 2:
            return

        hma_curr = self.hma[0]
        hma_prev = self.hma[-1]
        atr_val = self.atr[0]
        close = self.data.close[0]
        atr_pct = atr_val / close

        if pd.isna(hma_curr) or pd.isna(hma_prev) or pd.isna(atr_val):
            return

        # ATR filter
        if atr_pct < self.p.atr_min:
            return

        # HMA signals (inverted)
        signal_long = hma_prev > hma_curr
        signal_short = hma_prev < hma_curr

        pos = self.getposition()

        # Check SL if in position
        if self.position_tracker:
            if self.position_tracker['side'] == 'long':
                if self.data.low[0] <= self.position_tracker['sl_price']:
                    self.close()
                    self.trades_log.append({
                        'side': 'long',
                        'entry_time': self.position_tracker['entry_time'],
                        'entry_price': self.position_tracker['entry_price'],
                        'exit_time': dt,
                        'exit_price': self.position_tracker['sl_price'],
                        'exit_reason': 'SL',
                        'pnl_pct': -0.005
                    })
                    self.position_tracker = None
                    return
            else:  # short
                if self.data.high[0] >= self.position_tracker['sl_price']:
                    self.close()
                    self.trades_log.append({
                        'side': 'short',
                        'entry_time': self.position_tracker['entry_time'],
                        'entry_price': self.position_tracker['entry_price'],
                        'exit_time': dt,
                        'exit_price': self.position_tracker['sl_price'],
                        'exit_reason': 'SL',
                        'pnl_pct': -0.005
                    })
                    self.position_tracker = None
                    return

        # Signal logic
        if signal_long:
            if pos.size < 0:  # Close short
                exit_price = close
                pnl_pct = (self.position_tracker['entry_price'] - exit_price) / self.position_tracker['entry_price']
                self.close(exectype=bt.Order.Close)  # Close at current bar close
                self.trades_log.append({
                    'side': 'short',
                    'entry_time': self.position_tracker['entry_time'],
                    'entry_price': self.position_tracker['entry_price'],
                    'exit_time': dt,
                    'exit_price': exit_price,
                    'exit_reason': 'signal',
                    'pnl_pct': pnl_pct
                })

            if pos.size == 0:  # Open long
                # Execute at CLOSE of current bar (like Monte Carlo limit at close)
                self.buy(exectype=bt.Order.Close)
                self.position_tracker = {
                    'side': 'long',
                    'entry_time': dt,
                    'entry_price': close,  # Will fill at close
                    'sl_price': close * (1 - self.p.sl_pct)
                }

        elif signal_short:
            if pos.size > 0:  # Close long
                exit_price = close
                pnl_pct = (exit_price - self.position_tracker['entry_price']) / self.position_tracker['entry_price']
                self.close(exectype=bt.Order.Close)  # Close at current bar close
                self.trades_log.append({
                    'side': 'long',
                    'entry_time': self.position_tracker['entry_time'],
                    'entry_price': self.position_tracker['entry_price'],
                    'exit_time': dt,
                    'exit_price': exit_price,
                    'exit_reason': 'signal',
                    'pnl_pct': pnl_pct
                })

            if pos.size == 0:  # Open short
                # Execute at CLOSE of current bar (like Monte Carlo limit at close)
                self.sell(exectype=bt.Order.Close)
                self.position_tracker = {
                    'side': 'short',
                    'entry_time': dt,
                    'entry_price': close,  # Will fill at close
                    'sl_price': close * (1 + self.p.sl_pct)
                }

# ============================================================================
# MAIN TEST
# ============================================================================

def main():
    print("=" * 80)
    print("EXACT MATCH TEST: LCID 2025-05-07")
    print("=" * 80)

    # Load data
    data_file = Path('config/data/m/alpaca/LCID.csv')
    df = pd.read_csv(data_file)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df = df.set_index('timestamp')

    # Filter to 2025-05-07
    day_data = df[(df.index >= '2025-05-07') & (df.index < '2025-05-08')].copy()

    print(f"\nData loaded: {len(day_data)} bars")
    print(f"Time range: {day_data.index[0]} to {day_data.index[-1]}")

    # ========================================================================
    # TEST 1: Monte Carlo
    # ========================================================================
    print("\n" + "=" * 80)
    print("MONTE CARLO SIMULATION")
    print("=" * 80)

    mc_trades = monte_carlo_simulation(day_data, period=16, atr_min=0.7, sl_pct=0.005)

    print(f"\nTrades: {len(mc_trades)}")
    print("\nTrade details:")
    for i, trade in mc_trades.iterrows():
        print(f"{i+1}. {trade['side'].upper():5s} "
              f"Entry: {trade['entry_time'].strftime('%H:%M')} @ ${trade['entry_price']:.2f}, "
              f"Exit: {trade['exit_time'].strftime('%H:%M')} @ ${trade['exit_price']:.2f}, "
              f"PnL: {trade['pnl_pct']*100:+.2f}%, Reason: {trade['exit_reason']}")

    # ========================================================================
    # TEST 2: Backtrader
    # ========================================================================
    print("\n" + "=" * 80)
    print("BACKTRADER TEST")
    print("=" * 80)

    cerebro = bt.Cerebro()

    data = bt.feeds.PandasData(
        dataname=day_data,
        datetime=None,
        open='open',
        high='high',
        low='low',
        close='close',
        volume='volume',
        openinterest=-1
    )
    cerebro.adddata(data, name='LCID')
    cerebro.addstrategy(TestStrategy)
    cerebro.broker.set_cash(100000)
    cerebro.broker.set_coc(True)  # Cheat-on-close: execute at close of current bar

    strats = cerebro.run()
    strat = strats[0]

    print(f"\nTrades: {len(strat.trades_log)}")
    print("\nTrade details:")
    for i, trade in enumerate(strat.trades_log):
        print(f"{i+1}. {trade['side'].upper():5s} "
              f"Entry: {trade['entry_time'].strftime('%H:%M')} @ ${trade['entry_price']:.2f}, "
              f"Exit: {trade['exit_time'].strftime('%H:%M')} @ ${trade['exit_price']:.2f}, "
              f"PnL: {trade['pnl_pct']*100:+.2f}%, Reason: {trade['exit_reason']}")

    # ========================================================================
    # COMPARISON
    # ========================================================================
    print("\n" + "=" * 80)
    print("COMPARISON")
    print("=" * 80)

    print(f"\nMonte Carlo trades: {len(mc_trades)}")
    print(f"Backtrader trades:  {len(strat.trades_log)}")

    if len(mc_trades) == len(strat.trades_log):
        print("\n✅ TRADE COUNT MATCHES!")

        # Compare details
        all_match = True
        for i in range(len(mc_trades)):
            mc = mc_trades.iloc[i]
            bt_trade = strat.trades_log[i]

            side_match = mc['side'] == bt_trade['side']
            entry_time_match = mc['entry_time'] == bt_trade['entry_time']
            entry_price_match = abs(mc['entry_price'] - bt_trade['entry_price']) < 0.01
            exit_reason_match = mc['exit_reason'] == bt_trade['exit_reason']
            pnl_match = abs(mc['pnl_pct'] - bt_trade['pnl_pct']) < 0.0001

            if not all([side_match, entry_time_match, entry_price_match, exit_reason_match, pnl_match]):
                all_match = False
                print(f"\n❌ Trade {i+1} MISMATCH:")
                print(f"   MC: {mc['side']} @ {mc['entry_time'].strftime('%H:%M')} ${mc['entry_price']:.2f} → {mc['exit_reason']} {mc['pnl_pct']*100:+.2f}%")
                print(f"   BT: {bt_trade['side']} @ {bt_trade['entry_time'].strftime('%H:%M')} ${bt_trade['entry_price']:.2f} → {bt_trade['exit_reason']} {bt_trade['pnl_pct']*100:+.2f}%")

        if all_match:
            print("\n✅ ALL TRADE DETAILS MATCH!")
            print("\n🎉 SUCCESS: Backtrader EXACTLY matches Monte Carlo!")
        else:
            print("\n⚠️ Trade count matches but details differ")
    else:
        print(f"\n❌ TRADE COUNT MISMATCH: {len(mc_trades)} vs {len(strat.trades_log)}")
        print("\nDEBUG: Finding first divergence point...")

if __name__ == '__main__':
    main()
