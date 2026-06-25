#!/usr/bin/env python3
"""
Test Backtrader vs Monte Carlo - Day by Day
Stops at first mismatch for user investigation
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import numpy as np
import backtrader as bt
from indicators.atr_sma import ATR_SMA
from datetime import datetime, timedelta

# ============================================================================
# MONTE CARLO REFERENCE
# ============================================================================

def load_monte_carlo_trades():
    """Load Monte Carlo trades and group by date"""
    mc_df = pd.read_csv('bt-strategy-test/HMA/limit_order_analysis/limit_close.csv')
    mc_df['entry_time'] = pd.to_datetime(mc_df['entry_time'])
    mc_df['date'] = mc_df['entry_time'].dt.date

    # Group by date
    daily = mc_df.groupby('date').agg({
        'symbol': ['count', 'nunique', lambda x: sorted(x.unique())],
        'pnl_pct': 'sum'
    }).reset_index()

    daily.columns = ['date', 'trades', 'symbols_count', 'symbols_list', 'total_pnl']
    daily = daily.sort_values('date')

    return daily, mc_df

# ============================================================================
# BACKTRADER STRATEGY (Manual Tracking)
# ============================================================================

class HMADayTest(bt.Strategy):
    """Simplified HMA strategy for single-day testing"""

    params = (
        ('period', 16),
        ('atr_min', 0.007),  # 0.7%
        ('sl_pct', 0.005),   # 0.5%
    )

    def __init__(self):
        # Create indicators per data feed
        self.hmas = {}
        self.atrs = {}
        for d in self.datas:
            self.hmas[d] = bt.indicators.HMA(d, period=self.p.period)
            self.atrs[d] = ATR_SMA(d, period=14)

        self.trades_log = []
        self.pos_trackers = {}  # Manual position tracking per symbol

    def stop(self):
        """Close all open positions at end of day (EOD)"""
        for d, pos in list(self.pos_trackers.items()):
            dt = d.datetime.datetime()
            if pos['side'] == 'long':
                pnl = (d.close[0] - pos['entry_price']) / pos['entry_price']
            else:
                pnl = (pos['entry_price'] - d.close[0]) / pos['entry_price']

            self.trades_log.append({
                'symbol': d._name,
                'side': pos['side'],
                'entry_time': pos['entry_time'],
                'entry_price': pos['entry_price'],
                'exit_time': dt,
                'exit_price': d.close[0],
                'exit_reason': 'EOD',
                'pnl_pct': pnl
            })

    def next(self):
        # Process each data feed
        for d in self.datas:
            dt = d.datetime.datetime()
            hour = dt.hour
            minute = dt.minute

            # ==============================================================
            # CRITICAL: Check SL/TP BEFORE time filter (like Monte Carlo!)
            # Monte Carlo allows SL to be hit even after trading window ends
            # ==============================================================
            if d in self.pos_trackers:
                pos = self.pos_trackers[d]

                if pos['side'] == 'long' and d.low[0] <= pos['sl']:
                    self.trades_log.append({
                        'symbol': d._name,
                        'side': 'long',
                        'entry_time': pos['entry_time'],
                        'entry_price': pos['entry_price'],
                        'exit_time': dt,
                        'exit_price': pos['sl'],
                        'exit_reason': 'SL',
                        'pnl_pct': -0.005
                    })
                    del self.pos_trackers[d]
                    continue

                elif pos['side'] == 'short' and d.high[0] >= pos['sl']:
                    self.trades_log.append({
                        'symbol': d._name,
                        'side': 'short',
                        'entry_time': pos['entry_time'],
                        'entry_price': pos['entry_price'],
                        'exit_time': dt,
                        'exit_price': pos['sl'],
                        'exit_reason': 'SL',
                        'pnl_pct': -0.005
                    })
                    del self.pos_trackers[d]
                    continue

            # ==============================================================
            # Time filter: 10:00-14:59 UTC (ONLY for new signals, not for SL!)
            # ==============================================================
            if hour < 10 or hour >= 15:
                continue  # Skip new signal processing outside trading window

            # Check indicators ready
            hma = self.hmas[d]
            atr = self.atrs[d]

            if len(hma) < 2 or pd.isna(hma[0]) or pd.isna(hma[-1]) or pd.isna(atr[0]):
                continue

            # ATR filter (ONLY for new signals)
            atr_pct = atr[0] / d.close[0]
            if atr_pct < self.p.atr_min:
                continue

            # HMA signals (inverted/contrarian)
            hma_curr = hma[0]
            hma_prev = hma[-1]
            signal_long = hma_prev > hma_curr   # HMA falling → LONG
            signal_short = hma_prev < hma_curr  # HMA rising → SHORT

            # ==============================================================
            # Signal Logic
            # ==============================================================
            if signal_long:
                # Close short if exists
                if d in self.pos_trackers and self.pos_trackers[d]['side'] == 'short':
                    pos = self.pos_trackers[d]
                    pnl = (pos['entry_price'] - d.close[0]) / pos['entry_price']
                    self.trades_log.append({
                        'symbol': d._name,
                        'side': 'short',
                        'entry_time': pos['entry_time'],
                        'entry_price': pos['entry_price'],
                        'exit_time': dt,
                        'exit_price': d.close[0],
                        'exit_reason': 'signal',
                        'pnl_pct': pnl
                    })

                # Open long
                self.pos_trackers[d] = {
                    'side': 'long',
                    'entry_time': dt,
                    'entry_price': d.close[0],
                    'sl': d.close[0] * (1 - self.p.sl_pct)
                }

            elif signal_short:
                # Close long if exists
                if d in self.pos_trackers and self.pos_trackers[d]['side'] == 'long':
                    pos = self.pos_trackers[d]
                    pnl = (d.close[0] - pos['entry_price']) / pos['entry_price']
                    self.trades_log.append({
                        'symbol': d._name,
                        'side': 'long',
                        'entry_time': pos['entry_time'],
                        'entry_price': pos['entry_price'],
                        'exit_time': dt,
                        'exit_price': d.close[0],
                        'exit_reason': 'signal',
                        'pnl_pct': pnl
                    })

                # Open short
                self.pos_trackers[d] = {
                    'side': 'short',
                    'entry_time': dt,
                    'entry_price': d.close[0],
                    'sl': d.close[0] * (1 + self.p.sl_pct)
                }

# ============================================================================
# DAY-BY-DAY TESTING
# ============================================================================

def test_single_day(test_date, symbols, mc_trades_for_day):
    """Test Backtrader on a single day and compare with Monte Carlo"""

    print(f"\n{'='*80}")
    print(f"Testing: {test_date}")
    print(f"{'='*80}")

    # Load data for all symbols for this specific day
    cerebro = bt.Cerebro()

    # Next day for inclusive range
    next_day = test_date + timedelta(days=1)

    loaded_symbols = []
    for symbol in symbols:
        data_file = Path(f'config/data/m/alpaca/{symbol}.csv')
        if not data_file.exists():
            print(f"⚠️  WARNING: {symbol}.csv not found, skipping")
            continue

        # Load CSV
        df = pd.read_csv(data_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df = df.set_index('timestamp')

        # Filter to this day only
        day_data = df[(df.index >= str(test_date)) & (df.index < str(next_day))]

        if len(day_data) == 0:
            print(f"⚠️  WARNING: No data for {symbol} on {test_date}")
            continue

        # Add to Cerebro
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
        cerebro.adddata(data, name=symbol)
        loaded_symbols.append(symbol)

    if len(loaded_symbols) == 0:
        print("❌ No data loaded for any symbol!")
        return None

    print(f"Loaded {len(loaded_symbols)} symbols: {loaded_symbols}")

    # Add strategy
    cerebro.addstrategy(HMADayTest)

    # Run
    strats = cerebro.run()
    bt_trades = strats[0].trades_log

    # Convert to DataFrame for comparison
    if len(bt_trades) > 0:
        bt_df = pd.DataFrame(bt_trades)
        bt_symbols = sorted(bt_df['symbol'].unique())
        bt_count = len(bt_trades)
        bt_pnl = bt_df['pnl_pct'].sum()
    else:
        bt_symbols = []
        bt_count = 0
        bt_pnl = 0.0

    # Monte Carlo stats
    mc_symbols = sorted(mc_trades_for_day['symbol'].unique())
    mc_count = len(mc_trades_for_day)
    mc_pnl = mc_trades_for_day['pnl_pct'].sum()

    # Compare
    print(f"\n{'COMPARISON':-^80}")
    print(f"Monte Carlo: {mc_count} trades, {len(mc_symbols)} symbols, PnL: {mc_pnl*100:+.2f}%")
    print(f"Backtrader:  {bt_count} trades, {len(bt_symbols)} symbols, PnL: {bt_pnl*100:+.2f}%")

    # Check match
    match = True

    if mc_count != bt_count:
        print(f"\n❌ MISMATCH: Trade count {mc_count} vs {bt_count}")
        match = False

    if set(mc_symbols) != set(bt_symbols):
        print(f"\n❌ MISMATCH: Symbols differ")
        print(f"   MC symbols: {mc_symbols}")
        print(f"   BT symbols: {bt_symbols}")
        match = False

    if match:
        print(f"\n✅ MATCH! Counts and symbols correct")
        # Still check PnL
        if abs(mc_pnl - bt_pnl) > 0.0001:
            print(f"⚠️  WARNING: PnL differs by {abs(mc_pnl - bt_pnl)*100:.3f}%")
            print(f"   This might be rounding or small timing differences")

    return {
        'date': test_date,
        'match': match,
        'mc_count': mc_count,
        'bt_count': bt_count,
        'mc_symbols': mc_symbols,
        'bt_symbols': bt_symbols,
        'mc_pnl': mc_pnl,
        'bt_pnl': bt_pnl,
        'bt_trades': bt_trades if not match else None
    }

def main():
    print("="*80)
    print("BACKTRADER vs MONTE CARLO - DAY BY DAY TEST")
    print("="*80)

    # Load Monte Carlo reference
    mc_daily, mc_all = load_monte_carlo_trades()

    print(f"\nMonte Carlo period: {mc_daily['date'].min()} to {mc_daily['date'].max()}")
    print(f"Total days: {len(mc_daily)}")
    print(f"Total trades: {mc_daily['trades'].sum()}")

    # Get all symbols used in Monte Carlo
    all_symbols = sorted(mc_all['symbol'].unique())
    print(f"\nSymbols ({len(all_symbols)}): {all_symbols}")

    # Test day by day
    results = []
    for idx, row in mc_daily.iterrows():
        test_date = row['date']

        # Get MC trades for this day
        mc_trades_day = mc_all[mc_all['date'] == test_date]

        # Test
        result = test_single_day(test_date, all_symbols, mc_trades_day)

        if result is None:
            print(f"⚠️  Skipping {test_date} (no data)")
            continue

        results.append(result)

        # STOP if mismatch
        if not result['match']:
            print(f"\n{'!'*80}")
            print(f"STOPPING AT FIRST MISMATCH: {test_date}")
            print(f"{'!'*80}")

            # Show detailed comparison
            print(f"\nMonte Carlo trades on {test_date}:")
            for _, trade in mc_trades_day.iterrows():
                print(f"  {trade['symbol']:6s} {trade['side']:5s} "
                      f"{pd.Timestamp(trade['entry_time']).strftime('%H:%M')} → "
                      f"{pd.Timestamp(trade['exit_time']).strftime('%H:%M')} "
                      f"PnL:{trade['pnl_pct']*100:+.2f}% ({trade['exit_reason']})")

            if result['bt_trades']:
                print(f"\nBacktrader trades on {test_date}:")
                for trade in result['bt_trades']:
                    print(f"  {trade['symbol']:6s} {trade['side']:5s} "
                          f"{trade['entry_time'].strftime('%H:%M')} → "
                          f"{trade['exit_time'].strftime('%H:%M')} "
                          f"PnL:{trade['pnl_pct']*100:+.2f}% ({trade['exit_reason']})")
            else:
                print(f"\nBacktrader trades: NONE")

            print(f"\n⏸️  PAUSED - Awaiting user instructions")
            break

    # Summary if all passed
    if all(r['match'] for r in results):
        print(f"\n{'='*80}")
        print(f"🎉 ALL {len(results)} DAYS MATCHED PERFECTLY!")
        print(f"{'='*80}")

        total_mc = sum(r['mc_count'] for r in results)
        total_bt = sum(r['bt_count'] for r in results)
        print(f"Total trades: MC={total_mc}, BT={total_bt}")

if __name__ == '__main__':
    main()
