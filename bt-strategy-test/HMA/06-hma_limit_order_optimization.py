#!/usr/bin/env python3
"""
HMA Strategy - Limit Order Optimization

Tests different limit order strategies to eliminate/reduce slippage:
1. Market orders (baseline, 0.03% slippage)
2. Limit at close price (signal bar close)
3. Limit at better prices (close - X bps for long, close + X bps for short)
4. Different exit strategies (limit TP, exit at signal reversal)

Goal: Find optimal entry/exit price levels that maximize fill rate while minimizing slippage
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import logging
from datetime import datetime, time
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
log = logging.getLogger(__name__)

def calculate_hma(prices: pd.Series, period: int) -> pd.Series:
    """Calculate Hull Moving Average"""
    half_period = int(period / 2)
    sqrt_period = int(np.sqrt(period))

    wma_half = prices.rolling(window=half_period).apply(
        lambda x: np.sum(x * np.arange(1, len(x) + 1)) / np.sum(np.arange(1, len(x) + 1)),
        raw=True
    )
    wma_full = prices.rolling(window=period).apply(
        lambda x: np.sum(x * np.arange(1, len(x) + 1)) / np.sum(np.arange(1, len(x) + 1)),
        raw=True
    )

    raw_hma = 2 * wma_half - wma_full
    hma = raw_hma.rolling(window=sqrt_period).apply(
        lambda x: np.sum(x * np.arange(1, len(x) + 1)) / np.sum(np.arange(1, len(x) + 1)),
        raw=True
    )

    return hma

def calculate_atr_pct(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate ATR as percentage of close price"""
    high = df['high']
    low = df['low']
    close = df['close']

    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()

    return (atr / close) * 100

def apply_filters(bar: pd.Series, filters: dict) -> bool:
    """Check if bar passes all filters"""
    if 'atr_pct_min' in filters:
        if pd.isna(bar['atr_pct']) or bar['atr_pct'] < filters['atr_pct_min']:
            return False

    if 'hour_min' in filters:
        bar_hour = bar.name.hour
        if bar_hour < filters['hour_min']:
            return False

    if 'hour_max' in filters:
        bar_hour = bar.name.hour
        if bar_hour >= filters['hour_max']:
            return False

    return True

def try_fill_limit_order(
    order: Dict,
    current_bar: pd.Series,
    next_bar: Optional[pd.Series] = None
) -> Tuple[bool, Optional[float]]:
    """
    Try to fill a limit order.

    Returns (filled, fill_price)

    Order types:
    - 'limit_close': Fill at signal bar close (instant fill)
    - 'limit_next_open': Fill at next bar open if price allows
    - 'limit_next_range': Fill if price touches limit during next bar
    """
    order_type = order['order_type']
    side = order['side']
    limit_price = order['limit_price']

    if order_type == 'limit_close':
        # Fill at signal bar close (instant, no slippage)
        return True, current_bar['close']

    elif order_type == 'limit_next_open':
        if next_bar is None:
            return False, None

        # Try to fill at next bar open
        next_open = next_bar['open']

        if side == 'long':
            # Long limit: fill if market drops to/below limit
            if next_open <= limit_price:
                return True, limit_price
        else:  # short
            # Short limit: fill if market rises to/above limit
            if next_open >= limit_price:
                return True, limit_price

        return False, None

    elif order_type == 'limit_next_range':
        if next_bar is None:
            return False, None

        # Check if limit price is within next bar's range
        if side == 'long':
            # Long limit: fill if low <= limit_price
            if next_bar['low'] <= limit_price:
                return True, limit_price
        else:  # short
            # Short limit: fill if high >= limit_price
            if next_bar['high'] >= limit_price:
                return True, limit_price

        return False, None

    else:
        raise ValueError(f"Unknown order type: {order_type}")

def simulate_hma_day_limit_orders(
    day_df: pd.DataFrame,
    period: int,
    inverted: bool,
    filters: dict,
    order_config: dict,
    sl_pct: float = 0.005
) -> List[Dict]:
    """
    Simulate HMA strategy with limit orders.

    order_config keys:
    - entry_type: 'market', 'limit_close', 'limit_next_open', 'limit_next_range'
    - entry_offset_bps: For limit orders, offset from signal bar close (positive = worse price)
    - exit_type: 'market', 'limit_close', 'limit_tp'
    - tp_pct: Take profit percentage (if exit_type='limit_tp')
    """
    trades = []

    # Calculate HMA and ATR
    day_df = day_df.copy()
    day_df['hma'] = calculate_hma(day_df['close'], period)
    day_df['atr_pct'] = calculate_atr_pct(day_df, period=14)
    day_df['hma_rising'] = day_df['hma'].diff() > 0

    position = None
    current_direction = 0
    pending_order = None

    for i in range(1, len(day_df)):
        current_bar = day_df.iloc[i]
        prev_bar = day_df.iloc[i-1]
        next_bar = day_df.iloc[i+1] if i+1 < len(day_df) else None

        # Try to fill pending order
        if pending_order is not None:
            filled, fill_price = try_fill_limit_order(pending_order, prev_bar, current_bar)
            if filled:
                # Open position
                position = {
                    'side': pending_order['side'],
                    'entry_time': current_bar.name,
                    'entry_price': fill_price,
                    'sl_price': fill_price * (1 - sl_pct) if pending_order['side'] == 'long'
                               else fill_price * (1 + sl_pct)
                }
                if 'tp_pct' in order_config:
                    position['tp_price'] = fill_price * (1 + order_config['tp_pct']) if pending_order['side'] == 'long' \
                                          else fill_price * (1 - order_config['tp_pct'])
                else:
                    position['tp_price'] = None

                current_direction = 1 if pending_order['side'] == 'long' else -1
                pending_order = None

        # Check SL/TP if in position
        if position is not None:
            side = position['side']

            # Check SL
            if side == 'long' and current_bar['low'] <= position['sl_price']:
                trades.append({
                    'symbol': day_df['symbol'].iloc[0] if 'symbol' in day_df.columns else 'UNKNOWN',
                    'side': side,
                    'entry_time': position['entry_time'],
                    'entry_price': position['entry_price'],
                    'exit_time': current_bar.name,
                    'exit_price': position['sl_price'],
                    'exit_reason': 'SL',
                    'pnl_pct': -sl_pct,
                    'bars_held': i - day_df.index.get_loc(position['entry_time'])
                })
                position = None
                current_direction = 0
                continue

            elif side == 'short' and current_bar['high'] >= position['sl_price']:
                trades.append({
                    'symbol': day_df['symbol'].iloc[0] if 'symbol' in day_df.columns else 'UNKNOWN',
                    'side': side,
                    'entry_time': position['entry_time'],
                    'entry_price': position['entry_price'],
                    'exit_time': current_bar.name,
                    'exit_price': position['sl_price'],
                    'exit_reason': 'SL',
                    'pnl_pct': -sl_pct,
                    'bars_held': i - day_df.index.get_loc(position['entry_time'])
                })
                position = None
                current_direction = 0
                continue

            # Check TP
            if position['tp_price'] is not None:
                if side == 'long' and current_bar['high'] >= position['tp_price']:
                    pnl_pct = (position['tp_price'] - position['entry_price']) / position['entry_price']
                    trades.append({
                        'symbol': day_df['symbol'].iloc[0] if 'symbol' in day_df.columns else 'UNKNOWN',
                        'side': side,
                        'entry_time': position['entry_time'],
                        'entry_price': position['entry_price'],
                        'exit_time': current_bar.name,
                        'exit_price': position['tp_price'],
                        'exit_reason': 'TP',
                        'pnl_pct': pnl_pct,
                        'bars_held': i - day_df.index.get_loc(position['entry_time'])
                    })
                    position = None
                    current_direction = 0
                    continue

                elif side == 'short' and current_bar['low'] <= position['tp_price']:
                    pnl_pct = (position['entry_price'] - position['tp_price']) / position['entry_price']
                    trades.append({
                        'symbol': day_df['symbol'].iloc[0] if 'symbol' in day_df.columns else 'UNKNOWN',
                        'side': side,
                        'entry_time': position['entry_time'],
                        'entry_price': position['entry_price'],
                        'exit_time': current_bar.name,
                        'exit_price': position['tp_price'],
                        'exit_reason': 'TP',
                        'pnl_pct': pnl_pct,
                        'bars_held': i - day_df.index.get_loc(position['entry_time'])
                    })
                    position = None
                    current_direction = 0
                    continue

        # Skip if missing data
        if pd.isna(current_bar['hma']) or pd.isna(prev_bar['hma']):
            continue

        # Apply filters
        if not apply_filters(current_bar, filters):
            continue

        # Detect HMA signals
        if inverted:
            signal_long = prev_bar['hma_rising'] and not current_bar['hma_rising']
            signal_short = not prev_bar['hma_rising'] and current_bar['hma_rising']
        else:
            signal_long = not prev_bar['hma_rising'] and current_bar['hma_rising']
            signal_short = prev_bar['hma_rising'] and not current_bar['hma_rising']

        # Process signals
        if signal_long and current_direction != 1:
            # Close short if exists
            if position is not None and position['side'] == 'short':
                exit_price = current_bar['close']
                pnl_pct = (position['entry_price'] - exit_price) / position['entry_price']
                trades.append({
                    'symbol': day_df['symbol'].iloc[0] if 'symbol' in day_df.columns else 'UNKNOWN',
                    'side': 'short',
                    'entry_time': position['entry_time'],
                    'entry_price': position['entry_price'],
                    'exit_time': current_bar.name,
                    'exit_price': exit_price,
                    'exit_reason': 'signal',
                    'pnl_pct': pnl_pct,
                    'bars_held': i - day_df.index.get_loc(position['entry_time'])
                })
                position = None

            # Create long order
            entry_type = order_config.get('entry_type', 'market')

            if entry_type == 'market':
                # Market order: instant fill at close + slippage
                position = {
                    'side': 'long',
                    'entry_time': current_bar.name,
                    'entry_price': current_bar['close'] * 1.0003,  # 0.03% slippage
                    'sl_price': current_bar['close'] * 1.0003 * (1 - sl_pct)
                }
                if 'tp_pct' in order_config:
                    position['tp_price'] = current_bar['close'] * 1.0003 * (1 + order_config['tp_pct'])
                else:
                    position['tp_price'] = None
                current_direction = 1

            elif entry_type == 'limit_close':
                # Limit at close: instant fill at close (no slippage)
                position = {
                    'side': 'long',
                    'entry_time': current_bar.name,
                    'entry_price': current_bar['close'],
                    'sl_price': current_bar['close'] * (1 - sl_pct)
                }
                if 'tp_pct' in order_config:
                    position['tp_price'] = current_bar['close'] * (1 + order_config['tp_pct'])
                else:
                    position['tp_price'] = None
                current_direction = 1

            else:  # limit_next_open, limit_next_range
                offset_bps = order_config.get('entry_offset_bps', 0)
                limit_price = current_bar['close'] * (1 - offset_bps / 10000)

                pending_order = {
                    'side': 'long',
                    'order_type': entry_type,
                    'limit_price': limit_price
                }

        elif signal_short and current_direction != -1:
            # Close long if exists
            if position is not None and position['side'] == 'long':
                exit_price = current_bar['close']
                pnl_pct = (exit_price - position['entry_price']) / position['entry_price']
                trades.append({
                    'symbol': day_df['symbol'].iloc[0] if 'symbol' in day_df.columns else 'UNKNOWN',
                    'side': 'long',
                    'entry_time': position['entry_time'],
                    'entry_price': position['entry_price'],
                    'exit_time': current_bar.name,
                    'exit_price': exit_price,
                    'exit_reason': 'signal',
                    'pnl_pct': pnl_pct,
                    'bars_held': i - day_df.index.get_loc(position['entry_time'])
                })
                position = None

            # Create short order
            entry_type = order_config.get('entry_type', 'market')

            if entry_type == 'market':
                # Market order: instant fill at close + slippage
                position = {
                    'side': 'short',
                    'entry_time': current_bar.name,
                    'entry_price': current_bar['close'] * 0.9997,  # 0.03% slippage
                    'sl_price': current_bar['close'] * 0.9997 * (1 + sl_pct)
                }
                if 'tp_pct' in order_config:
                    position['tp_price'] = current_bar['close'] * 0.9997 * (1 - order_config['tp_pct'])
                else:
                    position['tp_price'] = None
                current_direction = -1

            elif entry_type == 'limit_close':
                # Limit at close: instant fill at close (no slippage)
                position = {
                    'side': 'short',
                    'entry_time': current_bar.name,
                    'entry_price': current_bar['close'],
                    'sl_price': current_bar['close'] * (1 + sl_pct)
                }
                if 'tp_pct' in order_config:
                    position['tp_price'] = current_bar['close'] * (1 - order_config['tp_pct'])
                else:
                    position['tp_price'] = None
                current_direction = -1

            else:  # limit_next_open, limit_next_range
                offset_bps = order_config.get('entry_offset_bps', 0)
                limit_price = current_bar['close'] * (1 + offset_bps / 10000)

                pending_order = {
                    'side': 'short',
                    'order_type': entry_type,
                    'limit_price': limit_price
                }

    # Close any open position at EOD
    if position is not None:
        last_bar = day_df.iloc[-1]
        exit_price = last_bar['close']

        if position['side'] == 'long':
            pnl_pct = (exit_price - position['entry_price']) / position['entry_price']
        else:
            pnl_pct = (position['entry_price'] - exit_price) / position['entry_price']

        trades.append({
            'symbol': day_df['symbol'].iloc[0] if 'symbol' in day_df.columns else 'UNKNOWN',
            'side': position['side'],
            'entry_time': position['entry_time'],
            'entry_price': position['entry_price'],
            'exit_time': last_bar.name,
            'exit_price': exit_price,
            'exit_reason': 'EOD',
            'pnl_pct': pnl_pct,
            'bars_held': len(day_df) - 1 - day_df.index.get_loc(position['entry_time'])
        })

    return trades

def analyze_order_config(
    config_name: str,
    order_config: dict,
    data_dir: Path,
    period: int,
    inverted: bool,
    filters: dict,
    sl_pct: float,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """Analyze a specific order configuration"""
    all_trades = []

    csv_files = sorted(data_dir.glob('*.csv'))

    for csv_file in csv_files:
        symbol = csv_file.stem

        try:
            df = pd.read_csv(csv_file)

            # Parse datetime
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.set_index('datetime', inplace=True)
            elif 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
            else:
                log.warning(f"No datetime column in {csv_file}")
                continue

            # Ensure timezone aware
            if df.index.tz is None:
                df.index = df.index.tz_localize('UTC')

            # Filter by date range
            if start_date:
                start_ts = pd.to_datetime(start_date).tz_localize('UTC')
                df = df[df.index >= start_ts]

            if end_date:
                end_ts = pd.to_datetime(end_date).tz_localize('UTC')
                df = df[df.index < end_ts]

            if len(df) == 0:
                continue

            df['symbol'] = symbol

            # Normalize column names
            df.columns = df.columns.str.lower()

            # Group by date and simulate each day
            for date, day_df in df.groupby(df.index.date):
                if len(day_df) < 100:
                    continue

                trades = simulate_hma_day_limit_orders(
                    day_df, period, inverted, filters, order_config, sl_pct
                )
                all_trades.extend(trades)

        except Exception as e:
            log.error(f"Error processing {csv_file}: {e}")
            continue

    if not all_trades:
        log.warning(f"No trades for {config_name}")
        return pd.DataFrame()

    return pd.DataFrame(all_trades)

def compute_metrics(trades_df: pd.DataFrame, config_name: str) -> dict:
    """Compute performance metrics"""
    if len(trades_df) == 0:
        return {
            'config': config_name,
            'trades': 0,
            'win_rate': 0.0,
            'avg_win': 0.0,
            'avg_loss': 0.0,
            'gross_exp': 0.0,
            'status': '❌ NO TRADES'
        }

    total_trades = len(trades_df)
    winners = trades_df[trades_df['pnl_pct'] > 0]
    losers = trades_df[trades_df['pnl_pct'] < 0]

    win_rate = len(winners) / total_trades if total_trades > 0 else 0
    avg_win = winners['pnl_pct'].mean() * 100 if len(winners) > 0 else 0
    avg_loss = abs(losers['pnl_pct'].mean()) * 100 if len(losers) > 0 else 0

    gross_exp = trades_df['pnl_pct'].mean() * 100

    # Status
    if gross_exp >= 0.08:
        status = '✅ EXCELLENT'
    elif gross_exp >= 0.04:
        status = '✅ PASS'
    elif gross_exp >= 0.02:
        status = '⚠️  MARGINAL'
    elif gross_exp > 0:
        status = '⚠️  POSITIVE'
    else:
        status = '❌ FAIL'

    return {
        'config': config_name,
        'trades': total_trades,
        'win_rate': win_rate * 100,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'gross_exp': gross_exp,
        'status': status
    }

def main():
    # Configuration
    RUN_ID = datetime.now().strftime('%Y%m%d_%H%M%S')
    DATA_DIR = Path('config/data/m/alpaca')
    OUTPUT_DIR = Path('bt-strategy-test/HMA/limit_order_analysis')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Strategy parameters
    PERIOD = 16
    INVERTED = True
    SL_PCT = 0.005

    # Optimal filters from previous analysis
    FILTERS = {
        'atr_pct_min': 0.7,
        'hour_min': 10,
        'hour_max': 15
    }

    # Out-of-sample period
    START_DATE = '2025-05-06'
    END_DATE = '2025-07-03'

    log.info("=" * 80)
    log.info("HMA LIMIT ORDER OPTIMIZATION")
    log.info("=" * 80)
    log.info(f"Run ID: {RUN_ID}")
    log.info(f"Period: {START_DATE} to {END_DATE} (out-of-sample)")
    log.info(f"HMA: period={PERIOD}, inverted={INVERTED}")
    log.info(f"Filters: ATR >= {FILTERS['atr_pct_min']}%, hour {FILTERS['hour_min']}-{FILTERS['hour_max']}")
    log.info("")

    # Order configurations to test
    configs = [
        {
            'name': 'market',
            'config': {'entry_type': 'market'},
            'description': 'Market orders (baseline, 0.03% slippage)'
        },
        {
            'name': 'limit_close',
            'config': {'entry_type': 'limit_close'},
            'description': 'Limit at signal bar close (no slippage)'
        },
        {
            'name': 'limit_next_open',
            'config': {'entry_type': 'limit_next_open', 'entry_offset_bps': 0},
            'description': 'Limit at close, fill at next open if possible'
        },
        {
            'name': 'limit_next_open_10bp',
            'config': {'entry_type': 'limit_next_open', 'entry_offset_bps': 10},
            'description': 'Limit 10bp better than close, fill at next open'
        },
        {
            'name': 'limit_next_range',
            'config': {'entry_type': 'limit_next_range', 'entry_offset_bps': 0},
            'description': 'Limit at close, fill if touched during next bar'
        },
        {
            'name': 'limit_next_range_10bp',
            'config': {'entry_type': 'limit_next_range', 'entry_offset_bps': 10},
            'description': 'Limit 10bp better, fill if touched during next bar'
        },
        {
            'name': 'limit_next_range_20bp',
            'config': {'entry_type': 'limit_next_range', 'entry_offset_bps': 20},
            'description': 'Limit 20bp better, fill if touched during next bar'
        }
    ]

    results = []

    for cfg in configs:
        log.info(f"Testing: {cfg['name']} - {cfg['description']}")

        trades_df = analyze_order_config(
            cfg['name'],
            cfg['config'],
            DATA_DIR,
            PERIOD,
            INVERTED,
            FILTERS,
            SL_PCT,
            START_DATE,
            END_DATE
        )

        if len(trades_df) > 0:
            # Save trades
            output_file = OUTPUT_DIR / f"{cfg['name']}.csv"
            trades_df.to_csv(output_file, index=False)
            log.info(f"  {len(trades_df)} trades → {output_file}")

            # Compute metrics
            metrics = compute_metrics(trades_df, cfg['name'])
            results.append(metrics)
        else:
            log.warning(f"  No trades for {cfg['name']}")
            results.append({
                'config': cfg['name'],
                'trades': 0,
                'win_rate': 0.0,
                'avg_win': 0.0,
                'avg_loss': 0.0,
                'gross_exp': 0.0,
                'status': '❌ NO TRADES'
            })

    # Print comparison
    log.info("")
    log.info("=" * 80)
    log.info("ORDER TYPE COMPARISON")
    log.info("=" * 80)
    log.info(f"{'Config':<25} {'Trades':<8} {'Win%':<8} {'AvgWin%':<10} {'AvgLoss%':<10} {'GrossExp%':<10} {'Status':<15}")
    log.info("-" * 80)

    for r in results:
        log.info(
            f"{r['config']:<25} "
            f"{r['trades']:<8} "
            f"{r['win_rate']:<7.1f}% "
            f"{r['avg_win']:<9.3f}% "
            f"{r['avg_loss']:<9.3f}% "
            f"{r['gross_exp']:<9.3f}% "
            f"{r['status']:<15}"
        )

    # Save summary
    summary_df = pd.DataFrame(results)
    summary_file = OUTPUT_DIR / 'limit_order_summary.csv'
    summary_df.to_csv(summary_file, index=False)
    log.info("")
    log.info(f"Summary saved to: {summary_file}")

    # Find best config
    best = max(results, key=lambda x: x['gross_exp'])
    log.info("")
    log.info("=" * 80)
    log.info("RECOMMENDATION")
    log.info("=" * 80)
    log.info(f"Best order type: {best['config']}")
    log.info(f"  Gross expectancy: {best['gross_exp']:.3f}%")
    log.info(f"  Trades: {best['trades']}")
    log.info(f"  Win rate: {best['win_rate']:.1f}%")
    log.info("")

if __name__ == '__main__':
    main()
