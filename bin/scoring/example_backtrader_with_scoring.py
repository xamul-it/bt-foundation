#!/usr/bin/env python3
"""
Esempio di come usare scoring_data.csv in una strategia Backtrader
"""

import backtrader as bt
import pandas as pd
from datetime import datetime, time

# ═══════════════════════════════════════════════════════════════
# STRATEGY CON SCORING DATA
# ═══════════════════════════════════════════════════════════════

class IntradayStrategyWithScoring(bt.Strategy):
    """
    Strategy che usa scoring data pre-calcolati per filtrare asset
    """
    params = (
        ('min_score', 70),           # Score minimo per trading
        ('preferred_rating', 'EXCELLENT'),  # Rating preferito
        ('entry_time', time(9, 45)),
        ('exit_time', time(15, 30)),
        ('max_positions', 5),
    )

    def __init__(self):
        # Carica scoring data
        self.scoring_df = pd.read_csv('scoring_data_example.csv')
        self.scoring_df['date'] = pd.to_datetime(self.scoring_df['date'])

        print(f"Loaded scoring data: {len(self.scoring_df)} records")
        print(f"Date range: {self.scoring_df['date'].min()} to {self.scoring_df['date'].max()}")
        print(f"Symbols: {self.scoring_df['symbol'].nunique()}")

        # Cache per evitare lookup ripetuti
        self.daily_scores = {}

    def prenext(self):
        """Called when not all data feeds have enough data"""
        self.next()

    def next(self):
        """Main strategy logic"""
        current_date = self.datas[0].datetime.date(0)
        current_time = self.datas[0].datetime.time(0)

        # Pre-carica score per oggi (una volta al giorno)
        if current_date not in self.daily_scores:
            self._load_daily_scores(current_date)

        # Check trading window
        if not self._is_trading_window(current_time):
            return

        # Force close alla fine del giorno
        if current_time >= self.p.exit_time:
            self._close_all_positions()
            return

        # Loop su tutti gli asset
        for data in self.datas:
            symbol = data._name
            position = self.getposition(data)

            # Recupera score per questo asset oggi
            score_data = self.daily_scores.get(current_date, {}).get(symbol)

            if score_data is None:
                continue  # Nessuno score disponibile per oggi

            # Entry logic
            if position.size == 0:
                self._check_entry(data, symbol, score_data)
            else:
                self._check_exit(data, symbol, score_data)

    def _load_daily_scores(self, current_date):
        """Carica scores per la data corrente"""
        # Filtra scoring data per questa data
        daily_data = self.scoring_df[
            self.scoring_df['date'] == pd.Timestamp(current_date)
        ]

        # Crea dizionario symbol -> score_data
        scores = {}
        for _, row in daily_data.iterrows():
            scores[row['symbol']] = row.to_dict()

        self.daily_scores[current_date] = scores

        # Log
        excellent = sum(1 for s in scores.values() if s['rating'] == 'EXCELLENT')
        good = sum(1 for s in scores.values() if s['rating'] == 'GOOD')
        print(f"\n{current_date}: Loaded scores for {len(scores)} symbols "
              f"(EXCELLENT: {excellent}, GOOD: {good})")

    def _check_entry(self, data, symbol, score_data):
        """Check entry conditions usando scoring data"""

        # 1. Filtra per score minimo
        if score_data['score'] < self.p.min_score:
            return

        # 2. Limit max positions
        if self._count_positions() >= self.p.max_positions:
            return

        # 3. Entry signal (semplificato per esempio)
        # In realtà useresti indicatori su 1-min bars
        entry_signal = False

        if score_data['pattern'] == 'Gap Down → Rally':
            # Attendi gap down e poi reversal
            # (qui semplificato, in realtà controlli VWAP, momentum, etc)
            entry_signal = True

        elif score_data['pattern'] == 'Flat → Rally':
            # Entry su breakout
            entry_signal = True

        if entry_signal:
            # Calcola size basato su score
            size = self._calculate_size(data, score_data)

            # Buy
            self.buy(data=data, size=size)

            self.log(f"ENTRY {symbol}: Score={score_data['score']:.1f}, "
                    f"Pattern={score_data['pattern']}, "
                    f"Expected={score_data['expected_move_pct']:.2f}%")

    def _check_exit(self, data, symbol, score_data):
        """Check exit conditions"""
        position = self.getposition(data)

        # Exit semplificato (in realtà usi stop loss, target, trailing)
        # Qui solo per esempio
        pnl_pct = (data.close[0] - position.price) / position.price

        # Exit se profit >= expected move
        if pnl_pct >= score_data['expected_move_pct'] / 100:
            self.close(data=data)
            self.log(f"EXIT {symbol}: Target hit, PnL={pnl_pct:.2%}")

        # Exit se loss > -0.5%
        elif pnl_pct <= -0.005:
            self.close(data=data)
            self.log(f"EXIT {symbol}: Stop loss, PnL={pnl_pct:.2%}")

    def _calculate_size(self, data, score_data):
        """Calcola position size basato su score"""
        # Più alto è lo score, più grande la posizione
        base_size = 100

        if score_data['score'] >= 80:
            multiplier = 1.5
        elif score_data['score'] >= 70:
            multiplier = 1.2
        else:
            multiplier = 1.0

        return int(base_size * multiplier)

    def _is_trading_window(self, current_time):
        """Check se siamo nella trading window"""
        return (
            current_time >= self.p.entry_time and
            current_time < self.p.exit_time
        )

    def _count_positions(self):
        """Conta posizioni aperte"""
        return sum(1 for data in self.datas if self.getposition(data).size > 0)

    def _close_all_positions(self):
        """Chiudi tutte le posizioni"""
        for data in self.datas:
            if self.getposition(data).size > 0:
                self.close(data=data)
                self.log(f"EOD CLOSE {data._name}")

    def log(self, txt):
        """Logging"""
        dt = self.datas[0].datetime.datetime()
        print(f'{dt}: {txt}')

# ═══════════════════════════════════════════════════════════════
# ESEMPIO DI UTILIZZO
# ═══════════════════════════════════════════════════════════════

def main():
    """Main function"""
    print("="*60)
    print("BACKTRADER STRATEGY WITH SCORING DATA - EXAMPLE")
    print("="*60)

    # 1. Carica scoring data per determinare watchlist
    scoring_df = pd.read_csv('scoring_data_example.csv')

    # Filtra solo asset EXCELLENT per la prima data disponibile
    first_date = scoring_df['date'].min()
    watchlist = scoring_df[
        (scoring_df['date'] == first_date) &
        (scoring_df['rating'] == 'EXCELLENT')
    ]['symbol'].tolist()

    print(f"\nWatchlist (EXCELLENT on {first_date}):")
    print(f"  {watchlist}")

    # 2. Setup Cerebro
    cerebro = bt.Cerebro()

    # Broker settings
    cerebro.broker.setcash(100000)
    cerebro.broker.setcommission(commission=0.001)

    # 3. Add data feeds (esempio - in realtà useresti dati 1-min reali)
    # Per questo esempio, assumiamo di avere file CSV 1-min
    # for symbol in watchlist:
    #     data = bt.feeds.GenericCSVData(
    #         dataname=f'data/1min/{symbol}.csv',
    #         dtformat='%Y-%m-%d %H:%M:%S',
    #         timeframe=bt.TimeFrame.Minutes,
    #         compression=1,
    #     )
    #     cerebro.adddata(data, name=symbol)

    # 4. Add strategy
    cerebro.addstrategy(
        IntradayStrategyWithScoring,
        min_score=70,
        preferred_rating='EXCELLENT',
    )

    # 5. Add analyzers
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')

    # 6. Run
    print(f"\nStarting Portfolio Value: ${cerebro.broker.getvalue():,.2f}")

    # results = cerebro.run()

    # print(f"Final Portfolio Value: ${cerebro.broker.getvalue():,.2f}")

    print("\n" + "="*60)
    print("NOTE: This is a skeleton example.")
    print("You need to add real 1-minute data feeds to run the backtest.")
    print("="*60)

    # 7. Esempio di come recuperare score in una strategia custom
    print("\n" + "="*60)
    print("SCORING DATA USAGE EXAMPLE")
    print("="*60)

    # Simula recupero score per una specifica data/simbolo
    target_date = '2024-02-01'
    target_symbol = 'CRWD'

    score_row = scoring_df[
        (scoring_df['date'] == target_date) &
        (scoring_df['symbol'] == target_symbol)
    ]

    if len(score_row) > 0:
        print(f"\nScore data for {target_symbol} on {target_date}:")
        print(f"  Score: {score_row['score'].values[0]:.2f}")
        print(f"  Rating: {score_row['rating'].values[0]}")
        print(f"  Pattern: {score_row['pattern'].values[0]}")
        print(f"  Expected Move: {score_row['expected_move_pct'].values[0]:.2f}%")
        print(f"  Intraday R²: {score_row['intraday_r2'].values[0]:.4f}")
    else:
        print(f"\nNo score data found for {target_symbol} on {target_date}")

if __name__ == '__main__':
    main()
