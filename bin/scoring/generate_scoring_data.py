#!/usr/bin/env python3
"""
Script per generare dati di scoring rolling per backtest e paper trading

Usage:
    # Backtest mode: genera score per ogni giorno da fromdate
    python generate_scoring_data.py --mode backtest --config config.json

    # Paper trading mode: genera score solo per oggi
    python generate_scoring_data.py --mode papertrading --config config.json

Input:
    - config.json: configurazione con lista simboli, parametri, etc.

Output:
    - scoring_data.csv: file con score per data/simbolo
"""

import argparse
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from scipy import stats
import warnings
import logging
from tqdm import tqdm

warnings.filterwarnings('ignore')

# ═══════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════

def setup_logging(log_file='scoring_generator.log'):
    """Setup logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# ═══════════════════════════════════════════════════════════════
# CORE FUNCTIONS (da notebook)
# ═══════════════════════════════════════════════════════════════

def calculate_overnight_intraday_returns(df):
    """
    Calcola rendimenti overnight e intraday

    Args:
        df: DataFrame con OHLC

    Returns:
        DataFrame con overnight_return e intraday_return
    """
    df = df.copy()

    # Overnight return: Close(t-1) -> Open(t)
    df['overnight_return'] = (df['Open'] / df['Close'].shift(1) - 1)

    # Intraday return: Open(t) -> Close(t)
    df['intraday_return'] = (df['Close'] / df['Open'] - 1)

    # Total return
    df['total_return'] = df['Close'].pct_change()

    return df.dropna()

def fit_t_student(returns):
    """
    Fit distribuzione t-Student sui rendimenti

    Returns:
        dict con parametri: df, loc, scale
    """
    returns_clean = returns[np.isfinite(returns)]

    if len(returns_clean) < 30:
        return None

    try:
        df_t, loc_t, scale_t = stats.t.fit(returns_clean)
        return {
            'df': df_t,
            'loc': loc_t,
            'scale': scale_t
        }
    except:
        return None

def linear_regression_returns(returns):
    """
    Esegue regressione lineare su rendimenti CUMULATIVI

    IMPORTANTE: La regressione viene fatta su cumsum dei rendimenti,
    non sui rendimenti singoli, per catturare il trend complessivo.

    Returns:
        dict con slope, r_squared
    """
    if len(returns) < 30:
        return None

    # IMPORTANTE: Usa rendimenti CUMULATIVI per la regressione
    y_cumulative = returns.cumsum().values
    x = np.arange(len(y_cumulative))

    # Rimuovi NaN
    mask = np.isfinite(y_cumulative)
    x = x[mask]
    y = y_cumulative[mask]

    if len(x) < 30:
        return None

    try:
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
        return {
            'slope': slope,
            'r_squared': r_value**2
        }
    except:
        return None

def calculate_intraday_score(intraday_r2, intraday_slope, overnight_slope,
                              overnight_r2, intraday_sigma):
    """
    Calcola Intraday Tradability Score (0-100)

    Criteri:
    - Intraday R² > 0.65: predicibilità (40 punti)
    - Intraday Slope > 0: trend positivo (25 punti)
    - Overnight Slope < 0: gap down (15 punti)
    - Overnight R² > 0.5: gap predicibili (10 punti)
    - Intraday σ: 0.5-2% (10 punti)
    """
    score = 0

    # 1. Intraday R² (0-40 punti)
    if intraday_r2 >= 0.80:
        score += 40
    elif intraday_r2 >= 0.65:
        score += 30 + (intraday_r2 - 0.65) * 66.67
    elif intraday_r2 >= 0.50:
        score += 15 + (intraday_r2 - 0.50) * 100
    else:
        score += intraday_r2 * 30

    # 2. Intraday Slope positivo (0-25 punti)
    if intraday_slope > 0.005:
        score += 25
    elif intraday_slope > 0.002:
        score += 20
    elif intraday_slope > 0:
        score += 10 + (intraday_slope / 0.002) * 10
    else:
        score += 0

    # 3. Overnight Slope negativo (0-15 punti)
    if overnight_slope < -0.002:
        score += 15
    elif overnight_slope < 0:
        score += 10 + abs(overnight_slope / 0.002) * 5
    elif overnight_slope == 0:
        score += 5
    else:
        score += 0

    # 4. Overnight R² alto (0-10 punti)
    if overnight_r2 >= 0.70:
        score += 10
    elif overnight_r2 >= 0.50:
        score += 5 + (overnight_r2 - 0.50) * 25
    else:
        score += overnight_r2 * 10

    # 5. Volatilità intraday (0-10 punti)
    if 0.005 <= intraday_sigma <= 0.020:
        score += 10
    elif 0.003 <= intraday_sigma <= 0.030:
        score += 7
    elif intraday_sigma < 0.003:
        score += 3
    else:
        score += 5

    return min(score, 100)

def classify_pattern(overnight_slope, intraday_slope):
    """Classifica pattern overnight/intraday"""
    if overnight_slope < 0 and intraday_slope > 0:
        return 'Gap Down → Rally'
    elif overnight_slope >= 0 and intraday_slope > 0:
        return 'Flat → Rally'
    elif overnight_slope < 0 and intraday_slope < 0:
        return 'Gap Down → Fade'
    else:
        return 'Gap Up → Continue'

def classify_rating(score):
    """Classifica rating basato su score"""
    if score >= 70:
        return 'EXCELLENT'
    elif score >= 55:
        return 'GOOD'
    elif score >= 40:
        return 'FAIR'
    else:
        return 'POOR'

# ═══════════════════════════════════════════════════════════════
# SCORING CALCULATOR
# ═══════════════════════════════════════════════════════════════

class ScoringCalculator:
    """
    Calcola score per un simbolo in una specifica data
    usando finestra rolling
    """

    def __init__(self, window_days=180, min_bars=120):
        """
        Args:
            window_days: giorni di lookback per calcolo score
            min_bars: minimo numero di barre necessarie
        """
        self.window_days = window_days
        self.min_bars = min_bars

    def calculate_score_for_date(self, df, target_date):
        """
        Calcola score usando finestra che termina a target_date

        Args:
            df: DataFrame con dati OHLC (daily)
            target_date: data di chiusura dell'ultima barra da usare

        Returns:
            dict con score e metriche o None se insufficienti dati

        Note:
            La colonna 'date' nell'output indica la data dell'ultima barra usata.
            La strategy Backtrader si occuperà di fare lookup corretto
            considerando weekend/holidays.
        """
        # Finestra: da (target_date - window_days) a target_date (incluso)
        end_date = pd.to_datetime(target_date)
        start_date = end_date - timedelta(days=self.window_days)

        # Filtra dati nella finestra
        window_df = df[(df.index >= start_date) & (df.index <= end_date)].copy()

        # Verifica minimo dati
        if len(window_df) < self.min_bars:
            logger.debug(f"Insufficient data: {len(window_df)} bars (need {self.min_bars})")
            return None

        # Calcola overnight/intraday returns
        try:
            window_df = calculate_overnight_intraday_returns(window_df)
        except Exception as e:
            logger.error(f"Error calculating returns: {e}")
            return None

        # Fit t-student per overnight e intraday
        overnight_fit = fit_t_student(window_df['overnight_return'])
        intraday_fit = fit_t_student(window_df['intraday_return'])

        if overnight_fit is None or intraday_fit is None:
            logger.debug("Failed to fit t-student distributions")
            return None

        # Regressione lineare per overnight e intraday
        overnight_reg = linear_regression_returns(window_df['overnight_return'])
        intraday_reg = linear_regression_returns(window_df['intraday_return'])

        if overnight_reg is None or intraday_reg is None:
            logger.debug("Failed linear regression")
            return None

        # Estrai metriche
        intraday_r2 = intraday_reg['r_squared']
        intraday_slope = intraday_reg['slope']
        overnight_slope = overnight_reg['slope']
        overnight_r2 = overnight_reg['r_squared']
        intraday_sigma = intraday_fit['scale']

        # Calcola score
        score = calculate_intraday_score(
            intraday_r2, intraday_slope, overnight_slope,
            overnight_r2, intraday_sigma
        )

        # Classifica pattern e rating
        pattern = classify_pattern(overnight_slope, intraday_slope)
        rating = classify_rating(score)

        # Expected move intraday
        expected_move = intraday_slope * 100  # in percentuale

        return {
            'date': end_date.strftime('%Y-%m-%d'),  # Data ultima barra usata
            'score': round(score, 2),
            'rating': rating,
            'intraday_r2': round(intraday_r2, 4),
            'intraday_slope': round(intraday_slope, 6),
            'overnight_slope': round(overnight_slope, 6),
            'overnight_r2': round(overnight_r2, 4),
            'expected_move_pct': round(expected_move, 4),
            'intraday_sigma_pct': round(intraday_sigma * 100, 4),
            'pattern': pattern,
            'n_bars': len(window_df)
        }

# ═══════════════════════════════════════════════════════════════
# DATA LOADER
# ═══════════════════════════════════════════════════════════════

class DataLoader:
    """Carica dati storici per simboli"""

    def __init__(self, data_folder='data/daily'):
        self.data_folder = Path(data_folder)

    def load_symbol_data(self, symbol):
        """
        Carica dati daily per un simbolo

        Returns:
            DataFrame con OHLC o None se errore
        """
        file_path = self.data_folder / f'{symbol}.csv'

        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return None

        try:
            df = pd.read_csv(file_path, index_col=0, parse_dates=True)

            # Normalizza nomi colonne
            df.columns = [col.capitalize() for col in df.columns]

            # Verifica colonne necessarie
            required = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in df.columns for col in required):
                logger.error(f"Missing required columns in {file_path}")
                return None

            # Ordina per data
            df = df.sort_index()

            logger.info(f"Loaded {symbol}: {len(df)} bars from {df.index[0]} to {df.index[-1]}")
            return df

        except Exception as e:
            logger.error(f"Error loading {symbol}: {e}")
            return None

# ═══════════════════════════════════════════════════════════════
# MAIN GENERATOR
# ═══════════════════════════════════════════════════════════════

class ScoringDataGenerator:
    """Main class per generare dati scoring"""

    def __init__(self, config):
        self.config = config
        self.data_loader = DataLoader(config.get('data_folder', 'data/daily'))
        self.calculator = ScoringCalculator(
            window_days=config.get('window_days', 180),
            min_bars=config.get('min_bars', 120)
        )

    def generate_backtest_data(self, symbols, from_date, to_date=None):
        """
        Genera score per ogni giorno da from_date a to_date

        Args:
            symbols: lista simboli
            from_date: data inizio
            to_date: data fine (default: today)

        Returns:
            DataFrame con risultati
        """
        if to_date is None:
            to_date = datetime.now()

        from_date = pd.to_datetime(from_date)
        to_date = pd.to_datetime(to_date)

        logger.info(f"Generating backtest data from {from_date.date()} to {to_date.date()}")
        logger.info(f"Symbols: {len(symbols)}")
        logger.info(f"Window: {self.calculator.window_days} days, Min bars: {self.calculator.min_bars}")

        results = []

        # Loop su simboli
        for symbol in tqdm(symbols, desc="Processing symbols"):
            # Carica dati
            df = self.data_loader.load_symbol_data(symbol)

            if df is None:
                logger.warning(f"Skipping {symbol}: no data")
                continue

            # Genera date range: solo date effettivamente presenti nei dati
            # Questo automaticamente esclude weekend e holidays
            available_dates = df[(df.index >= from_date) & (df.index <= to_date)].index

            logger.info(f"Processing {symbol}: {len(available_dates)} trading days")

            # Loop su date
            for target_date in tqdm(available_dates, desc=f"  {symbol}", leave=False):
                # Calcola score per questa data
                score_data = self.calculator.calculate_score_for_date(df, target_date)

                if score_data is not None:
                    score_data['symbol'] = symbol
                    results.append(score_data)

        # Converti a DataFrame
        if len(results) == 0:
            logger.error("No results generated!")
            return pd.DataFrame()

        results_df = pd.DataFrame(results)

        # Riordina colonne
        cols = ['date', 'symbol', 'score', 'rating', 'intraday_r2', 'intraday_slope',
                'overnight_slope', 'overnight_r2', 'expected_move_pct',
                'intraday_sigma_pct', 'pattern', 'n_bars']
        results_df = results_df[cols]

        # Ordina per data e score
        results_df = results_df.sort_values(['date', 'score'], ascending=[True, False])

        logger.info(f"Generated {len(results_df)} records for {results_df['symbol'].nunique()} symbols")
        logger.info(f"Date range: {results_df['date'].min()} to {results_df['date'].max()}")

        return results_df

    def generate_papertrading_data(self, symbols):
        """
        Genera score usando l'ultimo dato disponibile

        Args:
            symbols: lista simboli

        Returns:
            DataFrame con risultati

        Note:
            Lo score viene calcolato usando l'ultimo dato disponibile
            per ogni simbolo. La data nel CSV è la data dell'ultimo dato usato.
            La strategy si occuperà di usare lo score al momento giusto.
        """
        logger.info(f"Generating paper trading data")
        logger.info(f"Using latest available data for each symbol")
        logger.info(f"Symbols: {len(symbols)}")

        results = []

        # Loop su simboli
        for symbol in tqdm(symbols, desc="Processing symbols"):
            # Carica dati
            df = self.data_loader.load_symbol_data(symbol)

            if df is None:
                logger.warning(f"Skipping {symbol}: no data")
                continue

            # Usa l'ultima data disponibile nel dataframe
            last_date = df.index[-1]

            # Calcola score usando finestra che termina all'ultimo dato disponibile
            score_data = self.calculator.calculate_score_for_date(df, last_date)

            if score_data is not None:
                score_data['symbol'] = symbol
                results.append(score_data)
                logger.info(f"{symbol}: score={score_data['score']:.1f}, last_data={last_date.date()}")
            else:
                logger.warning(f"Could not calculate score for {symbol}")

        if len(results) == 0:
            logger.error("No results generated!")
            return pd.DataFrame()

        results_df = pd.DataFrame(results)

        # Riordina colonne
        cols = ['date', 'symbol', 'score', 'rating', 'intraday_r2', 'intraday_slope',
                'overnight_slope', 'overnight_r2', 'expected_move_pct',
                'intraday_sigma_pct', 'pattern', 'n_bars']
        results_df = results_df[cols]

        # Ordina per score
        results_df = results_df.sort_values('score', ascending=False)

        logger.info(f"Generated scores for {len(results_df)} symbols")
        logger.info(f"Top picks: {results_df[results_df['rating'] == 'EXCELLENT']['symbol'].tolist()}")

        return results_df

# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

def load_config(config_path):
    """Carica configurazione da JSON"""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"Loaded config from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        raise

def save_results(df, output_path):
    """Salva risultati su file"""
    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Saved results to {output_path} ({len(df)} records)")

        # Stats summary
        logger.info("\nSummary:")
        logger.info(f"  Total records: {len(df)}")
        logger.info(f"  Symbols: {df['symbol'].nunique()}")
        logger.info(f"  Date range: {df['date'].min()} to {df['date'].max()}")
        logger.info(f"\nRating distribution:")
        for rating, count in df['rating'].value_counts().items():
            logger.info(f"    {rating}: {count} ({count/len(df)*100:.1f}%)")

    except Exception as e:
        logger.error(f"Error saving results: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(
        description='Generate scoring data for intraday trading strategy'
    )
    parser.add_argument(
        '--mode',
        choices=['backtest', 'papertrading'],
        required=True,
        help='Generation mode'
    )
    parser.add_argument(
        '--config',
        required=True,
        help='Path to config JSON file'
    )
    parser.add_argument(
        '--output',
        help='Output file path (default: from config or auto-generated)'
    )

    args = parser.parse_args()

    # Carica config
    config = load_config(args.config)

    # Estrai parametri
    symbols = config.get('symbols', [])

    if len(symbols) == 0:
        logger.error("No symbols specified in config!")
        return 1

    # Inizializza generator
    generator = ScoringDataGenerator(config)

    # Genera dati
    if args.mode == 'backtest':
        from_date = config.get('from_date')
        to_date = config.get('to_date', None)

        if from_date is None:
            logger.error("'from_date' not specified in config for backtest mode!")
            return 1

        logger.info(f"Mode: BACKTEST")
        results_df = generator.generate_backtest_data(symbols, from_date, to_date)

        # Output path
        output_path = args.output or config.get('output_file', 'scoring_data_backtest.csv')

    else:  # papertrading
        logger.info(f"Mode: PAPER TRADING")
        results_df = generator.generate_papertrading_data(symbols)

        # Output path
        today = datetime.now().strftime('%Y%m%d')
        output_path = args.output or config.get('output_file', f'scoring_data_{today}.csv')

    # Salva risultati
    if len(results_df) > 0:
        save_results(results_df, output_path)
        logger.info("\n✓ Generation completed successfully!")
        return 0
    else:
        logger.error("No results to save!")
        return 1

if __name__ == '__main__':
    import sys
    sys.exit(main())
