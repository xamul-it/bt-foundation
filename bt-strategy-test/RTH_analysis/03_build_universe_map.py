#!/usr/bin/env python3
"""
03_build_universe_map.py — RTH Universe Map Builder

Legge rth_features.parquet, aggiunge trading_date (prossimo giorno di mercato
NYSE dopo la feature_date), calcola ranking cross-sectional percentile per
le 6 feature chiave, e salva rth_universe_map.parquet.

Anti-lookahead: le feature calcolate su feature_date sono disponibili solo
dal giorno di mercato successivo (trading_date). Backtrader filtra su
trading_date == data_corrente.
"""

import argparse
import json
import logging
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Colonne da rankare (cross-sectional per trading_date)
# ---------------------------------------------------------------------------
RANK_COLS = [
    "rth_mom_20",
    "rth_eff_20",
    "rth_signed_eff_20",
    "rth_range_exp_5_20",
    "rth_rvol_5_20",
    "rth_mom_norm_20",
]


# ---------------------------------------------------------------------------
# Calendar utilities
# ---------------------------------------------------------------------------

def load_market_days(calendar_path: Path) -> list:
    """
    Carica tutti i giorni di mercato dalla cache Alpaca.

    Returns
    -------
    list of datetime.date
        Lista ordinata di giorni di mercato.
    """
    with open(calendar_path, "r") as f:
        data = json.load(f)

    days = set()
    for month_data in data.get("months", {}).values():
        for day_str in month_data.get("days", {}).keys():
            days.add(pd.Timestamp(day_str).date())

    return sorted(days)


def _next_market_day_from_list(date, market_days_sorted: list):
    """
    Trova il primo giorno in market_days_sorted strettamente dopo `date`.

    Parameters
    ----------
    date : datetime.date or pd.Timestamp
    market_days_sorted : list of datetime.date

    Returns
    -------
    datetime.date or None
        Il prossimo giorno di mercato, oppure None se non trovato.
    """
    import bisect
    if hasattr(date, "date"):
        date = date.date()
    # bisect_right per trovare l'indice del primo elemento > date
    idx = bisect.bisect_right(market_days_sorted, date)
    if idx < len(market_days_sorted):
        return market_days_sorted[idx]
    return None


def _next_market_day_fallback(date) -> "datetime.date":
    """
    Fallback: prossimo giorno lavorativo dopo `date` usando
    pandas CustomBusinessDay con USFederalHolidayCalendar.
    Approssima il calendario NYSE (non copre tutti i mezze-giornate, ma
    copre le principali festività federali).
    """
    from pandas.tseries.offsets import CustomBusinessDay
    from pandas.tseries.holiday import USFederalHolidayCalendar

    cbd = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    ts = pd.Timestamp(date)
    next_ts = ts + cbd
    return next_ts.date()


def get_trading_date(feature_date, market_days_sorted: list):
    """
    Ritorna il prossimo giorno di mercato dopo feature_date.
    Prima tenta dalla lista cache; se non trovato usa il fallback.

    Parameters
    ----------
    feature_date : pd.Timestamp or datetime.date
    market_days_sorted : list of datetime.date

    Returns
    -------
    (datetime.date, bool)
        Il trading_date e un flag `used_fallback`.
    """
    result = _next_market_day_from_list(feature_date, market_days_sorted)
    if result is not None:
        return result, False
    # Fallback per date future / non coperte
    result = _next_market_day_fallback(feature_date)
    return result, True


# ---------------------------------------------------------------------------
# Core computation (testable in isolation)
# ---------------------------------------------------------------------------

def compute_universe_map(df: pd.DataFrame, market_days) -> pd.DataFrame:
    """
    Aggiunge trading_date e ranking cross-sectional al DataFrame delle feature.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame delle feature RTH. Deve contenere: feature_date, symbol,
        e le colonne in RANK_COLS.
    market_days : list
        Lista di giorni di mercato (datetime.date, pd.Timestamp o str YYYY-MM-DD).
        Deve essere già ordinata (o verrà ordinata internamente).

    Returns
    -------
    pd.DataFrame
        Copia di df con colonne aggiunte:
        - trading_date (datetime64[ns])
        - {col}_rank_pct per ogni col in RANK_COLS
    """
    df = df.copy()

    # Normalizza market_days a lista di datetime.date ordinata
    normalized_days = []
    for d in market_days:
        if isinstance(d, str):
            normalized_days.append(pd.Timestamp(d).date())
        elif hasattr(d, "date"):
            normalized_days.append(d.date())
        else:
            normalized_days.append(d)
    market_days_sorted = sorted(set(normalized_days))

    # Assicura feature_date sia Timestamp
    if not pd.api.types.is_datetime64_any_dtype(df["feature_date"]):
        df["feature_date"] = pd.to_datetime(df["feature_date"])

    # Calcola trading_date per ogni feature_date unica
    unique_fdates = df["feature_date"].unique()
    trading_date_map = {}
    fallback_count = 0

    for fd in unique_fdates:
        td, used_fallback = get_trading_date(fd, market_days_sorted)
        trading_date_map[fd] = td
        if used_fallback:
            fallback_count += 1
            logger.warning(
                "Fallback offset usato per feature_date=%s → trading_date=%s",
                pd.Timestamp(fd).strftime("%Y-%m-%d"),
                td,
            )

    # Mappa trading_date e converte a datetime64[ns]
    df["trading_date"] = df["feature_date"].map(trading_date_map)
    df["trading_date"] = pd.to_datetime(df["trading_date"])

    # Ranking cross-sectional percentile per trading_date
    for col in RANK_COLS:
        rank_col = f"{col}_rank_pct"
        if col in df.columns:
            df[rank_col] = df.groupby("trading_date")[col].rank(
                pct=True, na_option="keep"
            )
        else:
            logger.warning("Colonna %s non trovata in df, rank non calcolato", col)
            df[rank_col] = np.nan

    return df, fallback_count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    here = Path(__file__).resolve().parent
    repo_root = here.parent.parent
    out_default = here / "out"
    features_default = out_default / "rth_features.parquet"
    workspace_root = repo_root
    calendar_default = workspace_root / "config-common" / "cache" / "alpaca_calendar_cache.json"

    parser = argparse.ArgumentParser(
        description="Aggiunge trading_date e ranking cross-sectional alle feature RTH."
    )
    parser.add_argument(
        "--features",
        default=str(features_default),
        help="Path input parquet (default: bt-strategy-test/RTH_analysis/out/rth_features.parquet)",
    )
    parser.add_argument(
        "--calendar",
        default=str(calendar_default),
        help="Path cache calendar JSON (default: config-common/cache/alpaca_calendar_cache.json)",
    )
    parser.add_argument(
        "--out-dir",
        default=str(out_default),
        help="Directory output (default: bt-strategy-test/RTH_analysis/out)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Logging dettagliato",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    features_path = Path(args.features)
    if not features_path.exists():
        logger.error("File features non trovato: %s", features_path)
        sys.exit(1)

    calendar_path = Path(args.calendar)
    if not calendar_path.exists():
        logger.error("File calendar non trovato: %s", calendar_path)
        sys.exit(1)

    # Carica features
    logger.info("Carico features: %s", features_path)
    df = pd.read_parquet(features_path)
    n_rows_in = len(df)
    n_symbols = df["symbol"].nunique()

    # Carica calendario
    logger.info("Carico calendario: %s", calendar_path)
    market_days = load_market_days(calendar_path)
    n_market_days = len(market_days)

    # Range feature_date
    fd_min = pd.to_datetime(df["feature_date"]).min().strftime("%Y-%m-%d")
    fd_max = pd.to_datetime(df["feature_date"]).max().strftime("%Y-%m-%d")

    # Calcola universe map
    df_out, fallback_count = compute_universe_map(df, market_days)

    n_unique_fdates = df["feature_date"].nunique()
    coverage_pct = (
        (n_unique_fdates - fallback_count) / n_unique_fdates * 100
        if n_unique_fdates > 0
        else 0
    )

    # Salva output
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "rth_universe_map.parquet"
    df_out.to_parquet(out_path, engine="pyarrow", index=False)

    # Riepilogo a schermo
    print(f"\nLetto: rth_features.parquet ({n_rows_in:,} righe, {n_symbols} simboli)")
    print(f"Giorni di mercato da cache: {n_market_days}")
    print(f"Feature date range: {fd_min} → {fd_max}")
    print(f"Coverage cache: {coverage_pct:.0f}% ({fallback_count} con fallback offset)")
    print(f"Ranking calcolato su {len(RANK_COLS)} feature chiave")
    print(f"Salvato: {out_path}")
    print(f"Schema finale: {len(df_out):,} righe, {len(df_out.columns)} colonne")


if __name__ == "__main__":
    main()
