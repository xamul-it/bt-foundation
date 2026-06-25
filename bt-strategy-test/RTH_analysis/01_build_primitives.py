#!/usr/bin/env python3
"""
01_build_primitives.py — RTH Primitive Builder

Scarica OHLCV daily da Yahoo Finance per ogni simbolo dell'universo e
calcola le primitive RTH giornaliere. Salva il risultato in parquet.

Filosofia: le primitive RTH usano solo ln(C_t/O_t) e derivati, senza mai
toccare rendimenti close-close come feature (solo come check di coerenza).
"""

import argparse
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

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
# Core computation (testable in isolation)
# ---------------------------------------------------------------------------

def compute_primitives(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcola le primitive RTH da un DataFrame OHLCV single-symbol.

    Parameters
    ----------
    df : pd.DataFrame
        Deve contenere colonne: Open, High, Low, Close, Volume
        Indice: DatetimeIndex (o convertibile)

    Returns
    -------
    pd.DataFrame
        Stesse righe di df (dopo drop NaN/zero), con colonne:
        open, high, low, close, volume,
        rth_loggain, rth_up_loggain, rth_down_loggain, rth_range_log,
        log_volume, overnight_loggain, cc_logret
    """
    # Normalizza nomi colonne: accetta sia capitalizzati che lowercase
    df = df.copy()
    col_map = {c.lower(): c for c in df.columns}
    needed = ["open", "high", "low", "close", "volume"]
    rename = {}
    for n in needed:
        if n in col_map and col_map[n] != n:
            rename[col_map[n]] = n
    if rename:
        df = df.rename(columns=rename)

    # Seleziona solo le colonne necessarie
    df = df[needed].copy()

    # Drop righe con NaN
    n_before = len(df)
    df = df.dropna(subset=needed)
    dropped_nan = n_before - len(df)
    if dropped_nan > 0:
        logger.warning("  Dropped %d rows with NaN", dropped_nan)

    # Drop righe con Open/High/Low/Close == 0
    zero_mask = (df["open"] == 0) | (df["high"] == 0) | (df["low"] == 0) | (df["close"] == 0)
    n_zero = zero_mask.sum()
    if n_zero > 0:
        logger.warning("  Dropped %d rows with zero OHLC", n_zero)
        df = df[~zero_mask]

    if df.empty:
        return pd.DataFrame(columns=[
            "open", "high", "low", "close", "volume",
            "rth_loggain", "rth_up_loggain", "rth_down_loggain",
            "rth_range_log", "log_volume", "overnight_loggain", "cc_logret",
        ])

    o = df["open"].values.astype(float)
    h = df["high"].values.astype(float)
    lo = df["low"].values.astype(float)
    c = df["close"].values.astype(float)
    v = df["volume"].values.astype(float)

    # RTH primitives (solo intra-sessione)
    rth_loggain = np.log(c / o)                     # pressione netta RTH
    rth_up_loggain = np.log(h / o)                  # escursione rialzista
    rth_down_loggain = np.log(lo / o)               # escursione ribassista (≤0)

    # Range: se H == L (candela flat), ln(H/L) = ln(1) = 0
    hl_ratio = np.where(lo > 0, h / lo, 1.0)
    rth_range_log = np.log(hl_ratio)                # range sessione

    log_volume = np.log(np.where(v > 0, v, 1.0))    # volume log-normalizzato

    # Overnight: usa close precedente (shift +1)
    c_prev = np.empty_like(c)
    c_prev[0] = np.nan
    c_prev[1:] = c[:-1]
    overnight_loggain = np.log(o / c_prev)          # ln(O_t / C_{t-1})

    # Close-to-close (solo check coerenza)
    cc_logret = np.log(c / c_prev)                  # ln(C_t / C_{t-1})

    result = pd.DataFrame(
        {
            "open": o,
            "high": h,
            "low": lo,
            "close": c,
            "volume": v,
            "rth_loggain": rth_loggain,
            "rth_up_loggain": rth_up_loggain,
            "rth_down_loggain": rth_down_loggain,
            "rth_range_log": rth_range_log,
            "log_volume": log_volume,
            "overnight_loggain": overnight_loggain,
            "cc_logret": cc_logret,
        },
        index=df.index,
    )

    return result


# ---------------------------------------------------------------------------
# Download + processing per singolo simbolo
# ---------------------------------------------------------------------------

def process_symbol(
    symbol: str,
    start: str,
    end: str,
    verbose: bool = False,
) -> pd.DataFrame | None:
    """
    Scarica i dati per un simbolo e calcola le primitive.

    Returns
    -------
    pd.DataFrame con colonna 'symbol' aggiunta, o None se fallisce.
    """
    try:
        raw = yf.download(
            symbol,
            start=start,
            end=end,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception as exc:
        logger.warning("SKIP %s — download error: %s", symbol, exc)
        return None

    if raw is None or raw.empty:
        logger.warning("SKIP %s — dati vuoti", symbol)
        return None

    # yfinance con multi-level columns: flatten se necessario
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    # Verifica colonne minime
    required_cols = {"Open", "High", "Low", "Close", "Volume"}
    if not required_cols.issubset(set(raw.columns)):
        logger.warning("SKIP %s — colonne mancanti: %s", symbol, raw.columns.tolist())
        return None

    if verbose:
        logger.info("  %s: %d righe scaricate", symbol, len(raw))

    prim = compute_primitives(raw)

    if prim.empty:
        logger.warning("SKIP %s — nessuna riga valida dopo cleaning", symbol)
        return None

    prim.insert(0, "symbol", symbol)
    return prim


# ---------------------------------------------------------------------------
# Coerenza check
# ---------------------------------------------------------------------------

def check_coherence(df: pd.DataFrame) -> tuple[bool, float]:
    """
    Verifica che cc_logret ≈ overnight_loggain + rth_loggain per ogni riga.

    Ignora le righe dove overnight_loggain è NaN (primo giorno di ogni simbolo).

    Returns
    -------
    (ok: bool, max_error: float)
    """
    mask = df["overnight_loggain"].notna() & df["cc_logret"].notna()
    sub = df[mask]
    if sub.empty:
        return True, 0.0

    reconstructed = sub["overnight_loggain"] + sub["rth_loggain"]
    errors = (sub["cc_logret"] - reconstructed).abs()
    max_err = float(errors.max())
    ok = max_err < 1e-8
    return ok, max_err


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    workspace = Path(__file__).resolve().parents[2]  # backtrader/
    out_default = Path(__file__).resolve().parent / "out"

    parser = argparse.ArgumentParser(
        description="Calcola le primitive RTH giornaliere per un universo di simboli."
    )
    parser.add_argument(
        "--tickers",
        default=str(workspace / "config-common" / "tickers" / "NASDAQ_100_US.json"),
        help="Path al JSON con lista di ticker (default: NASDAQ_100_US.json)",
    )
    parser.add_argument(
        "--start",
        default="2015-01-01",
        help="Data inizio YYYY-MM-DD (default: 2015-01-01)",
    )
    parser.add_argument(
        "--end",
        default=date.today().strftime("%Y-%m-%d"),
        help="Data fine YYYY-MM-DD (default: oggi)",
    )
    parser.add_argument(
        "--out-dir",
        default=str(out_default),
        help="Directory output (default: bt-strategy-test/RTH_analysis/out)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Logging dettagliato per simbolo",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Carica tickers
    ticker_path = Path(args.tickers)
    if not ticker_path.exists():
        logger.error("File tickers non trovato: %s", ticker_path)
        sys.exit(1)

    with open(ticker_path) as f:
        tickers = json.load(f)

    if not isinstance(tickers, list):
        logger.error("Il JSON dei tickers deve essere una lista di stringhe.")
        sys.exit(1)

    logger.info("Tickers caricati: %d simboli da %s", len(tickers), ticker_path.name)
    logger.info("Periodo: %s → %s", args.start, args.end)

    # Crea output dir
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "rth_primitives.parquet"

    # Processa tutti i simboli
    frames = []
    processed = 0
    skipped = []

    for i, symbol in enumerate(tickers):
        logger.info("[%d/%d] Processo %s ...", i + 1, len(tickers), symbol)
        result = process_symbol(symbol, start=args.start, end=args.end, verbose=args.verbose)
        if result is not None:
            frames.append(result)
            processed += 1
        else:
            skipped.append(symbol)

    if not frames:
        logger.error("Nessun simbolo processato con successo. Uscita.")
        sys.exit(1)

    # Concatena
    combined = pd.concat(frames)

    # Reset index: feature_date come colonna (non index)
    combined = combined.reset_index()
    combined = combined.rename(columns={"index": "feature_date", "Date": "feature_date"})

    # Assicura tipi corretti
    if "feature_date" in combined.columns:
        combined["feature_date"] = pd.to_datetime(combined["feature_date"])
    combined["symbol"] = combined["symbol"].astype(str)

    # Riordina colonne secondo schema atteso
    col_order = [
        "feature_date", "symbol",
        "open", "high", "low", "close", "volume",
        "rth_loggain", "rth_up_loggain", "rth_down_loggain",
        "rth_range_log", "log_volume", "overnight_loggain", "cc_logret",
    ]
    # Mantieni solo colonne presenti (defensive)
    col_order = [c for c in col_order if c in combined.columns]
    combined = combined[col_order]

    # Ordina per symbol + date
    combined = combined.sort_values(["symbol", "feature_date"]).reset_index(drop=True)

    # Check coerenza
    coherence_ok, max_err = check_coherence(combined)
    coherence_status = "OK" if coherence_ok else "WARN"

    # Salva parquet
    combined.to_parquet(out_path, engine="pyarrow", index=False)

    # Riepilogo finale
    date_min = combined["feature_date"].min().date() if not combined.empty else "N/A"
    date_max = combined["feature_date"].max().date() if not combined.empty else "N/A"

    print(f"\nSimboli processati: {processed}/{len(tickers)}")
    if skipped:
        print(f"Simboli saltati: {', '.join(skipped)}")
    print(f"Date range: {date_min} → {date_max}")
    print(f"Righe totali: {len(combined):,}")
    print(f"Coerenza cc_logret: {coherence_status} (max error: {max_err:.2e})")
    print(f"Salvato: {out_path}")

    if not coherence_ok:
        logger.warning(
            "Coerenza cc_logret NON soddisfatta (max_err=%.2e > 1e-8). "
            "Possibile problema nei dati grezzi.",
            max_err,
        )


if __name__ == "__main__":
    main()
