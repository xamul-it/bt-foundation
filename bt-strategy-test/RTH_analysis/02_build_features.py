#!/usr/bin/env python3
"""
02_build_features.py — RTH Feature Builder

Legge rth_primitives.parquet e calcola tutte le feature rolling RTH su
finestre multiple. Salva il risultato in rth_features.parquet.

Famiglie di feature calcolate:
  1.  Momentum cumulato (rth_mom_{N})
  2.  Drift medio (rth_mean_{N})
  3.  Drift esponenziale (rth_ewm_mean_{N})
  4.  Accelerazione drift MACD-like
  5.  Volatilità (rth_std_{N})
  6.  Momentum normalizzato (rth_mom_norm_{N})
  7.  Efficiency ratio (rth_eff_{N}, rth_signed_eff_{N})
  8.  Win rate (rth_winrate_{N})
  9.  Pressione H/L (rth_upside_{N}, rth_downside_{N}, rth_pressure_balance_{N})
  10. Close-to-range (rth_close_to_range, rth_close_to_range_mean_{N})
  11. Range expansion (rth_range_mean_{N}, rth_range_exp_5_20)
  12. Volume relativo (rth_logvol_z_20, rth_rvol_5_20)
  13. Relative strength vs benchmark (rth_rs, rth_rs_{N})
  14. Overnight dependency (on_mom_{N}, rth_share_20)
"""

import argparse
import logging
import sys
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
# Costanti
# ---------------------------------------------------------------------------
WINDOWS = [5, 10, 20, 50, 63, 100]


# ---------------------------------------------------------------------------
# Core computation (testable in isolation)
# ---------------------------------------------------------------------------

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcola tutte le feature rolling RTH da un DataFrame di primitive.

    Parameters
    ----------
    df : pd.DataFrame
        Deve contenere le colonne dello schema rth_primitives:
        feature_date, symbol, open, high, low, close, volume,
        rth_loggain, rth_up_loggain, rth_down_loggain, rth_range_log,
        log_volume, overnight_loggain, cc_logret

    Returns
    -------
    pd.DataFrame
        Stesso DataFrame con colonne aggiuntive per ogni feature rolling.
        feature_date e symbol rimangono come colonne (non index).
    """
    df = df.copy()

    # Ordina per symbol + date (fondamentale per rolling by-group)
    df = df.sort_values(["symbol", "feature_date"]).reset_index(drop=True)

    # -----------------------------------------------------------------------
    # Famiglia 13 — Relative strength vs benchmark
    # Calcola prima (richiede aggregazione cross-symbol)
    # -----------------------------------------------------------------------
    logger.debug("Famiglia 13: relative strength vs benchmark")
    benchmark = (
        df.groupby("feature_date")["rth_loggain"]
        .mean()
        .rename("_benchmark_rt")
    )
    df = df.merge(benchmark, on="feature_date", how="left")
    df["rth_rs"] = df["rth_loggain"] - df["_benchmark_rt"]
    df = df.drop(columns=["_benchmark_rt"])

    # -----------------------------------------------------------------------
    # Grouped operations: tutto per simbolo
    # -----------------------------------------------------------------------
    logger.debug("Inizio calcolo feature rolling per simbolo")

    grouped = df.groupby("symbol", sort=False)

    # --- Famiglia 1: Momentum cumulato ---
    logger.debug("Famiglia 1: momentum cumulato")
    for n in WINDOWS:
        df[f"rth_mom_{n}"] = grouped["rth_loggain"].transform(
            lambda s, n=n: s.rolling(n, min_periods=n).sum()
        )

    # --- Famiglia 2: Drift medio ---
    logger.debug("Famiglia 2: drift medio")
    for n in WINDOWS:
        df[f"rth_mean_{n}"] = grouped["rth_loggain"].transform(
            lambda s, n=n: s.rolling(n, min_periods=n).mean()
        )

    # --- Famiglia 3: Drift esponenziale ---
    logger.debug("Famiglia 3: drift esponenziale")
    for span in [5, 20, 50]:
        df[f"rth_ewm_mean_{span}"] = grouped["rth_loggain"].transform(
            lambda s, sp=span: s.ewm(span=sp, adjust=False).mean()
        )

    # --- Famiglia 4: Accelerazione drift (MACD-like) ---
    logger.debug("Famiglia 4: accelerazione drift")
    ewm12 = grouped["rth_loggain"].transform(
        lambda s: s.ewm(span=12, adjust=False).mean()
    )
    ewm26 = grouped["rth_loggain"].transform(
        lambda s: s.ewm(span=26, adjust=False).mean()
    )
    df["rth_drift_accel_12_26"] = ewm12 - ewm26

    # Signal line: EWM(9) della colonna rth_drift_accel_12_26, by group
    df["rth_drift_accel_signal_9"] = df.groupby("symbol")["rth_drift_accel_12_26"].transform(
        lambda s: s.ewm(span=9, adjust=False).mean()
    )
    df["rth_drift_accel_hist"] = df["rth_drift_accel_12_26"] - df["rth_drift_accel_signal_9"]

    # --- Famiglia 5: Volatilità ---
    logger.debug("Famiglia 5: volatilità")
    for n in WINDOWS:
        df[f"rth_std_{n}"] = grouped["rth_loggain"].transform(
            lambda s, n=n: s.rolling(n, min_periods=n).std(ddof=1)
        )

    # --- Famiglia 6: Momentum normalizzato ---
    logger.debug("Famiglia 6: momentum normalizzato")
    for n in WINDOWS:
        mom_col = f"rth_mom_{n}"
        std_col = f"rth_std_{n}"
        denom = df[std_col] * np.sqrt(n)
        df[f"rth_mom_norm_{n}"] = np.where(
            (df[std_col].isna()) | (df[std_col] == 0) | (denom == 0),
            np.nan,
            df[mom_col] / denom,
        )

    # --- Famiglia 7: Efficiency ratio ---
    logger.debug("Famiglia 7: efficiency ratio")
    # Calcola abs(rth_loggain) come serie standalone (non come colonna temp)
    abs_gain_series = df["rth_loggain"].abs()

    for n in WINDOWS:
        net_sum = grouped["rth_loggain"].transform(
            lambda s, n=n: s.rolling(n, min_periods=n).sum()
        )
        # abs_sum: rolling su abs(rth_loggain) per simbolo
        abs_sum = (
            abs_gain_series
            .groupby(df["symbol"], sort=False)
            .transform(lambda s, n=n: s.rolling(n, min_periods=n).sum())
        )
        df[f"rth_eff_{n}"] = np.where(
            abs_sum == 0,
            0.0,
            net_sum.abs() / abs_sum,
        )
        df[f"rth_signed_eff_{n}"] = np.where(
            abs_sum == 0,
            0.0,
            net_sum / abs_sum,
        )

    # --- Famiglia 8: Win rate ---
    logger.debug("Famiglia 8: win rate")
    win_flag_series = (df["rth_loggain"] > 0).astype(float)
    for n in WINDOWS:
        df[f"rth_winrate_{n}"] = (
            win_flag_series
            .groupby(df["symbol"], sort=False)
            .transform(lambda s, n=n: s.rolling(n, min_periods=n).mean())
        )

    # --- Famiglia 9: Pressione H/L ---
    logger.debug("Famiglia 9: pressione H/L")
    abs_down_series = df["rth_down_loggain"].abs()

    for n in WINDOWS:
        df[f"rth_upside_{n}"] = grouped["rth_up_loggain"].transform(
            lambda s, n=n: s.rolling(n, min_periods=n).mean()
        )
        df[f"rth_downside_{n}"] = (
            abs_down_series
            .groupby(df["symbol"], sort=False)
            .transform(lambda s, n=n: s.rolling(n, min_periods=n).mean())
        )
        df[f"rth_pressure_balance_{n}"] = df[f"rth_upside_{n}"] - df[f"rth_downside_{n}"]

    # --- Famiglia 10: Close-to-range ---
    logger.debug("Famiglia 10: close-to-range")
    df["rth_close_to_range"] = np.where(
        df["rth_range_log"].abs() < 1e-8,
        np.nan,
        df["rth_loggain"] / df["rth_range_log"],
    )
    ctr_series = df["rth_close_to_range"]
    for n in [5, 20]:
        df[f"rth_close_to_range_mean_{n}"] = (
            ctr_series
            .groupby(df["symbol"], sort=False)
            .transform(lambda s, n=n: s.rolling(n, min_periods=n).mean())
        )

    # --- Famiglia 11: Range expansion ---
    logger.debug("Famiglia 11: range expansion")
    for n in [5, 20]:
        df[f"rth_range_mean_{n}"] = grouped["rth_range_log"].transform(
            lambda s, n=n: s.rolling(n, min_periods=n).mean()
        )
    df["rth_range_exp_5_20"] = np.where(
        df["rth_range_mean_20"].abs() < 1e-8,
        np.nan,
        df["rth_range_mean_5"] / df["rth_range_mean_20"],
    )

    # --- Famiglia 12: Volume relativo ---
    logger.debug("Famiglia 12: volume relativo")
    vol_mean_20 = grouped["log_volume"].transform(
        lambda s: s.rolling(20, min_periods=20).mean()
    )
    vol_std_20 = grouped["log_volume"].transform(
        lambda s: s.rolling(20, min_periods=20).std(ddof=1)
    )
    df["rth_logvol_z_20"] = np.where(
        (vol_std_20.isna()) | (vol_std_20 == 0),
        np.nan,
        (df["log_volume"] - vol_mean_20) / vol_std_20,
    )

    rvol_mean_5 = grouped["volume"].transform(
        lambda s: s.rolling(5, min_periods=5).mean()
    )
    rvol_mean_20 = grouped["volume"].transform(
        lambda s: s.rolling(20, min_periods=20).mean()
    )
    df["rth_rvol_5_20"] = np.where(
        rvol_mean_20.abs() < 1e-8,
        np.nan,
        rvol_mean_5 / rvol_mean_20,
    )

    # --- Famiglia 13 (continuazione): rolling su rth_rs ---
    logger.debug("Famiglia 13: rolling relative strength")
    for n in [5, 20]:
        df[f"rth_rs_{n}"] = df.groupby("symbol")["rth_rs"].transform(
            lambda s, n=n: s.rolling(n, min_periods=n).sum()
        )

    # --- Famiglia 14: Overnight dependency ---
    logger.debug("Famiglia 14: overnight dependency")
    for n in [5, 20]:
        df[f"on_mom_{n}"] = grouped["overnight_loggain"].transform(
            lambda s, n=n: s.rolling(n, min_periods=n).sum()
        )

    df["rth_share_20"] = df["rth_mom_20"].abs() / (
        df["rth_mom_20"].abs() + df["on_mom_20"].abs() + 1e-10
    )

    logger.debug("Calcolo feature completato")
    return df


# ---------------------------------------------------------------------------
# Statistiche NaN per famiglia
# ---------------------------------------------------------------------------

def _nan_stats(df: pd.DataFrame, original_cols: set) -> dict[str, float]:
    """Calcola NaN ratio per ogni famiglia di feature."""
    families = {
        "mom": [c for c in df.columns if c.startswith("rth_mom_") and not c.startswith("rth_mom_norm")],
        "mean": [c for c in df.columns if c.startswith("rth_mean_")],
        "ewm_mean": [c for c in df.columns if c.startswith("rth_ewm_mean_")],
        "accel": [c for c in df.columns if c.startswith("rth_drift_accel")],
        "std": [c for c in df.columns if c.startswith("rth_std_")],
        "mom_norm": [c for c in df.columns if c.startswith("rth_mom_norm_")],
        "eff": [c for c in df.columns if c.startswith("rth_eff_")],
        "signed_eff": [c for c in df.columns if c.startswith("rth_signed_eff_")],
        "winrate": [c for c in df.columns if c.startswith("rth_winrate_")],
        "pressure": [c for c in df.columns if c.startswith("rth_upside_") or
                     c.startswith("rth_downside_") or c.startswith("rth_pressure_balance_")],
        "close_to_range": [c for c in df.columns if "close_to_range" in c],
        "range_exp": [c for c in df.columns if "range_mean" in c or "range_exp" in c],
        "volume_rel": [c for c in df.columns if c in ("rth_logvol_z_20", "rth_rvol_5_20")],
        "rs": [c for c in df.columns if c.startswith("rth_rs")],
        "overnight": [c for c in df.columns if c.startswith("on_mom_") or c == "rth_share_20"],
    }
    n = len(df)
    stats = {}
    for name, cols in families.items():
        valid_cols = [c for c in cols if c in df.columns]
        if not valid_cols:
            continue
        nan_ratio = df[valid_cols].isna().mean().mean()
        stats[name] = float(nan_ratio)
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    here = Path(__file__).resolve().parent
    out_default = here / "out"
    prim_default = out_default / "rth_primitives.parquet"

    parser = argparse.ArgumentParser(
        description="Calcola le feature rolling RTH da rth_primitives.parquet."
    )
    parser.add_argument(
        "--primitives",
        default=str(prim_default),
        help="Path input parquet (default: bt-strategy-test/RTH_analysis/out/rth_primitives.parquet)",
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

    prim_path = Path(args.primitives)
    if not prim_path.exists():
        logger.error("File primitives non trovato: %s", prim_path)
        sys.exit(1)

    # Carica primitives
    logger.info("Carico: %s", prim_path)
    df = pd.read_parquet(prim_path)
    n_symbols = df["symbol"].nunique()
    n_rows = len(df)
    logger.info("Letto: %d righe, %d simboli", n_rows, n_symbols)

    original_cols = set(df.columns)

    # Calcola feature
    df_feat = compute_features(df)

    new_cols = [c for c in df_feat.columns if c not in original_cols]
    n_features = len(new_cols)

    # Crea output dir e salva
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "rth_features.parquet"

    df_feat.to_parquet(out_path, engine="pyarrow", index=False)

    # NaN stats
    nan_stats = _nan_stats(df_feat, original_cols)

    # Riepilogo
    print(f"\nLetto: rth_primitives.parquet ({n_rows:,} righe, {n_symbols} simboli)")
    print(f"Feature calcolate: {n_features} colonne")
    print(f"Finestre: {WINDOWS}")
    print("NaN ratio per famiglia:")
    for fam, ratio in nan_stats.items():
        print(f"  {fam:20s}: {ratio:.1%}")
    print(f"Salvato: {out_path}")


if __name__ == "__main__":
    main()
