#!/usr/bin/env python3
"""
Analisi filtri basati su parametri di qualità fit (disp, rmse, r2).

Testa se aggiungere filtri su:
- disp (dispersione residui) ALTO → volatilità alta
- rmse (root mean squared error) ALTO → volatilità alta
- r2 BASSO → cattivo fit lineare, movimenti non lineari

Questi filtri sono COMPLEMENTARI al filtro magnitudine slope.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import re
from typing import List

import numpy as np
import pandas as pd

LOG = logging.getLogger("quality_filter_analysis")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Test filtri qualità fit (disp, rmse, r2)")
    p.add_argument("--input-glob", default="results/*_feature_outcome_full.csv")
    p.add_argument("--output-dir", default="results/quality_filters")

    # Filtro base (magnitudine)
    p.add_argument("--use-mag-filter", action="store_true",
                   help="Applica prima filtro magnitudine slope (p75)")

    # Finestra per metriche qualità
    p.add_argument("--window", type=int, default=60, choices=[5, 20, 60],
                   help="Finestra per disp/rmse/r2 (default: 60)")

    # Soglie qualità (percentili)
    p.add_argument("--disp-percentile", type=float, default=75,
                   help="Percentile per disp (alta dispersione = alta volatilità)")
    p.add_argument("--rmse-percentile", type=float, default=75,
                   help="Percentile per rmse")
    p.add_argument("--r2-percentile", type=float, default=25,
                   help="Percentile per r2 (BASSO r2 = cattivo fit lineare)")

    # TP/SL
    p.add_argument("--tp-pct", type=float, default=0.005)
    p.add_argument("--sl-pct", type=float, default=0.003)

    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def resolve(path_like: str) -> Path:
    p = Path(path_like)
    if p.is_absolute():
        return p
    return (Path(__file__).resolve().parent / path_like).resolve()


def load_full_data(input_glob: str) -> pd.DataFrame:
    here = Path(__file__).resolve().parent
    files = sorted(here.glob(input_glob))
    if not files:
        raise FileNotFoundError(f"Nessun file trovato con glob: {input_glob}")

    parts: List[pd.DataFrame] = []
    for fp in files:
        m = re.match(r"^([^_]+)_feature_outcome_full\.csv$", fp.name)
        asset = m.group(1) if m else "UNK"
        df = pd.read_csv(fp)
        df["asset"] = asset
        parts.append(df)

    out = pd.concat(parts, ignore_index=True)
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
    return out


def calculate_metrics(df: pd.DataFrame, tp_pct: float, sl_pct: float) -> dict:
    """Calcola metriche aggregate."""
    high = pd.to_numeric(df["high_ret_max_5m"], errors="coerce")
    low = pd.to_numeric(df["low_ret_min_5m"], errors="coerce")

    valid = high.notna() & low.notna()
    n = valid.sum()

    if n == 0:
        return {}

    hit_tp = (high[valid] >= tp_pct).sum()
    hit_sl = (low[valid] <= -sl_pct).sum()

    return {
        "n": int(n),
        "p_hit_tp": hit_tp / n if n > 0 else np.nan,
        "p_hit_sl": hit_sl / n if n > 0 else np.nan,
        "mean_high": high[valid].mean(),
        "median_high": high[valid].median(),
        "mean_low": low[valid].mean(),
        "median_low": low[valid].median(),
    }


def main():
    args = parse_args()
    setup_logging(args.log_level)

    out_dir = resolve(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    LOG.info("Caricamento dati...")
    df = load_full_data(args.input_glob)
    LOG.info("  Caricati %d righe totali", len(df))

    # Baseline
    metrics_baseline = calculate_metrics(df, args.tp_pct, args.sl_pct)

    # Filtro magnitudine (opzionale)
    if args.use_mag_filter:
        LOG.info("\nApplicazione filtro magnitudine slope (p75)...")
        s5_abs = pd.to_numeric(df["slope_5"], errors="coerce").abs()
        s20_abs = pd.to_numeric(df["slope_20"], errors="coerce").abs()
        s60_abs = pd.to_numeric(df["slope_60"], errors="coerce").abs()

        p5 = s5_abs.quantile(0.75)
        p20 = s20_abs.quantile(0.75)
        p60 = s60_abs.quantile(0.75)

        mag_mask = (s5_abs > p5) & (s20_abs > p20) & (s60_abs > p60)
        df_mag = df[mag_mask].copy()
        LOG.info("  Righe dopo mag filter: %d (%.1f%%)", len(df_mag), 100*len(df_mag)/len(df))
        metrics_mag = calculate_metrics(df_mag, args.tp_pct, args.sl_pct)
    else:
        df_mag = df
        metrics_mag = metrics_baseline

    # Prepara metriche qualità
    win = args.window
    disp_col = f"disp_{win}"
    rmse_col = f"rmse_{win}"
    r2_col = f"r2_{win}"

    for col in [disp_col, rmse_col, r2_col]:
        if col not in df_mag.columns:
            LOG.error("Colonna %s mancante! Exit.", col)
            return

    disp = pd.to_numeric(df_mag[disp_col], errors="coerce")
    rmse = pd.to_numeric(df_mag[rmse_col], errors="coerce")
    r2 = pd.to_numeric(df_mag[r2_col], errors="coerce")

    # Calcola soglie
    disp_threshold = disp.quantile(args.disp_percentile / 100.0)
    rmse_threshold = rmse.quantile(args.rmse_percentile / 100.0)
    r2_threshold = r2.quantile(args.r2_percentile / 100.0)  # BASSO r2

    LOG.info("\nSoglie calcolate (finestra=%d):", win)
    LOG.info("  disp p%.0f: %.6f (alta dispersione = alta volatilità)", args.disp_percentile, disp_threshold)
    LOG.info("  rmse p%.0f: %.6f (alto errore = alta volatilità)", args.rmse_percentile, rmse_threshold)
    LOG.info("  r2 p%.0f: %.6f (BASSO r2 = cattivo fit lineare)", args.r2_percentile, r2_threshold)

    # Test filtri
    filters = {
        "Baseline": pd.Series(True, index=df_mag.index),
        f"disp_{win} > p{args.disp_percentile}": disp > disp_threshold,
        f"rmse_{win} > p{args.rmse_percentile}": rmse > rmse_threshold,
        f"r2_{win} < p{args.r2_percentile}": r2 < r2_threshold,
        f"disp_{win} > p{args.disp_percentile} + rmse_{win} > p{args.rmse_percentile}": (
            (disp > disp_threshold) & (rmse > rmse_threshold)
        ),
        f"disp_{win} > p{args.disp_percentile} + r2_{win} < p{args.r2_percentile}": (
            (disp > disp_threshold) & (r2 < r2_threshold)
        ),
        f"All 3 (disp + rmse + r2)": (
            (disp > disp_threshold) & (rmse > rmse_threshold) & (r2 < r2_threshold)
        ),
    }

    results = []
    for filter_name, mask in filters.items():
        filtered = df_mag[mask]
        metrics = calculate_metrics(filtered, args.tp_pct, args.sl_pct)

        if not metrics:
            continue

        pct_kept = 100 * metrics["n"] / len(df_mag) if len(df_mag) > 0 else 0
        lift_tp = metrics["p_hit_tp"] / metrics_mag["p_hit_tp"] if metrics_mag["p_hit_tp"] > 0 else np.nan
        lift_high = metrics["mean_high"] / metrics_mag["mean_high"] if metrics_mag["mean_high"] > 0 else np.nan

        results.append({
            "Filter": filter_name,
            "N candele": metrics["n"],
            "% kept": pct_kept,
            "P(hit TP)": metrics["p_hit_tp"],
            "Mean high": metrics["mean_high"],
            "Lift P(TP)": lift_tp,
            "Lift Mean high": lift_high,
        })

    res_df = pd.DataFrame(results).sort_values("Lift P(TP)", ascending=False)
    res_df.to_csv(out_dir / "quality_filter_comparison.csv", index=False)

    LOG.info("\n" + "="*80)
    LOG.info("RISULTATI CONFRONTO FILTRI QUALITÀ")
    LOG.info("="*80)
    LOG.info("\nMagnitudine filter applicato: %s", "SÌ" if args.use_mag_filter else "NO")
    LOG.info("\n%s", res_df.to_string(index=False))

    LOG.info("\n" + "="*80)
    LOG.info("TOP 3 FILTRI")
    LOG.info("="*80)
    for i, (_, row) in enumerate(res_df.head(3).iterrows(), 1):
        LOG.info("\n#%d: %s", i, row["Filter"])
        LOG.info("    N candele:    %d (%.1f%%)", int(row["N candele"]), row["% kept"])
        LOG.info("    P(hit TP):    %.1f%%", row["P(hit TP)"]*100)
        LOG.info("    Mean high:    %.3f%%", row["Mean high"]*100)
        LOG.info("    Lift P(TP):   %.2fx", row["Lift P(TP)"])

    LOG.info("\n✅ Analisi completata. Output: %s", out_dir)


if __name__ == "__main__":
    main()
