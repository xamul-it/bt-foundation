#!/usr/bin/env python3
"""
Analisi strategia con filtro Triple Magnitudine.

Applica il filtro migliore identificato (abs(slope_5/20/60) > p75)
e calcola metriche dettagliate per valutare l'efficacia.

Output:
- Metriche di performance (win rate, mean outcome, ecc.)
- Distribuzione timing hit TP/SL
- Confronto con baseline (nessun filtro)
- Grafici e report
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import re
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

LOG = logging.getLogger("filtered_strategy_analysis")

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analisi strategia con filtro magnitudine slope")
    p.add_argument("--input-glob", default="results/*_feature_outcome_full.csv")
    p.add_argument("--output-dir", default="results/filtered_strategy")

    # Filtri triple magnitudine (usa percentili)
    p.add_argument("--slope5-percentile", type=float, default=75,
                   help="Percentile per abs(slope_5) (default: 75)")
    p.add_argument("--slope20-percentile", type=float, default=75,
                   help="Percentile per abs(slope_20) (default: 75)")
    p.add_argument("--slope60-percentile", type=float, default=75,
                   help="Percentile per abs(slope_60) (default: 75)")

    # Filtri opzionali aggiuntivi
    p.add_argument("--use-alignment", action="store_true",
                   help="Aggiungi filtro trend alignment (slope stessa direzione)")
    p.add_argument("--volz-min", type=float, default=None,
                   help="Filtro volZ minimo (opzionale)")
    p.add_argument("--quality-min", type=float, default=None,
                   help="Filtro fit_quality minimo (opzionale, richiede --quality-window)")
    p.add_argument("--quality-window", type=int, default=60, choices=[5, 20, 60])

    # TP/SL per metriche
    p.add_argument("--tp-pct", type=float, default=0.005, help="Take profit %")
    p.add_argument("--sl-pct", type=float, default=0.003, help="Stop loss %")

    # Orizzonti analisi
    p.add_argument("--horizons", type=str, default="5,10,15",
                   help="Orizzonti in minuti (comma-separated)")

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


def add_fit_quality(df: pd.DataFrame, window: int) -> pd.DataFrame:
    """Aggiunge fit_quality se necessario."""
    col = f"fit_quality_{window}"
    if col in df.columns:
        return df

    out = df.copy()
    need = [f"r2_{window}", f"disp_{window}", f"mse_{window}", f"rmse_{window}", f"mae_{window}"]
    if not all(c in out.columns for c in need):
        LOG.warning("Cannot compute fit_quality_%d: missing columns", window)
        return out

    r2_rank = pd.to_numeric(out[f"r2_{window}"], errors="coerce").rank(pct=True)
    disp_rank = pd.to_numeric(out[f"disp_{window}"], errors="coerce").rank(pct=True)
    mse_rank = pd.to_numeric(out[f"mse_{window}"], errors="coerce").rank(pct=True)
    rmse_rank = pd.to_numeric(out[f"rmse_{window}"], errors="coerce").rank(pct=True)
    mae_rank = pd.to_numeric(out[f"mae_{window}"], errors="coerce").rank(pct=True)

    out[col] = pd.concat(
        [
            r2_rank.rename("r2_good"),
            (1 - disp_rank).rename("disp_good"),
            (1 - mse_rank).rename("mse_good"),
            (1 - rmse_rank).rename("rmse_good"),
            (1 - mae_rank).rename("mae_good"),
        ],
        axis=1,
    ).mean(axis=1, skipna=False)
    return out


def apply_filters(df: pd.DataFrame, args: argparse.Namespace) -> tuple[pd.DataFrame, dict]:
    """Applica filtri e restituisce dataset filtrato + metadata."""

    # Calcola percentili
    slope5_abs = pd.to_numeric(df["slope_5"], errors="coerce").abs()
    slope20_abs = pd.to_numeric(df["slope_20"], errors="coerce").abs()
    slope60_abs = pd.to_numeric(df["slope_60"], errors="coerce").abs()

    p5_threshold = slope5_abs.quantile(args.slope5_percentile / 100.0)
    p20_threshold = slope20_abs.quantile(args.slope20_percentile / 100.0)
    p60_threshold = slope60_abs.quantile(args.slope60_percentile / 100.0)

    LOG.info("Soglie percentili calcolate:")
    LOG.info("  slope_5  p%.0f: %.6f", args.slope5_percentile, p5_threshold)
    LOG.info("  slope_20 p%.0f: %.6f", args.slope20_percentile, p20_threshold)
    LOG.info("  slope_60 p%.0f: %.6f", args.slope60_percentile, p60_threshold)

    # Filtro triple magnitudine
    mask = (
        (slope5_abs > p5_threshold) &
        (slope20_abs > p20_threshold) &
        (slope60_abs > p60_threshold)
    )

    filter_desc = f"abs(slope_5)>p{args.slope5_percentile} + abs(slope_20)>p{args.slope20_percentile} + abs(slope_60)>p{args.slope60_percentile}"

    # Filtri opzionali
    if args.use_alignment:
        s5 = np.sign(pd.to_numeric(df["slope_5"], errors="coerce"))
        s20 = np.sign(pd.to_numeric(df["slope_20"], errors="coerce"))
        s60 = np.sign(pd.to_numeric(df["slope_60"], errors="coerce"))
        alignment = (s5 == s20) & (s20 == s60) & (s5 != 0)
        mask &= alignment
        filter_desc += " + trend_alignment"
        LOG.info("  Trend alignment aggiunto al filtro")

    if args.volz_min is not None:
        volz_mask = pd.to_numeric(df["volZ"], errors="coerce") > args.volz_min
        mask &= volz_mask
        filter_desc += f" + volZ>{args.volz_min}"
        LOG.info("  volZ > %.2f aggiunto al filtro", args.volz_min)

    if args.quality_min is not None:
        df = add_fit_quality(df, args.quality_window)
        quality_col = f"fit_quality_{args.quality_window}"
        quality_mask = pd.to_numeric(df[quality_col], errors="coerce") >= args.quality_min
        mask &= quality_mask
        filter_desc += f" + fit_quality_{args.quality_window}>={args.quality_min}"
        LOG.info("  fit_quality_%d >= %.2f aggiunto al filtro", args.quality_window, args.quality_min)

    filtered_df = df[mask].copy()

    metadata = {
        "n_input": len(df),
        "n_filtered": len(filtered_df),
        "pct_kept": 100.0 * len(filtered_df) / len(df) if len(df) > 0 else 0.0,
        "filter_description": filter_desc,
        "slope5_threshold": p5_threshold,
        "slope20_threshold": p20_threshold,
        "slope60_threshold": p60_threshold,
        "use_alignment": args.use_alignment,
        "volz_min": args.volz_min,
        "quality_min": args.quality_min,
        "quality_window": args.quality_window if args.quality_min else None,
    }

    return filtered_df, metadata


def calculate_metrics(df: pd.DataFrame, horizons: List[int], tp_pct: float, sl_pct: float) -> pd.DataFrame:
    """Calcola metriche di performance per ogni orizzonte."""
    rows = []

    for h in horizons:
        high_col = f"high_ret_max_{h}m"
        low_col = f"low_ret_min_{h}m"
        close_col = f"close_ret_{h}m"

        if high_col not in df.columns or low_col not in df.columns:
            continue

        high = pd.to_numeric(df[high_col], errors="coerce")
        low = pd.to_numeric(df[low_col], errors="coerce")
        close = pd.to_numeric(df[close_col], errors="coerce")

        valid = high.notna() & low.notna() & close.notna()
        n = valid.sum()

        if n == 0:
            continue

        # TP/SL hits
        hit_tp = (high[valid] >= tp_pct).sum()
        hit_sl = (low[valid] <= -sl_pct).sum()
        hit_neither = ((high[valid] < tp_pct) & (low[valid] > -sl_pct)).sum()

        # Metriche outcome
        mean_high = high[valid].mean()
        median_high = high[valid].median()
        mean_low = low[valid].mean()
        median_low = low[valid].median()
        mean_close = close[valid].mean()
        median_close = close[valid].median()

        rows.append({
            "horizon_m": h,
            "n": int(n),
            "n_hit_tp": int(hit_tp),
            "n_hit_sl": int(hit_sl),
            "n_hit_neither": int(hit_neither),
            "p_hit_tp": hit_tp / n if n > 0 else np.nan,
            "p_hit_sl": hit_sl / n if n > 0 else np.nan,
            "p_hit_neither": hit_neither / n if n > 0 else np.nan,
            "mean_high": mean_high,
            "median_high": median_high,
            "mean_low": mean_low,
            "median_low": median_low,
            "mean_close": mean_close,
            "median_close": median_close,
        })

    return pd.DataFrame(rows)


def plot_metrics(metrics_filtered: pd.DataFrame, metrics_baseline: pd.DataFrame, out_dir: Path, tp_pct: float, sl_pct: float):
    """Genera grafici comparativi."""
    sns.set_theme(style="whitegrid")

    # 1) Probabilità hit TP
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(metrics_baseline["horizon_m"], metrics_baseline["p_hit_tp"] * 100,
            marker="o", label="Baseline (nessun filtro)", linewidth=2, color="#CCCCCC")
    ax.plot(metrics_filtered["horizon_m"], metrics_filtered["p_hit_tp"] * 100,
            marker="s", label="Filtered (triple mag)", linewidth=2.5, color="#2E7D32")
    ax.set_title(f"Probabilità Hit TP ({tp_pct:.1%}) per Orizzonte", fontsize=14, fontweight="bold")
    ax.set_xlabel("Orizzonte (minuti)", fontsize=12)
    ax.set_ylabel("P(hit TP) %", fontsize=12)
    ax.legend(fontsize=11)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_dir / "p_hit_tp_comparison.png", dpi=150)
    plt.close(fig)

    # 2) Probabilità hit SL
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(metrics_baseline["horizon_m"], metrics_baseline["p_hit_sl"] * 100,
            marker="o", label="Baseline", linewidth=2, color="#CCCCCC")
    ax.plot(metrics_filtered["horizon_m"], metrics_filtered["p_hit_sl"] * 100,
            marker="s", label="Filtered", linewidth=2.5, color="#B71C1C")
    ax.set_title(f"Probabilità Hit SL ({sl_pct:.1%}) per Orizzonte", fontsize=14, fontweight="bold")
    ax.set_xlabel("Orizzonte (minuti)", fontsize=12)
    ax.set_ylabel("P(hit SL) %", fontsize=12)
    ax.legend(fontsize=11)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_dir / "p_hit_sl_comparison.png", dpi=150)
    plt.close(fig)

    # 3) Mean high
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(metrics_baseline["horizon_m"], metrics_baseline["mean_high"] * 100,
            marker="o", label="Baseline", linewidth=2, color="#CCCCCC")
    ax.plot(metrics_filtered["horizon_m"], metrics_filtered["mean_high"] * 100,
            marker="s", label="Filtered", linewidth=2.5, color="#1976D2")
    ax.set_title("Mean High Return per Orizzonte", fontsize=14, fontweight="bold")
    ax.set_xlabel("Orizzonte (minuti)", fontsize=12)
    ax.set_ylabel("Mean High (%)", fontsize=12)
    ax.legend(fontsize=11)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_dir / "mean_high_comparison.png", dpi=150)
    plt.close(fig)

    # 4) Lift (Filtered / Baseline)
    merged = metrics_filtered.merge(
        metrics_baseline,
        on="horizon_m",
        suffixes=("_filt", "_base")
    )
    merged["lift_p_hit_tp"] = merged["p_hit_tp_filt"] / merged["p_hit_tp_base"]
    merged["lift_mean_high"] = merged["mean_high_filt"] / merged["mean_high_base"]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(merged["horizon_m"], merged["lift_p_hit_tp"],
            marker="o", label="Lift P(hit TP)", linewidth=2.5, color="#2E7D32")
    ax.plot(merged["horizon_m"], merged["lift_mean_high"],
            marker="s", label="Lift Mean High", linewidth=2.5, color="#1976D2")
    ax.axhline(1.0, color="black", linestyle="--", alpha=0.5, label="Baseline (1.0x)")
    ax.set_title("Lift: Filtered vs Baseline", fontsize=14, fontweight="bold")
    ax.set_xlabel("Orizzonte (minuti)", fontsize=12)
    ax.set_ylabel("Lift (ratio)", fontsize=12)
    ax.legend(fontsize=11)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_dir / "lift_comparison.png", dpi=150)
    plt.close(fig)

    LOG.info("✅ Grafici salvati in: %s", out_dir)


def main():
    args = parse_args()
    setup_logging(args.log_level)

    out_dir = resolve(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    horizons = [int(h.strip()) for h in args.horizons.split(",")]

    LOG.info("Caricamento dati...")
    df = load_full_data(args.input_glob)
    LOG.info("  Caricati %d righe totali", len(df))

    LOG.info("\nApplicazione filtri...")
    filtered_df, metadata = apply_filters(df, args)
    LOG.info("  Righe dopo filtro: %d (%.1f%% del totale)",
             metadata["n_filtered"], metadata["pct_kept"])

    # Salva metadata
    pd.DataFrame([metadata]).to_csv(out_dir / "filter_metadata.csv", index=False)

    if filtered_df.empty:
        LOG.error("Nessuna riga dopo filtro! Exit.")
        return

    # Calcola metriche filtrate
    LOG.info("\nCalcolo metriche (dataset filtrato)...")
    metrics_filtered = calculate_metrics(filtered_df, horizons, args.tp_pct, args.sl_pct)
    metrics_filtered.to_csv(out_dir / "metrics_filtered.csv", index=False)

    # Calcola metriche baseline (nessun filtro)
    LOG.info("Calcolo metriche (baseline, nessun filtro)...")
    metrics_baseline = calculate_metrics(df, horizons, args.tp_pct, args.sl_pct)
    metrics_baseline.to_csv(out_dir / "metrics_baseline.csv", index=False)

    # Genera report testuale
    LOG.info("\n" + "="*80)
    LOG.info("REPORT CONFRONTO")
    LOG.info("="*80)
    LOG.info("\nFiltro applicato: %s", metadata["filter_description"])
    LOG.info("\nRighe:")
    LOG.info("  Input:    %d", metadata["n_input"])
    LOG.info("  Filtered: %d (%.1f%%)", metadata["n_filtered"], metadata["pct_kept"])

    LOG.info("\nMetriche @ 5 minuti:")
    if not metrics_filtered.empty and not metrics_baseline.empty:
        row_f = metrics_filtered[metrics_filtered["horizon_m"] == 5].iloc[0] if 5 in metrics_filtered["horizon_m"].values else None
        row_b = metrics_baseline[metrics_baseline["horizon_m"] == 5].iloc[0] if 5 in metrics_baseline["horizon_m"].values else None

        if row_f is not None and row_b is not None:
            LOG.info("  P(hit TP):")
            LOG.info("    Baseline: %.1f%%", row_b["p_hit_tp"] * 100)
            LOG.info("    Filtered: %.1f%%", row_f["p_hit_tp"] * 100)
            LOG.info("    Lift:     %.2fx", row_f["p_hit_tp"] / row_b["p_hit_tp"] if row_b["p_hit_tp"] > 0 else np.nan)

            LOG.info("  Mean High:")
            LOG.info("    Baseline: %.3f%%", row_b["mean_high"] * 100)
            LOG.info("    Filtered: %.3f%%", row_f["mean_high"] * 100)
            LOG.info("    Lift:     %.2fx", row_f["mean_high"] / row_b["mean_high"] if row_b["mean_high"] > 0 else np.nan)

            LOG.info("  P(hit SL):")
            LOG.info("    Baseline: %.1f%%", row_b["p_hit_sl"] * 100)
            LOG.info("    Filtered: %.1f%%", row_f["p_hit_sl"] * 100)

    # Genera grafici
    LOG.info("\nGenerazione grafici...")
    plot_metrics(metrics_filtered, metrics_baseline, out_dir, args.tp_pct, args.sl_pct)

    LOG.info("\n" + "="*80)
    LOG.info("✅ Analisi completata!")
    LOG.info("Output salvati in: %s", out_dir)
    LOG.info("="*80)


if __name__ == "__main__":
    main()
