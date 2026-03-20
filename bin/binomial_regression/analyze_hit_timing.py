#!/usr/bin/env python3
"""
Analisi distribuzione tempi di hit TP/SL

Obiettivo: Capire su quali orizzonti temporali le posizioni si chiudono
e identificare quelle "stuck" (che non raggiungono né TP né SL entro N minuti)

Metriche calcolate:
- p_hit_tp_within_Nm: probabilità di raggiungere TP entro N minuti
- p_hit_sl_within_Nm: probabilità di raggiungere SL entro N minuti
- p_no_close_within_Nm: probabilità di non chiudere (né TP né SL) entro N minuti
- median_bars_to_tp: mediana del numero di barre per raggiungere TP
- median_bars_to_sl: mediana del numero di barre per raggiungere SL
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

LOG = logging.getLogger("analyze_hit_timing")

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analisi timing hit TP/SL")
    p.add_argument("--input-glob", default="results/*_feature_outcome_full.csv")
    p.add_argument("--output-dir", default="results/hit_timing_analysis")
    p.add_argument("--minute-data-dir", default="../../config/data/m/alpaca")

    # Filtri entry
    p.add_argument("--slope5-min", type=float, default=0.0004)
    p.add_argument("--slope20-min", type=float, default=0.0002)
    p.add_argument("--slope60-min", type=float, default=0.00015)
    p.add_argument("--volz-min", type=float, default=0.85)
    p.add_argument("--quality-window", type=int, default=60, choices=[5, 20, 60])
    p.add_argument("--quality-min", type=float, default=0.5)

    # TP/SL levels
    p.add_argument("--tp-pct", type=float, default=0.005, help="Take profit % (default 0.5%%)")
    p.add_argument("--sl-pct", type=float, default=0.003, help="Stop loss % (default 0.3%%)")

    # Orizzonti da analizzare (in minuti)
    p.add_argument("--horizons", type=str, default="3,5,10,15,30,60",
                   help="Comma-separated list of horizons in minutes")

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
    out = df.copy()
    need = [f"r2_{window}", f"disp_{window}", f"mse_{window}", f"rmse_{window}", f"mae_{window}"]
    if not all(c in out.columns for c in need):
        raise RuntimeError(f"Colonne mancanti per fit_quality_{window}: {need}")

    r2_rank = pd.to_numeric(out[f"r2_{window}"], errors="coerce").rank(pct=True)
    disp_rank = pd.to_numeric(out[f"disp_{window}"], errors="coerce").rank(pct=True)
    mse_rank = pd.to_numeric(out[f"mse_{window}"], errors="coerce").rank(pct=True)
    rmse_rank = pd.to_numeric(out[f"rmse_{window}"], errors="coerce").rank(pct=True)
    mae_rank = pd.to_numeric(out[f"mae_{window}"], errors="coerce").rank(pct=True)

    out[f"fit_quality_{window}"] = pd.concat(
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

def load_minute_bars_for_timing(asset: str, minute_dir: Path) -> pd.DataFrame:
    """Carica dati minute per calcolare timing preciso."""
    fp = minute_dir / f"{asset}.csv"
    if not fp.exists():
        LOG.warning("File minute mancante per %s: %s", asset, fp)
        return pd.DataFrame()

    df = pd.read_csv(fp, usecols=["timestamp", "high", "low", "close"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)

def calculate_hit_timing(
    entry_timestamp: pd.Timestamp,
    entry_close: float,
    minute_bars: pd.DataFrame,
    tp_pct: float,
    sl_pct: float,
    max_horizon_bars: int,
) -> dict:
    """Calcola timing preciso di hit TP/SL.

    Returns:
        dict con:
        - bars_to_tp: numero di barre per raggiungere TP (np.nan se non raggiunto)
        - bars_to_sl: numero di barre per raggiungere SL (np.nan se non raggiunto)
        - bars_to_first_hit: numero di barre al primo hit (TP o SL)
        - first_hit_type: 'tp', 'sl', 'none'
    """
    # Trova indice della barra di entry
    entry_idx = minute_bars[minute_bars["timestamp"] == entry_timestamp].index
    if len(entry_idx) == 0:
        return {
            "bars_to_tp": np.nan,
            "bars_to_sl": np.nan,
            "bars_to_first_hit": np.nan,
            "first_hit_type": "missing",
        }

    i = entry_idx[0]
    tp_price = entry_close * (1 + tp_pct)
    sl_price = entry_close * (1 - sl_pct)

    bars_to_tp = np.nan
    bars_to_sl = np.nan

    # Scansiona le barre future
    for offset in range(1, max_horizon_bars + 1):
        j = i + offset
        if j >= len(minute_bars):
            break

        bar = minute_bars.iloc[j]
        high_j = bar["high"]
        low_j = bar["low"]

        # Check TP
        if pd.isna(bars_to_tp) and high_j >= tp_price:
            bars_to_tp = offset

        # Check SL
        if pd.isna(bars_to_sl) and low_j <= sl_price:
            bars_to_sl = offset

        # Se entrambi raggiunti, esci
        if pd.notna(bars_to_tp) and pd.notna(bars_to_sl):
            break

    # Determina primo hit
    if pd.isna(bars_to_tp) and pd.isna(bars_to_sl):
        first_hit_type = "none"
        bars_to_first_hit = np.nan
    elif pd.isna(bars_to_sl):
        first_hit_type = "tp"
        bars_to_first_hit = bars_to_tp
    elif pd.isna(bars_to_tp):
        first_hit_type = "sl"
        bars_to_first_hit = bars_to_sl
    else:
        # Entrambi raggiunti, prendi il primo
        if bars_to_tp < bars_to_sl:
            first_hit_type = "tp"
            bars_to_first_hit = bars_to_tp
        elif bars_to_sl < bars_to_tp:
            first_hit_type = "sl"
            bars_to_first_hit = bars_to_sl
        else:
            # Stesso bar (raro ma possibile)
            first_hit_type = "both"
            bars_to_first_hit = bars_to_tp

    return {
        "bars_to_tp": bars_to_tp,
        "bars_to_sl": bars_to_sl,
        "bars_to_first_hit": bars_to_first_hit,
        "first_hit_type": first_hit_type,
    }

def main():
    args = parse_args()
    setup_logging(args.log_level)

    out_dir = resolve(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    horizons = [int(h.strip()) for h in args.horizons.split(",")]
    max_horizon = max(horizons)

    LOG.info("Caricamento dati feature...")
    df = load_full_data(args.input_glob)
    df = add_fit_quality(df, window=args.quality_window)

    LOG.info("Applicazione filtri entry...")
    mask = (
        (pd.to_numeric(df["slope_5"], errors="coerce") > args.slope5_min)
        & (pd.to_numeric(df["slope_20"], errors="coerce") > args.slope20_min)
        & (pd.to_numeric(df["slope_60"], errors="coerce") > args.slope60_min)
        & (pd.to_numeric(df[f"fit_quality_{args.quality_window}"], errors="coerce") >= args.quality_min)
        & (pd.to_numeric(df["volZ"], errors="coerce") > args.volz_min)
    )
    sdf = df[mask].copy()

    LOG.info("Dataset filtrato: %d -> %d righe (%.2f%%)",
             len(df), len(sdf), 100 * len(sdf) / len(df) if len(df) > 0 else 0)

    # Salva metadata
    meta = pd.DataFrame([{
        "n_input": len(df),
        "n_selected": len(sdf),
        "selection_rate": len(sdf) / len(df) if len(df) > 0 else np.nan,
        "tp_pct": args.tp_pct,
        "sl_pct": args.sl_pct,
        "max_horizon_bars": max_horizon,
        "slope_5_min": args.slope5_min,
        "slope_20_min": args.slope20_min,
        "slope_60_min": args.slope60_min,
        "quality_window": args.quality_window,
        "quality_min": args.quality_min,
        "volz_min": args.volz_min,
    }])
    meta.to_csv(out_dir / "analysis_meta.csv", index=False)

    if sdf.empty:
        LOG.warning("Nessuna riga dopo i filtri. Exit.")
        return

    # Calcola timing per ogni entry
    LOG.info("Calcolo timing hit per ogni entry...")
    minute_dir = resolve(args.minute_data_dir)

    timing_rows = []
    for asset, g in sdf.groupby("asset"):
        LOG.info("  Processando asset: %s (%d entries)", asset, len(g))
        minute_bars = load_minute_bars_for_timing(asset, minute_dir)
        if minute_bars.empty:
            LOG.warning("    Saltato (dati minute mancanti)")
            continue

        for idx, row in g.iterrows():
            timing = calculate_hit_timing(
                entry_timestamp=row["timestamp"],
                entry_close=row["close"],
                minute_bars=minute_bars,
                tp_pct=args.tp_pct,
                sl_pct=args.sl_pct,
                max_horizon_bars=max_horizon,
            )
            timing_rows.append({
                "asset": asset,
                "timestamp": row["timestamp"],
                "bars_to_tp": timing["bars_to_tp"],
                "bars_to_sl": timing["bars_to_sl"],
                "bars_to_first_hit": timing["bars_to_first_hit"],
                "first_hit_type": timing["first_hit_type"],
            })

    timing_df = pd.DataFrame(timing_rows)
    timing_df.to_csv(out_dir / "hit_timing_row_level.csv", index=False)
    LOG.info("Timing calcolato per %d entries", len(timing_df))

    # Statistiche aggregate
    stats_rows = []
    for h in horizons:
        hit_tp_within = (timing_df["bars_to_tp"] <= h).sum()
        hit_sl_within = (timing_df["bars_to_sl"] <= h).sum()
        no_close = ((timing_df["bars_to_tp"] > h) | timing_df["bars_to_tp"].isna()) & \
                   ((timing_df["bars_to_sl"] > h) | timing_df["bars_to_sl"].isna())
        no_close_within = no_close.sum()

        stats_rows.append({
            "horizon_bars": h,
            "n_total": len(timing_df),
            "n_hit_tp_within": hit_tp_within,
            "n_hit_sl_within": hit_sl_within,
            "n_no_close_within": no_close_within,
            "p_hit_tp_within": hit_tp_within / len(timing_df) if len(timing_df) > 0 else np.nan,
            "p_hit_sl_within": hit_sl_within / len(timing_df) if len(timing_df) > 0 else np.nan,
            "p_no_close_within": no_close_within / len(timing_df) if len(timing_df) > 0 else np.nan,
        })

    stats_df = pd.DataFrame(stats_rows)
    stats_df.to_csv(out_dir / "hit_timing_summary_by_horizon.csv", index=False)

    # Statistiche globali
    global_stats = {
        "median_bars_to_tp": timing_df["bars_to_tp"].median(),
        "mean_bars_to_tp": timing_df["bars_to_tp"].mean(),
        "median_bars_to_sl": timing_df["bars_to_sl"].median(),
        "mean_bars_to_sl": timing_df["bars_to_sl"].mean(),
        "median_bars_to_first_hit": timing_df["bars_to_first_hit"].median(),
        "mean_bars_to_first_hit": timing_df["bars_to_first_hit"].mean(),
        "p_hit_tp": timing_df["bars_to_tp"].notna().mean(),
        "p_hit_sl": timing_df["bars_to_sl"].notna().mean(),
        "p_no_hit": (timing_df["first_hit_type"] == "none").mean(),
    }

    first_hit_counts = timing_df["first_hit_type"].value_counts()
    for hit_type, count in first_hit_counts.items():
        global_stats[f"count_first_hit_{hit_type}"] = int(count)
        global_stats[f"p_first_hit_{hit_type}"] = count / len(timing_df)

    pd.DataFrame([global_stats]).to_csv(out_dir / "hit_timing_global_stats.csv", index=False)

    LOG.info("\n=== RISULTATI GLOBALI ===")
    LOG.info("Mediana barre a TP:          %.1f", global_stats["median_bars_to_tp"])
    LOG.info("Mediana barre a SL:          %.1f", global_stats["median_bars_to_sl"])
    LOG.info("Mediana barre a primo hit:   %.1f", global_stats["median_bars_to_first_hit"])
    LOG.info("P(hit TP entro max horizon): %.2f%%", 100 * global_stats["p_hit_tp"])
    LOG.info("P(hit SL entro max horizon): %.2f%%", 100 * global_stats["p_hit_sl"])
    LOG.info("P(no hit entro max horizon): %.2f%%", 100 * global_stats["p_no_hit"])

    # Grafici
    sns.set_theme(style="whitegrid")

    # 1) Probabilità hit vs horizon
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(stats_df["horizon_bars"], stats_df["p_hit_tp_within"], marker="o", label="Hit TP", color="#2E7D32")
    ax.plot(stats_df["horizon_bars"], stats_df["p_hit_sl_within"], marker="s", label="Hit SL", color="#B71C1C")
    ax.plot(stats_df["horizon_bars"], stats_df["p_no_close_within"], marker="^", label="No close", color="#616161")
    ax.set_title(f"Probabilità di hit TP/SL entro N bars | TP={args.tp_pct:.1%}, SL={args.sl_pct:.1%}")
    ax.set_xlabel("Orizzonte (bars)")
    ax.set_ylabel("Probabilità")
    ax.set_ylim(0, 1)
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_dir / "hit_prob_by_horizon.png", dpi=150)
    plt.close(fig)

    # 2) Distribuzione bars_to_tp
    fig, ax = plt.subplots(figsize=(10, 5))
    bars_to_tp_valid = timing_df["bars_to_tp"].dropna()
    if len(bars_to_tp_valid) > 0:
        sns.histplot(bars_to_tp_valid, bins=50, kde=True, ax=ax, color="#2E7D32")
        ax.axvline(bars_to_tp_valid.median(), color="red", linestyle="--", label=f"Median: {bars_to_tp_valid.median():.1f}")
        ax.set_title(f"Distribuzione tempo a TP | n={len(bars_to_tp_valid)}")
        ax.set_xlabel("Bars to TP")
        ax.set_ylabel("Count")
        ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "distribution_bars_to_tp.png", dpi=150)
    plt.close(fig)

    # 3) Distribuzione bars_to_sl
    fig, ax = plt.subplots(figsize=(10, 5))
    bars_to_sl_valid = timing_df["bars_to_sl"].dropna()
    if len(bars_to_sl_valid) > 0:
        sns.histplot(bars_to_sl_valid, bins=50, kde=True, ax=ax, color="#B71C1C")
        ax.axvline(bars_to_sl_valid.median(), color="red", linestyle="--", label=f"Median: {bars_to_sl_valid.median():.1f}")
        ax.set_title(f"Distribuzione tempo a SL | n={len(bars_to_sl_valid)}")
        ax.set_xlabel("Bars to SL")
        ax.set_ylabel("Count")
        ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "distribution_bars_to_sl.png", dpi=150)
    plt.close(fig)

    # 4) Distribuzione primo hit
    fig, ax = plt.subplots(figsize=(8, 5))
    first_hit_counts.plot.bar(ax=ax, color=["#2E7D32", "#B71C1C", "#616161", "#FFA726"])
    ax.set_title("Distribuzione tipo primo hit")
    ax.set_xlabel("Tipo")
    ax.set_ylabel("Count")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
    fig.tight_layout()
    fig.savefig(out_dir / "distribution_first_hit_type.png", dpi=150)
    plt.close(fig)

    LOG.info("Analisi completata. Output: %s", out_dir)

if __name__ == "__main__":
    main()
