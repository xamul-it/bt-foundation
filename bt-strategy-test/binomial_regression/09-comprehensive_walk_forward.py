#!/usr/bin/env python3
"""
Walk-forward testing completo su tutti i simboli disponibili.

Testa multiple finestre temporali (30, 60, 90 giorni) e genera report dettagliati.
"""

import argparse
import logging
from pathlib import Path
import re
from typing import List, Dict, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

LOG = logging.getLogger("comprehensive_walk_forward")


def parse_args():
    p = argparse.ArgumentParser(description="Walk-forward test completo multi-asset")
    p.add_argument("--input-glob", default="results/*_feature_outcome_full.csv")
    p.add_argument("--window-days", nargs='+', type=int, default=[30, 60, 90],
                   help="Dimensioni finestre da testare (default: 30 60 90)")
    p.add_argument("--num-windows", type=int, default=10,
                   help="Numero finestre random per dimensione")

    # Filtri da testare
    p.add_argument("--mag-pct", type=int, default=95,
                   help="Percentile magnitude (default: 95 = ultra-selettivo)")
    p.add_argument("--disp-pct", type=int, default=90,
                   help="Percentile disp (default: 90 = ultra-selettivo)")

    # TP/SL
    p.add_argument("--tp-pct", type=float, default=0.005)
    p.add_argument("--sl-pct", type=float, default=0.001)

    p.add_argument("--output-dir", default="results/comprehensive_walk_forward")
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def load_all_symbols(input_glob: str) -> Dict[str, pd.DataFrame]:
    """Carica TUTTI i simboli disponibili."""
    here = Path(__file__).resolve().parent
    files = sorted(here.glob(input_glob))

    if not files:
        raise FileNotFoundError(f"Nessun file trovato: {input_glob}")

    data_by_symbol = {}

    for fp in files:
        m = re.match(r"^([^_]+)_feature_outcome_full\.csv$", fp.name)
        symbol = m.group(1) if m else "UNK"

        df = pd.read_csv(fp)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

        required_cols = ["slope_5", "slope_20", "slope_60", "disp_60",
                        "high_ret_max_5m", "low_ret_min_5m", "close_ret_5m"]

        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            LOG.warning(f"  {symbol}: colonne mancanti {missing}, skip")
            continue

        # Filter out rows with insufficient data
        if len(df) < 1000:
            LOG.warning(f"  {symbol}: troppo poche righe ({len(df)}), skip")
            continue

        data_by_symbol[symbol] = df
        LOG.info(f"  Loaded {symbol}: {len(df)} rows, "
                f"date range: {df['timestamp'].min().date()} to {df['timestamp'].max().date()}")

    return data_by_symbol


def apply_filters(df: pd.DataFrame, mag_pct: int, disp_pct: int) -> pd.Series:
    """Applica filtri con percentili configurabili."""
    # Magnitude filters
    s5_abs = pd.to_numeric(df["slope_5"], errors="coerce").abs()
    s20_abs = pd.to_numeric(df["slope_20"], errors="coerce").abs()
    s60_abs = pd.to_numeric(df["slope_60"], errors="coerce").abs()

    p5 = s5_abs.quantile(mag_pct / 100)
    p20 = s20_abs.quantile(mag_pct / 100)
    p60 = s60_abs.quantile(mag_pct / 100)

    mag_filter = (s5_abs > p5) & (s20_abs > p20) & (s60_abs > p60)

    # Quality filter
    disp_60 = pd.to_numeric(df["disp_60"], errors="coerce")
    disp_threshold = disp_60.quantile(disp_pct / 100)
    quality_filter = disp_60 > disp_threshold

    return mag_filter & quality_filter


def calculate_metrics(df: pd.DataFrame, tp_pct: float, sl_pct: float) -> dict:
    """Calcola metriche con entry su close[i]."""
    high = pd.to_numeric(df["high_ret_max_5m"], errors="coerce")
    low = pd.to_numeric(df["low_ret_min_5m"], errors="coerce")
    close_ret = pd.to_numeric(df["close_ret_5m"], errors="coerce")

    valid = high.notna() & low.notna() & close_ret.notna()
    n = valid.sum()

    if n == 0:
        return {}

    # Priority: TP > SL > neither
    hit_tp_mask = (high[valid] >= tp_pct)
    hit_sl_mask = (low[valid] <= -sl_pct) & ~hit_tp_mask
    hit_neither_mask = ~hit_tp_mask & ~hit_sl_mask

    n_tp = hit_tp_mask.sum()
    n_sl = hit_sl_mask.sum()
    n_neither = hit_neither_mask.sum()

    p_tp = n_tp / n
    p_sl = n_sl / n
    p_neither = n_neither / n

    # Expectancy
    mean_close_neither = close_ret[valid][hit_neither_mask].mean() if n_neither > 0 else 0
    expectancy = p_tp * tp_pct - p_sl * sl_pct + p_neither * mean_close_neither

    return {
        "n": int(n),
        "n_tp": int(n_tp),
        "n_sl": int(n_sl),
        "n_neither": int(n_neither),
        "p_tp": p_tp,
        "p_sl": p_sl,
        "p_neither": p_neither,
        "mean_high": high[valid].mean(),
        "mean_low": low[valid].mean(),
        "expectancy": expectancy,
    }


def generate_random_windows(df: pd.DataFrame, window_days: int, num_windows: int, seed: int) -> List[Tuple]:
    """Genera finestre temporali casuali."""
    np.random.seed(seed)

    df_with_date = df.dropna(subset=["timestamp"])
    min_date = df_with_date["timestamp"].min()
    max_date = df_with_date["timestamp"].max()

    total_days = (max_date - min_date).days

    if total_days < window_days:
        LOG.debug(f"    Dataset troppo corto ({total_days} giorni < {window_days}), uso 1 finestra")
        return [(min_date, max_date)]

    windows = []
    max_start_offset = total_days - window_days

    for i in range(num_windows):
        if max_start_offset <= 0:
            start_offset = 0
        else:
            start_offset = np.random.randint(0, max_start_offset + 1)

        start = min_date + timedelta(days=start_offset)
        end = start + timedelta(days=window_days)

        windows.append((start, end))

    return windows


def main():
    args = parse_args()
    setup_logging(args.log_level)

    out_dir = Path(__file__).resolve().parent / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    LOG.info("="*80)
    LOG.info("COMPREHENSIVE WALK-FORWARD TEST")
    LOG.info("="*80)
    LOG.info(f"Filtri:  Magnitude p{args.mag_pct}, Disp p{args.disp_pct}")
    LOG.info(f"TP/SL:   {args.tp_pct:.2%} / {args.sl_pct:.2%}")
    LOG.info(f"Windows: {args.window_days} giorni, {args.num_windows} random per dimensione")
    LOG.info(f"Seed:    {args.seed}")
    LOG.info("")

    LOG.info("Caricamento simboli...")
    data_by_symbol = load_all_symbols(args.input_glob)

    if not data_by_symbol:
        LOG.error("❌ Nessun simbolo caricato!")
        return

    LOG.info(f"✅ Caricati {len(data_by_symbol)} simboli: {', '.join(sorted(data_by_symbol.keys()))}\n")

    all_results = []

    # Test per ogni simbolo
    for symbol, df_full in data_by_symbol.items():
        LOG.info(f"{'='*80}")
        LOG.info(f"Testing {symbol}")
        LOG.info(f"{'='*80}")

        # Applica filtri sull'intero dataset
        LOG.info(f"  Applicazione filtri (mag=p{args.mag_pct}, disp=p{args.disp_pct})...")
        filter_mask = apply_filters(df_full, args.mag_pct, args.disp_pct)
        df_filtered = df_full[filter_mask].copy()

        pct_kept = 100 * len(df_filtered) / len(df_full) if len(df_full) > 0 else 0
        LOG.info(f"  Candele filtrate: {len(df_filtered)} / {len(df_full)} ({pct_kept:.2f}%)")

        if len(df_filtered) < 100:
            LOG.warning(f"  ⚠️  Poche candele filtrate (<100), skip {symbol}\n")
            continue

        # Test per ogni dimensione finestra
        for window_days in args.window_days:
            LOG.info(f"\n  Testing finestre di {window_days} giorni...")

            # Genera finestre casuali
            windows = generate_random_windows(df_filtered, window_days, args.num_windows, args.seed)

            for i, (start, end) in enumerate(windows, 1):
                window_df = df_filtered[
                    (df_filtered["timestamp"] >= start) &
                    (df_filtered["timestamp"] < end)
                ].copy()

                if len(window_df) < 10:
                    LOG.debug(f"    Window {i}: poche candele ({len(window_df)}), skip")
                    continue

                # Calcola metriche
                metrics = calculate_metrics(window_df, args.tp_pct, args.sl_pct)

                if not metrics:
                    continue

                result = {
                    "symbol": symbol,
                    "window_days": window_days,
                    "window_id": i,
                    "start_date": start.strftime("%Y-%m-%d"),
                    "end_date": end.strftime("%Y-%m-%d"),
                    "n_setups": metrics["n"],
                    "n_tp": metrics["n_tp"],
                    "n_sl": metrics["n_sl"],
                    "n_neither": metrics["n_neither"],
                    "p_tp": metrics["p_tp"],
                    "p_sl": metrics["p_sl"],
                    "p_neither": metrics["p_neither"],
                    "mean_high": metrics["mean_high"],
                    "mean_low": metrics["mean_low"],
                    "expectancy": metrics["expectancy"],
                }

                all_results.append(result)

            LOG.info(f"    Tested {len([r for r in all_results if r['symbol']==symbol and r['window_days']==window_days])} finestre")

        LOG.info("")

    # Salva risultati dettagliati
    if not all_results:
        LOG.error("❌ Nessun risultato generato!")
        return

    results_df = pd.DataFrame(all_results)
    results_df.to_csv(out_dir / "all_windows_results.csv", index=False)

    # ========================================================================
    # REPORT 1: Summary per Simbolo
    # ========================================================================
    LOG.info("="*80)
    LOG.info("REPORT 1: SUMMARY PER SIMBOLO")
    LOG.info("="*80)

    summary_by_symbol = results_df.groupby("symbol").agg({
        "n_setups": "sum",
        "n_tp": "sum",
        "n_sl": "sum",
        "expectancy": ["mean", "std", "min", "max"],
        "p_tp": "mean",
        "p_sl": "mean",
        "window_id": "count",
    }).reset_index()

    summary_by_symbol.columns = [
        "Symbol", "Total Setups", "Total TP", "Total SL",
        "Exp Mean", "Exp Std", "Exp Min", "Exp Max",
        "P(TP) Avg", "P(SL) Avg", "Num Windows"
    ]

    # Calcola P(TP) e P(SL) aggregate
    summary_by_symbol["P(TP) Agg"] = summary_by_symbol["Total TP"] / summary_by_symbol["Total Setups"]
    summary_by_symbol["P(SL) Agg"] = summary_by_symbol["Total SL"] / summary_by_symbol["Total Setups"]

    # Calcola setups/giorno stimati
    summary_by_symbol["Setups/Day"] = summary_by_symbol["Total Setups"] / summary_by_symbol["Num Windows"] / 30  # Assume 30d avg

    # Sort by expectancy
    summary_by_symbol = summary_by_symbol.sort_values("Exp Mean", ascending=False)

    LOG.info("\n" + summary_by_symbol.to_string(index=False))
    summary_by_symbol.to_csv(out_dir / "summary_by_symbol.csv", index=False)

    # ========================================================================
    # REPORT 2: Summary per Window Size
    # ========================================================================
    LOG.info("\n" + "="*80)
    LOG.info("REPORT 2: SUMMARY PER WINDOW SIZE")
    LOG.info("="*80)

    summary_by_window_size = results_df.groupby("window_days").agg({
        "n_setups": ["sum", "mean"],
        "expectancy": ["mean", "std", "min", "max"],
        "p_tp": "mean",
        "p_sl": "mean",
        "window_id": "count",
    }).reset_index()

    summary_by_window_size.columns = [
        "Window Days", "Total Setups", "Avg Setups/Window",
        "Exp Mean", "Exp Std", "Exp Min", "Exp Max",
        "P(TP) Avg", "P(SL) Avg", "Num Windows"
    ]

    LOG.info("\n" + summary_by_window_size.to_string(index=False))
    summary_by_window_size.to_csv(out_dir / "summary_by_window_size.csv", index=False)

    # ========================================================================
    # REPORT 3: Best & Worst Windows
    # ========================================================================
    LOG.info("\n" + "="*80)
    LOG.info("REPORT 3: BEST & WORST WINDOWS")
    LOG.info("="*80)

    # Best 10
    best_windows = results_df.nlargest(10, "expectancy")[
        ["symbol", "window_days", "start_date", "end_date", "n_setups", "p_tp", "expectancy"]
    ]

    LOG.info("\nTOP 10 BEST WINDOWS:")
    LOG.info(best_windows.to_string(index=False))
    best_windows.to_csv(out_dir / "best_windows.csv", index=False)

    # Worst 10
    worst_windows = results_df.nsmallest(10, "expectancy")[
        ["symbol", "window_days", "start_date", "end_date", "n_setups", "p_tp", "expectancy"]
    ]

    LOG.info("\nTOP 10 WORST WINDOWS:")
    LOG.info(worst_windows.to_string(index=False))
    worst_windows.to_csv(out_dir / "worst_windows.csv", index=False)

    # ========================================================================
    # REPORT 4: Overall Statistics
    # ========================================================================
    LOG.info("\n" + "="*80)
    LOG.info("REPORT 4: OVERALL STATISTICS")
    LOG.info("="*80)

    overall = {
        "total_symbols": results_df["symbol"].nunique(),
        "total_windows": len(results_df),
        "total_setups": results_df["n_setups"].sum(),
        "total_tp": results_df["n_tp"].sum(),
        "total_sl": results_df["n_sl"].sum(),

        "p_tp_agg": results_df["n_tp"].sum() / results_df["n_setups"].sum(),
        "p_sl_agg": results_df["n_sl"].sum() / results_df["n_setups"].sum(),

        "exp_mean": results_df["expectancy"].mean(),
        "exp_median": results_df["expectancy"].median(),
        "exp_std": results_df["expectancy"].std(),
        "exp_min": results_df["expectancy"].min(),
        "exp_max": results_df["expectancy"].max(),

        "exp_p25": results_df["expectancy"].quantile(0.25),
        "exp_p75": results_df["expectancy"].quantile(0.75),

        "pct_positive_exp": (results_df["expectancy"] > 0).sum() / len(results_df) * 100,
        "pct_exp_gt_0p01": (results_df["expectancy"] > 0.0001).sum() / len(results_df) * 100,
    }

    LOG.info(f"\nTotal Symbols:         {overall['total_symbols']}")
    LOG.info(f"Total Windows Tested:  {overall['total_windows']}")
    LOG.info(f"Total Setups:          {overall['total_setups']:,}")
    LOG.info("")
    LOG.info("Aggregated Results:")
    LOG.info(f"  P(hit TP):           {overall['p_tp_agg']:.1%}")
    LOG.info(f"  P(hit SL):           {overall['p_sl_agg']:.1%}")
    LOG.info("")
    LOG.info("Expectancy Distribution:")
    LOG.info(f"  Mean:                {overall['exp_mean']:+.4%}")
    LOG.info(f"  Median:              {overall['exp_median']:+.4%}")
    LOG.info(f"  Std Dev:             {overall['exp_std']:.4%}")
    LOG.info(f"  Range:               [{overall['exp_min']:+.4%}, {overall['exp_max']:+.4%}]")
    LOG.info(f"  25th percentile:     {overall['exp_p25']:+.4%}")
    LOG.info(f"  75th percentile:     {overall['exp_p75']:+.4%}")
    LOG.info("")
    LOG.info(f"Windows with Positive Exp:     {overall['pct_positive_exp']:.1f}%")
    LOG.info(f"Windows with Exp > 0.01%:      {overall['pct_exp_gt_0p01']:.1f}%")

    pd.DataFrame([overall]).to_csv(out_dir / "overall_statistics.csv", index=False)

    # ========================================================================
    # REPORT 5: Tabella Finale Riassuntiva
    # ========================================================================
    LOG.info("\n" + "="*80)
    LOG.info("REPORT 5: TABELLA ATTESE PER SIMBOLO E WINDOW SIZE")
    LOG.info("="*80)

    # Pivot table: simbolo × window_size
    pivot = results_df.pivot_table(
        index="symbol",
        columns="window_days",
        values="expectancy",
        aggfunc="mean"
    )

    # Add average across all windows
    pivot["Overall"] = results_df.groupby("symbol")["expectancy"].mean()

    # Add number of setups
    pivot_setups = results_df.pivot_table(
        index="symbol",
        columns="window_days",
        values="n_setups",
        aggfunc="sum"
    )
    pivot_setups["Overall"] = results_df.groupby("symbol")["n_setups"].sum()

    LOG.info("\nEXPECTANCY per SIMBOLO e WINDOW SIZE:")
    LOG.info(pivot.to_string())

    LOG.info("\n\nSETUPS TOTALI per SIMBOLO e WINDOW SIZE:")
    LOG.info(pivot_setups.to_string())

    pivot.to_csv(out_dir / "expectancy_by_symbol_and_window.csv")
    pivot_setups.to_csv(out_dir / "setups_by_symbol_and_window.csv")

    # ========================================================================
    # VERDICT FINALE
    # ========================================================================
    LOG.info("\n" + "="*80)
    LOG.info("VERDICT FINALE")
    LOG.info("="*80)

    if overall["exp_mean"] > 0.001:
        verdict = "✅ STRATEGIA MOLTO PROFITTEVOLE (exp > 0.1%)"
    elif overall["exp_mean"] > 0.0001:
        verdict = "✅ STRATEGIA PROFITTEVOLE (exp > 0.01%)"
    elif overall["exp_mean"] > 0:
        verdict = "⚠️  STRATEGIA MARGINALMENTE PROFITTEVOLE (exp > 0%)"
    else:
        verdict = "❌ STRATEGIA NON PROFITTEVOLE (exp ≤ 0%)"

    LOG.info(f"\n{verdict}")
    LOG.info("")
    LOG.info(f"Setup testato: Magnitude p{args.mag_pct}, Disp p{args.disp_pct}")
    LOG.info(f"Expectancy media: {overall['exp_mean']:+.4%}")
    LOG.info(f"Setups stimati/giorno: {overall['total_setups'] / overall['total_windows'] / 30:.1f}")
    LOG.info(f"Daily expectancy stimata: {overall['exp_mean'] * overall['total_setups'] / overall['total_windows'] / 30:.4%}")
    LOG.info("")

    # Migliori simboli
    top_symbols = summary_by_symbol.head(5)["Symbol"].tolist()
    LOG.info(f"Top 5 simboli (per expectancy): {', '.join(top_symbols)}")

    LOG.info("")
    LOG.info(f"✅ Analisi completata. Output salvato in: {out_dir}")
    LOG.info("")
    LOG.info("Files generati:")
    LOG.info("  - all_windows_results.csv         (dettaglio completo)")
    LOG.info("  - summary_by_symbol.csv           (aggregato per simbolo)")
    LOG.info("  - summary_by_window_size.csv      (aggregato per dimensione finestra)")
    LOG.info("  - best_windows.csv                (top 10 finestre)")
    LOG.info("  - worst_windows.csv               (worst 10 finestre)")
    LOG.info("  - overall_statistics.csv          (statistiche complessive)")
    LOG.info("  - expectancy_by_symbol_and_window.csv  (pivot table)")


if __name__ == "__main__":
    main()
