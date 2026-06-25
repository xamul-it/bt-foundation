#!/usr/bin/env python3
"""
Walk-forward testing su finestre temporali casuali.
Testa strategia con entry su close della barra segnale.
"""

import argparse
import logging
from pathlib import Path
import re
from typing import List, Dict, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

LOG = logging.getLogger("walk_forward_test")


def parse_args():
    p = argparse.ArgumentParser(description="Walk-forward test con entry su close vs open")
    p.add_argument("--symbols", default="all",
                   help="Simboli da testare (comma-separated o 'all')")
    p.add_argument("--input-glob", default="results/*_feature_outcome_full.csv")
    p.add_argument("--window-days", type=int, default=30,
                   help="Giorni per finestra temporale")
    p.add_argument("--num-windows", type=int, default=10,
                   help="Numero finestre random da testare")
    p.add_argument("--tp-pct", type=float, default=0.005,
                   help="Take profit (default: 0.5%%)")
    p.add_argument("--sl-pct", type=float, default=0.001,
                   help="Stop loss (default: 0.1%%)")
    p.add_argument("--output-dir", default="results/walk_forward")
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--seed", type=int, default=42, help="Random seed")
    return p.parse_args()


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def resolve(path_like: str) -> Path:
    p = Path(path_like)
    if p.is_absolute():
        return p
    return (Path(__file__).resolve().parent / path_like).resolve()


def load_data(input_glob: str, symbols: str) -> Dict[str, pd.DataFrame]:
    """Carica dati per simboli specificati."""
    here = Path(__file__).resolve().parent
    files = sorted(here.glob(input_glob))

    if not files:
        raise FileNotFoundError(f"Nessun file trovato: {input_glob}")

    data_by_symbol = {}

    for fp in files:
        m = re.match(r"^([^_]+)_feature_outcome_full\.csv$", fp.name)
        symbol = m.group(1) if m else "UNK"

        # Filter by requested symbols
        if symbols != "all":
            requested = [s.strip().upper() for s in symbols.split(",")]
            if symbol.upper() not in requested:
                continue

        df = pd.read_csv(fp)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

        # Verifica colonne necessarie
        required_cols = ["slope_5", "slope_20", "slope_60", "disp_60",
                        "high_ret_max_5m", "low_ret_min_5m", "close_ret_5m"]

        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            LOG.warning(f"  {symbol}: colonne mancanti {missing}, skip")
            continue

        data_by_symbol[symbol] = df
        LOG.info(f"  Loaded {symbol}: {len(df)} rows")

    return data_by_symbol


def apply_filters_setup2(df: pd.DataFrame) -> pd.Series:
    """
    Applica filtri Setup 2: magnitude + disp.

    Returns:
        Boolean Series indicando quali righe passano il filtro
    """
    # Magnitude filters (p75 su abs di tutti gli slope)
    s5_abs = pd.to_numeric(df["slope_5"], errors="coerce").abs()
    s20_abs = pd.to_numeric(df["slope_20"], errors="coerce").abs()
    s60_abs = pd.to_numeric(df["slope_60"], errors="coerce").abs()

    p5 = s5_abs.quantile(0.75)
    p20 = s20_abs.quantile(0.75)
    p60 = s60_abs.quantile(0.75)

    mag_filter = (s5_abs > p5) & (s20_abs > p20) & (s60_abs > p60)

    # Quality filter (disp_60 > p75)
    disp_60 = pd.to_numeric(df["disp_60"], errors="coerce")
    disp_threshold = disp_60.quantile(0.75)
    quality_filter = disp_60 > disp_threshold

    return mag_filter & quality_filter


def calculate_metrics_close_entry(df: pd.DataFrame, tp_pct: float, sl_pct: float) -> dict:
    """
    Calcola metriche assumendo entry = close[i].

    I forward outcomes (high_ret_max_5m, etc.) sono già calcolati
    assumendo entry = close[i], quindi usiamo direttamente quelli.
    """
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
        "mean_close_neither": mean_close_neither,
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
        LOG.warning(f"  Dataset troppo corto ({total_days} giorni < {window_days}), uso 1 finestra completa")
        return [(min_date, max_date)]

    windows = []
    max_start_offset = total_days - window_days

    # Generate random windows
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

    out_dir = resolve(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    LOG.info("="*80)
    LOG.info("WALK-FORWARD TEST: Entry su Close")
    LOG.info("="*80)
    LOG.info(f"TP: {args.tp_pct:.2%}, SL: {args.sl_pct:.2%}")
    LOG.info(f"Window: {args.window_days} giorni, Num windows: {args.num_windows}")
    LOG.info(f"Random seed: {args.seed}")
    LOG.info("")

    LOG.info("Caricamento dati...")
    data_by_symbol = load_data(args.input_glob, args.symbols)

    if not data_by_symbol:
        LOG.error("❌ Nessun simbolo caricato!")
        return

    LOG.info(f"✅ Caricati {len(data_by_symbol)} simboli\n")

    all_results = []

    for symbol, df_full in data_by_symbol.items():
        LOG.info(f"{'='*80}")
        LOG.info(f"Testing {symbol}")
        LOG.info(f"{'='*80}")

        # Applica filtri Setup 2 sull'intero dataset
        LOG.info("Applicazione filtri Setup 2 (magnitude + disp)...")
        filter_mask = apply_filters_setup2(df_full)
        df_filtered = df_full[filter_mask].copy()

        pct_kept = 100 * len(df_filtered) / len(df_full) if len(df_full) > 0 else 0
        LOG.info(f"  Candele filtrate: {len(df_filtered)} / {len(df_full)} ({pct_kept:.2f}%)")

        if len(df_filtered) < 100:
            LOG.warning(f"  ⚠️  Poche candele filtrate (<100), skip {symbol}\n")
            continue

        # Genera finestre temporali casuali
        windows = generate_random_windows(df_filtered, args.window_days, args.num_windows, args.seed)

        LOG.info(f"  Testing su {len(windows)} finestre di {args.window_days} giorni\n")

        for i, (start, end) in enumerate(windows, 1):
            window_df = df_filtered[
                (df_filtered["timestamp"] >= start) &
                (df_filtered["timestamp"] < end)
            ].copy()

            if len(window_df) < 10:
                LOG.warning(f"  Window {i}: poche candele ({len(window_df)}), skip")
                continue

            # Test entry su close
            metrics = calculate_metrics_close_entry(window_df, args.tp_pct, args.sl_pct)

            if not metrics:
                continue

            result = {
                "symbol": symbol,
                "window": i,
                "start_date": start.strftime("%Y-%m-%d"),
                "end_date": end.strftime("%Y-%m-%d"),
                "days": (end - start).days,
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

            LOG.info(f"  Window {i} ({start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')})")
            LOG.info(f"    Setups: {metrics['n']}")
            LOG.info(f"    P(TP)={metrics['p_tp']:.1%}, P(SL)={metrics['p_sl']:.1%}, P(neither)={metrics['p_neither']:.1%}")
            LOG.info(f"    Expectancy: {metrics['expectancy']:.3%}")
            LOG.info("")

        LOG.info("")

    # Salva risultati dettagliati
    if not all_results:
        LOG.error("❌ Nessun risultato generato!")
        return

    results_df = pd.DataFrame(all_results)
    results_df.to_csv(out_dir / "walk_forward_results.csv", index=False)

    # Summary per simbolo
    LOG.info(f"{'='*80}")
    LOG.info("SUMMARY PER SIMBOLO")
    LOG.info(f"{'='*80}\n")

    summary = results_df.groupby("symbol").agg({
        "n_setups": "sum",
        "n_tp": "sum",
        "n_sl": "sum",
        "expectancy": "mean",
        "p_tp": "mean",
        "p_sl": "mean",
    }).reset_index()

    # Calcola aggregate P(TP) e P(SL)
    summary["p_tp_agg"] = summary["n_tp"] / summary["n_setups"]
    summary["p_sl_agg"] = summary["n_sl"] / summary["n_setups"]

    summary.columns = ["Symbol", "Total Setups", "n_tp", "n_sl", "Exp (avg)",
                       "P(TP) avg", "P(SL) avg", "P(TP) agg", "P(SL) agg"]

    LOG.info(summary.to_string(index=False))
    LOG.info("")

    summary.to_csv(out_dir / "summary_by_symbol.csv", index=False)

    # Overall summary
    LOG.info(f"{'='*80}")
    LOG.info("OVERALL SUMMARY")
    LOG.info(f"{'='*80}\n")

    overall = {
        "total_setups": results_df["n_setups"].sum(),
        "total_tp": results_df["n_tp"].sum(),
        "total_sl": results_df["n_sl"].sum(),
        "total_windows": len(results_df),
        "total_symbols": results_df["symbol"].nunique(),

        "p_tp_agg": results_df["n_tp"].sum() / results_df["n_setups"].sum(),
        "p_sl_agg": results_df["n_sl"].sum() / results_df["n_setups"].sum(),

        "exp_mean": results_df["expectancy"].mean(),
        "exp_std": results_df["expectancy"].std(),
        "exp_min": results_df["expectancy"].min(),
        "exp_max": results_df["expectancy"].max(),

        "p_tp_mean": results_df["p_tp"].mean(),
        "p_tp_std": results_df["p_tp"].std(),

        "p_sl_mean": results_df["p_sl"].mean(),
        "p_sl_std": results_df["p_sl"].std(),
    }

    LOG.info(f"Total setups:          {overall['total_setups']}")
    LOG.info(f"Total windows tested:  {overall['total_windows']}")
    LOG.info(f"Total symbols:         {overall['total_symbols']}")
    LOG.info("")
    LOG.info("Aggregated Probabilities:")
    LOG.info(f"  P(hit TP):           {overall['p_tp_agg']:.1%}")
    LOG.info(f"  P(hit SL):           {overall['p_sl_agg']:.1%}")
    LOG.info("")
    LOG.info("Expectancy (per window):")
    LOG.info(f"  Mean:                {overall['exp_mean']:+.3%}")
    LOG.info(f"  Std:                 {overall['exp_std']:.3%}")
    LOG.info(f"  Range:               [{overall['exp_min']:+.3%}, {overall['exp_max']:+.3%}]")
    LOG.info("")
    LOG.info("Win Rate (per window):")
    LOG.info(f"  P(TP) mean:          {overall['p_tp_mean']:.1%} ± {overall['p_tp_std']:.1%}")
    LOG.info(f"  P(SL) mean:          {overall['p_sl_mean']:.1%} ± {overall['p_sl_std']:.1%}")
    LOG.info("")

    # Verdict
    if overall["exp_mean"] > 0.0008:
        verdict = "✅ STRATEGIA PROFITTEVOLE (exp > 0.08%)"
    elif overall["exp_mean"] > 0:
        verdict = "⚠️  STRATEGIA MARGINALMENTE PROFITTEVOLE (0% < exp < 0.08%)"
    else:
        verdict = "❌ STRATEGIA NON PROFITTEVOLE (exp ≤ 0%)"

    LOG.info(f"{'='*80}")
    LOG.info(f"VERDICT: {verdict}")
    LOG.info(f"{'='*80}")

    pd.DataFrame([overall]).to_csv(out_dir / "overall_summary.csv", index=False)

    LOG.info(f"\n✅ Analisi completata. Output salvato in: {out_dir}")
    LOG.info(f"   - walk_forward_results.csv (dettaglio per finestra)")
    LOG.info(f"   - summary_by_symbol.csv (aggregato per simbolo)")
    LOG.info(f"   - overall_summary.csv (summary complessivo)")


if __name__ == "__main__":
    main()
