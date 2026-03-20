#!/usr/bin/env python3
"""
Ottimizzazione filtri volatilità per garantire expectancy minima.

Testa diverse combinazioni di:
- Percentili magnitude (p75, p80, p90)
- Percentili quality/disp (p75, p80, p90)
- ATR filter (opzionale)

Obiettivo: Poche entrate ma sicure (exp > 0.01%)
"""

import argparse
import logging
from pathlib import Path
import re
from typing import Dict, List
import pandas as pd
import numpy as np

LOG = logging.getLogger("volatility_filter_optimization")


def parse_args():
    p = argparse.ArgumentParser(description="Ottimizza filtri per expectancy minima")
    p.add_argument("--symbols", default="all")
    p.add_argument("--input-glob", default="results/*_feature_outcome_full.csv")
    p.add_argument("--min-expectancy", type=float, default=0.0001,
                   help="Expectancy minima richiesta (default: 0.01%%)")
    p.add_argument("--tp-pct", type=float, default=0.005)
    p.add_argument("--sl-pct", type=float, default=0.001)
    p.add_argument("--output-dir", default="results/volatility_optimization")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


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

        if symbols != "all":
            requested = [s.strip().upper() for s in symbols.split(",")]
            if symbol.upper() not in requested:
                continue

        df = pd.read_csv(fp)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

        required_cols = ["slope_5", "slope_20", "slope_60", "disp_60",
                        "high_ret_max_5m", "low_ret_min_5m", "close_ret_5m"]

        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            LOG.warning(f"  {symbol}: colonne mancanti {missing}, skip")
            continue

        data_by_symbol[symbol] = df
        LOG.info(f"  Loaded {symbol}: {len(df)} rows")

    return data_by_symbol


def apply_filters(df: pd.DataFrame, mag_pct: float, disp_pct: float) -> pd.Series:
    """
    Applica filtri con percentili configurabili.

    Args:
        mag_pct: Percentile per magnitude (75, 80, 90)
        disp_pct: Percentile per disp (75, 80, 90)
    """
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
        "p_tp": p_tp,
        "p_sl": p_sl,
        "expectancy": expectancy,
    }


def main():
    args = parse_args()
    setup_logging(args.log_level)

    out_dir = Path(__file__).resolve().parent / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    LOG.info("="*80)
    LOG.info("VOLATILITY FILTER OPTIMIZATION")
    LOG.info("="*80)
    LOG.info(f"Target: Expectancy >= {args.min_expectancy:.3%}")
    LOG.info(f"TP: {args.tp_pct:.2%}, SL: {args.sl_pct:.2%}")
    LOG.info("")

    LOG.info("Caricamento dati...")
    data_by_symbol = load_data(args.input_glob, args.symbols)

    if not data_by_symbol:
        LOG.error("❌ Nessun simbolo caricato!")
        return

    # Combina tutti i dati
    df_all = pd.concat(data_by_symbol.values(), ignore_index=True)
    LOG.info(f"✅ Dataset combinato: {len(df_all)} righe\n")

    # Test diverse combinazioni di percentili
    LOG.info("="*80)
    LOG.info("TESTING FILTER COMBINATIONS")
    LOG.info("="*80)

    percentiles_to_test = [
        # (mag_pct, disp_pct)
        (75, 75),   # Baseline (Setup 2 originale)
        (80, 75),   # Magnitude più selettivo
        (85, 75),
        (90, 75),
        (75, 80),   # Disp più selettivo
        (75, 85),
        (75, 90),
        (80, 80),   # Entrambi più selettivi
        (85, 85),
        (90, 90),   # Ultra selettivo
        (95, 90),
        (90, 95),
    ]

    results = []

    for mag_pct, disp_pct in percentiles_to_test:
        filter_mask = apply_filters(df_all, mag_pct, disp_pct)
        df_filtered = df_all[filter_mask]

        if len(df_filtered) < 100:
            LOG.warning(f"  mag={mag_pct}, disp={disp_pct}: troppo poche candele ({len(df_filtered)}), skip")
            continue

        metrics = calculate_metrics(df_filtered, args.tp_pct, args.sl_pct)

        if not metrics:
            continue

        pct_kept = 100 * metrics["n"] / len(df_all)
        meets_target = "✅" if metrics["expectancy"] >= args.min_expectancy else "❌"

        result = {
            "mag_percentile": mag_pct,
            "disp_percentile": disp_pct,
            "n_setups": metrics["n"],
            "pct_kept": pct_kept,
            "p_tp": metrics["p_tp"],
            "p_sl": metrics["p_sl"],
            "expectancy": metrics["expectancy"],
            "meets_target": meets_target,
        }

        results.append(result)

        LOG.info(f"  mag=p{mag_pct}, disp=p{disp_pct}:")
        LOG.info(f"    Setups:     {metrics['n']:5d} ({pct_kept:4.2f}%)")
        LOG.info(f"    P(TP):      {metrics['p_tp']:5.1%}")
        LOG.info(f"    P(SL):      {metrics['p_sl']:5.1%}")
        LOG.info(f"    Expectancy: {metrics['expectancy']:+.4%}  {meets_target}")

    # Salva risultati
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values("expectancy", ascending=False)
    results_df.to_csv(out_dir / "filter_combinations.csv", index=False)

    LOG.info("")
    LOG.info("="*80)
    LOG.info("TOP 5 CONFIGURATIONS (by Expectancy)")
    LOG.info("="*80)

    top5 = results_df.head(5)
    LOG.info("\n" + top5.to_string(index=False))

    LOG.info("")
    LOG.info("="*80)
    LOG.info("CONFIGURATIONS MEETING TARGET (Exp >= {:.3%})".format(args.min_expectancy))
    LOG.info("="*80)

    meets_target = results_df[results_df["expectancy"] >= args.min_expectancy]

    if len(meets_target) == 0:
        LOG.warning("❌ Nessuna configurazione raggiunge il target!")
        LOG.warning(f"   Miglior expectancy trovata: {results_df['expectancy'].max():.4%}")
        LOG.warning(f"   Target richiesto: {args.min_expectancy:.4%}")
        LOG.warning("")
        LOG.warning("Suggerimenti:")
        LOG.warning("  1. Riduci --min-expectancy (es. 0.00005 = 0.005%)")
        LOG.warning("  2. Aggiungi più asset volatili (NVDA, META, GOOGL)")
        LOG.warning("  3. Usa trailing stop invece di TP fisso")
    else:
        LOG.info(f"\n✅ Trovate {len(meets_target)} configurazioni valide:\n")
        LOG.info(meets_target.to_string(index=False))

        # Raccomandazione
        LOG.info("")
        LOG.info("="*80)
        LOG.info("RACCOMANDAZIONE")
        LOG.info("="*80)

        # Scegli quella con miglior bilanciamento exp vs opportunità
        # Preferisci quella con più setup se exp simile
        best = meets_target.iloc[0]

        LOG.info(f"\n✅ Configurazione raccomandata:")
        LOG.info(f"   Magnitude percentile:  p{best['mag_percentile']:.0f}")
        LOG.info(f"   Disp percentile:       p{best['disp_percentile']:.0f}")
        LOG.info(f"   Setups/giorno (stimati): ~{best['n_setups'] / 365:.1f}")
        LOG.info(f"   P(hit TP):             {best['p_tp']:.1%}")
        LOG.info(f"   P(hit SL):             {best['p_sl']:.1%}")
        LOG.info(f"   Expectancy:            {best['expectancy']:+.4%}")
        LOG.info("")
        LOG.info("   Expectancy giornaliera stimata: {:.3%}".format(
            best['expectancy'] * best['n_setups'] / 365
        ))

    LOG.info("")
    LOG.info(f"✅ Output salvato in: {out_dir}")


if __name__ == "__main__":
    main()
