#!/usr/bin/env python3
"""Fase 4 dello studio mesi negativi OvernightAH: pattern riconoscibili
(stagionalita', clustering temporale, sovrapposizione con drawdown macro
SPY/QQQ, classificazione regime-wide vs idiosincratico via breadth).

Riusa i mesi negativi di Fase 1 e il segnale di breadth di Fase 3. Nessun
CSV di episodi di drawdown SPY/QQQ price-based esisteva nel repo (verificato:
`out/overnight_ah/OvernightAH/LZ/dd_diagnostics/drawdown_episodes.csv` e'
drawdown del portafoglio strategia, non di SPY/QQQ) — costruito qui con una
semplice logica peak/trough/recovery.

Usage:
  bt-core/.venv/bin/python bt-strategy-test/overnight-ah/research/bad_months_patterns.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from edge_prediction_study import load_symbol  # noqa: E402
from bad_months_signals import breadth_monthly, load_daily_panel  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = Path(__file__).resolve().parent / "out" / "bad_months_study"
YAHOO_ADJ_DIR = ROOT / "config-common" / "data" / "d" / "yahoo_adj"

NEG_MONTHS_STATIC = OUT_DIR / "monthly_returns_static.csv"
NEG_MONTHS_SWITCH = OUT_DIR / "monthly_returns_switch.csv"

DD_ENTER_THRESHOLD = -0.10  # episode starts once price is 10% below rolling peak
DD_EXIT_THRESHOLD = -0.02  # episode ends once price recovers within 2% of peak


# --------------------------------------------------------------------- #
# Stagionalita'
# --------------------------------------------------------------------- #


def seasonality_table(neg_months: pd.DataFrame) -> pd.DataFrame:
    df = neg_months.copy()
    df["calendar_month"] = df["month"].dt.month
    base_rate = df["is_negative"].mean()
    g = df.groupby("calendar_month")["is_negative"].agg(["sum", "count", "mean"])
    g = g.rename(columns={"sum": "n_negative", "count": "n_total", "mean": "negative_rate"})
    g["base_rate"] = base_rate
    g["rate_vs_base"] = g["negative_rate"] - base_rate
    return g.reset_index()


# --------------------------------------------------------------------- #
# Clustering temporale
# --------------------------------------------------------------------- #


def clustering_table(neg_months: pd.DataFrame) -> pd.DataFrame:
    df = neg_months.sort_values("month").reset_index(drop=True)
    streak_id = (df["is_negative"] != df["is_negative"].shift()).cumsum()
    streaks = df.groupby(streak_id).agg(
        is_negative=("is_negative", "first"),
        start_month=("month", "min"),
        end_month=("month", "max"),
        length=("month", "count"),
    )
    neg_streaks = streaks[streaks["is_negative"]].sort_values("length", ascending=False)
    return neg_streaks.reset_index(drop=True)


def clustering_summary(streaks: pd.DataFrame) -> dict:
    if streaks.empty:
        return {}
    return {
        "n_negative_episodes": int(len(streaks)),
        "max_streak_months": int(streaks["length"].max()),
        "mean_streak_months": float(streaks["length"].mean()),
        "pct_isolated_1_month": float((streaks["length"] == 1).mean() * 100),
        "pct_2plus_consecutive": float((streaks["length"] >= 2).mean() * 100),
    }


# --------------------------------------------------------------------- #
# Episodi di drawdown macro SPY/QQQ (price-based, peak/trough/recovery)
# --------------------------------------------------------------------- #


def drawdown_episodes(symbol: str) -> pd.DataFrame:
    df = load_symbol(YAHOO_ADJ_DIR / f"{symbol}.csv", symbol).sort_values("date")
    closes = df.set_index("date")["close"]
    peak = closes.cummax()
    dd = closes / peak - 1.0

    episodes = []
    in_episode = False
    peak_date = trough_date = trough_val = None
    for date, dd_val, peak_val, close_val in zip(dd.index, dd.values, peak.values, closes.values):
        if not in_episode:
            if dd_val <= DD_ENTER_THRESHOLD:
                in_episode = True
                # peak date = last date the running peak equaled this peak value
                peak_date = closes[(closes.index <= date) & (closes == peak_val)].index.max()
                trough_date, trough_val = date, dd_val
        else:
            if dd_val < trough_val:
                trough_date, trough_val = date, dd_val
            if dd_val >= DD_EXIT_THRESHOLD:
                episodes.append(
                    {
                        "symbol": symbol,
                        "peak_date": peak_date,
                        "trough_date": trough_date,
                        "recovery_date": date,
                        "max_drawdown_pct": float(trough_val * 100),
                    }
                )
                in_episode = False
                peak_date = trough_date = trough_val = None
    if in_episode:
        episodes.append(
            {
                "symbol": symbol,
                "peak_date": peak_date,
                "trough_date": trough_date,
                "recovery_date": None,
                "max_drawdown_pct": float(trough_val * 100),
            }
        )
    return pd.DataFrame(episodes)


def month_in_any_episode(month_start: pd.Timestamp, episodes: pd.DataFrame) -> bool:
    month_end = month_start + pd.offsets.MonthEnd(0)
    for _, ep in episodes.iterrows():
        ep_start = ep["peak_date"]
        ep_end = ep["recovery_date"] if pd.notna(ep["recovery_date"]) else pd.Timestamp.max
        if ep_start <= month_end and ep_end >= month_start:
            return True
    return False


def macro_overlap_table(neg_months: pd.DataFrame, spy_eps: pd.DataFrame, qqq_eps: pd.DataFrame) -> pd.DataFrame:
    df = neg_months[neg_months["is_negative"]].copy()
    df["in_spy_drawdown_episode"] = df["month"].apply(lambda m: month_in_any_episode(m, spy_eps))
    df["in_qqq_drawdown_episode"] = df["month"].apply(lambda m: month_in_any_episode(m, qqq_eps))
    df["macro_overlap"] = df["in_spy_drawdown_episode"] | df["in_qqq_drawdown_episode"]
    return df


# --------------------------------------------------------------------- #
# Regime-wide vs idiosincratico via breadth
# --------------------------------------------------------------------- #


def breadth_classification(neg_months: pd.DataFrame, breadth: pd.DataFrame, breadth_threshold: float) -> pd.DataFrame:
    df = neg_months[neg_months["is_negative"]].copy()
    df["month_period"] = pd.PeriodIndex(df["month"], freq="M")
    merged = df.merge(breadth, left_on="month_period", right_on="month", how="left", suffixes=("", "_b"))
    merged["regime_wide"] = merged["breadth_pct_down_c2c_mean"] >= breadth_threshold
    return merged


def run(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    static_neg = pd.read_csv(NEG_MONTHS_STATIC, parse_dates=["month"])
    switch_neg = pd.read_csv(NEG_MONTHS_SWITCH, parse_dates=["month"])

    static_season = seasonality_table(static_neg)
    switch_season = seasonality_table(switch_neg)
    static_season.to_csv(out_dir / "seasonality_summary_static.csv", index=False)
    switch_season.to_csv(out_dir / "seasonality_summary_switch.csv", index=False)

    static_streaks = clustering_table(static_neg)
    switch_streaks = clustering_table(switch_neg)
    static_streaks.to_csv(out_dir / "clustering_summary_static.csv", index=False)
    switch_streaks.to_csv(out_dir / "clustering_summary_switch.csv", index=False)
    static_cluster_summary = clustering_summary(static_streaks)
    switch_cluster_summary = clustering_summary(switch_streaks)

    spy_eps = drawdown_episodes("SPY")
    qqq_eps = drawdown_episodes("QQQ")
    spy_eps.to_csv(out_dir / "macro_drawdown_episodes_spy.csv", index=False)
    qqq_eps.to_csv(out_dir / "macro_drawdown_episodes_qqq.csv", index=False)

    static_macro = macro_overlap_table(static_neg, spy_eps, qqq_eps)
    switch_macro = macro_overlap_table(switch_neg, spy_eps, qqq_eps)
    static_macro.to_csv(out_dir / "macro_drawdown_episodes_spy_qqq_static.csv", index=False)
    switch_macro.to_csv(out_dir / "macro_drawdown_episodes_spy_qqq_switch.csv", index=False)

    panel = load_daily_panel()
    breadth = breadth_monthly(panel)
    # Threshold: top quartile of breadth level across ALL months (not just
    # negative ones), so "regime-wide" is relative to the whole history, not
    # self-referential to the negative-month sample.
    breadth_threshold = float(breadth["breadth_pct_down_c2c_mean"].quantile(0.75))

    static_breadth_class = breadth_classification(static_neg, breadth, breadth_threshold)
    switch_breadth_class = breadth_classification(switch_neg, breadth, breadth_threshold)
    static_breadth_class.to_csv(out_dir / "breadth_vs_bad_months_static.csv", index=False)
    switch_breadth_class.to_csv(out_dir / "breadth_vs_bad_months_switch.csv", index=False)

    static_idio = static_macro.merge(
        static_breadth_class[["month", "regime_wide"]], on="month", how="left"
    )
    static_idio["idiosyncratic"] = ~static_idio["macro_overlap"] & ~static_idio["regime_wide"].fillna(False)
    switch_idio = switch_macro.merge(
        switch_breadth_class[["month", "regime_wide"]], on="month", how="left"
    )
    switch_idio["idiosyncratic"] = ~switch_idio["macro_overlap"] & ~switch_idio["regime_wide"].fillna(False)
    static_idio.to_csv(out_dir / "idiosyncratic_bad_months_static.csv", index=False)
    switch_idio.to_csv(out_dir / "idiosyncratic_bad_months_switch.csv", index=False)

    report = []
    report.append("# Fase 4 — Pattern riconoscibili")
    report.append("")
    report.append(
        "Nota freshness: `QQQ.csv` (yahoo_adj) e' fermo al 2026-06-18, quindi "
        "gli ultimi 1-2 mesi dello studio (fino a 2026-07) possono mancare "
        "dell'overlap QQQ; SPY resta aggiornato. Non blocca la lettura "
        "generale (i mesi negativi piu' rilevanti sono storici)."
    )
    report.append("")
    report.append("## Stagionalita' (deviazione dal base rate per mese calendario)")
    report.append("### static-10")
    report.append(static_season.to_markdown(index=False))
    report.append("")
    report.append("### weak_theme_switch")
    report.append(switch_season.to_markdown(index=False))
    report.append("")
    report.append("## Clustering temporale")
    report.append(f"### static-10: {static_cluster_summary}")
    report.append(f"### weak_theme_switch: {switch_cluster_summary}")
    report.append("")
    report.append("## Episodi di drawdown macro (SPY/QQQ, price-based)")
    report.append(f"SPY: {len(spy_eps)} episodi; QQQ: {len(qqq_eps)} episodi (soglia entrata {DD_ENTER_THRESHOLD:.0%}, uscita {DD_EXIT_THRESHOLD:.0%}).")
    report.append("")
    report.append("## Regime-wide vs idiosincratico")
    report.append(f"Soglia breadth 'regime-wide' (p75 storico): {breadth_threshold:.3f}")
    report.append(
        f"- static-10: {int(static_idio['macro_overlap'].sum())}/{len(static_idio)} mesi negativi "
        f"in un episodio di drawdown macro SPY/QQQ, "
        f"{int(static_idio['regime_wide'].sum())}/{len(static_idio)} con breadth alta, "
        f"{int(static_idio['idiosyncratic'].sum())}/{len(static_idio)} idiosincratici (ne' macro ne' breadth alta)."
    )
    report.append(
        f"- weak_theme_switch: {int(switch_idio['macro_overlap'].sum())}/{len(switch_idio)} mesi negativi "
        f"in un episodio di drawdown macro SPY/QQQ, "
        f"{int(switch_idio['regime_wide'].sum())}/{len(switch_idio)} con breadth alta, "
        f"{int(switch_idio['idiosyncratic'].sum())}/{len(switch_idio)} idiosincratici (ne' macro ne' breadth alta)."
    )
    (out_dir / "summary_fase4.md").write_text("\n".join(report))

    print(f"wrote {out_dir}")
    print("static clustering:", static_cluster_summary)
    print("switch clustering:", switch_cluster_summary)
    print(f"SPY episodes: {len(spy_eps)}, QQQ episodes: {len(qqq_eps)}")
    print(
        f"static idiosyncratic: {int(static_idio['idiosyncratic'].sum())}/{len(static_idio)}, "
        f"switch idiosyncratic: {int(switch_idio['idiosyncratic'].sum())}/{len(switch_idio)}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fase 4: pattern riconoscibili OvernightAH")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args().out_dir)
