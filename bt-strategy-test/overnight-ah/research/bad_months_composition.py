#!/usr/bin/env python3
"""Fase 2 dello studio mesi negativi OvernightAH: composizione del paniere
tradato nei mesi negativi, per identificare worst-contributor ricorrenti.

Legge `trades.json` gia' generati (nessun nuovo backtest) e i mesi negativi
gia' identificati da `bad_months_identify.py` (Fase 1). Usa gli stessi
segmenti/floor di trading reale di Fase 1 per evitare la contaminazione
warmup/full-history gia' documentata li' (vedi bad_months_identify.py per il
dettaglio).

Usage:
  bt-core/.venv/bin/python bt-strategy-test/overnight-ah/research/bad_months_composition.py
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = Path(__file__).resolve().parent / "out" / "bad_months_study"
BT_OUT = ROOT / "bt-core" / "out" / "overnight_ah" / "OvernightAH"

STATIC_DIRS = {
    "train": BT_OUT / "static_bench_train_stable_ah_top10_train",
    "validation": BT_OUT / "static_bench_val_stable_ah_top10_validation",
    "oos": BT_OUT / "static_bench_oos_stable_ah_top10_oos",
}
SWITCH_DIRS = {
    "train": BT_OUT / "native_switch_3mpos_train_warmup2015_trade2016",
    "validation": BT_OUT / "native_switch_3mpos_val_warmup2020_trade2021",
    "oos": BT_OUT / "native_switch_3mpos_oos_warmup2023_trade2024",
}
# Same real-trading floors as bad_months_identify.py (train/validation trim
# embedded warmup; oos trims the full-history contamination) — see that
# script's docstring for the verified reasoning.
SWITCH_TRADE_FLOOR = {
    "train": "2016-01-01",
    "validation": "2021-01-01",
    "oos": "2024-01-01",
}

NEG_MONTHS_STATIC = OUT_DIR / "negative_months_static.csv"
NEG_MONTHS_SWITCH = OUT_DIR / "negative_months_switch.csv"

REGIME_DETAIL_CSV = (
    ROOT
    / "bt-strategy-test"
    / "overnight-ah"
    / "research"
    / "out"
    / "edge_prediction_study_all_adj"
    / "monthly_universes_regime_switch"
    / "switch_semis_total_3m_gt_p0_detail.csv"
)


def load_trades(path: Path) -> pd.DataFrame:
    data = json.loads(path.read_text())
    df = pd.DataFrame(data)
    df["close_datetime"] = pd.to_datetime(df["close_datetime"])
    df["month"] = df["close_datetime"].dt.to_period("M")
    return df


def load_policy_trades(dirs: dict[str, Path], trade_floor: dict[str, str] | None = None) -> pd.DataFrame:
    frames = []
    for name, d in dirs.items():
        df = load_trades(d / "trades.json")
        floor = (trade_floor or {}).get(name)
        if floor is not None:
            df = df[df["close_datetime"] >= pd.Timestamp(floor)]
        df = df.copy()
        df["segment"] = name
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def monthly_symbol_pnl(trades: pd.DataFrame) -> pd.DataFrame:
    return trades.groupby(["month", "asset"], as_index=False)["pnl"].sum()


def worst_contributor_table(monthly_symbol: pd.DataFrame, negative_months) -> pd.DataFrame:
    rows = []
    for month in negative_months:
        g = monthly_symbol[monthly_symbol["month"] == month]
        if g.empty:
            continue
        total_pnl = float(g["pnl"].sum())
        neg = g[g["pnl"] < 0]
        neg_sum = float(neg["pnl"].sum())  # <= 0
        n_symbols = int(g["asset"].nunique())
        worst_row = g.loc[g["pnl"].idxmin()]
        worst_share = float(worst_row["pnl"] / neg_sum) if neg_sum < 0 else np.nan
        rows.append(
            {
                "month": str(month),
                "total_month_pnl": total_pnl,
                "n_symbols_traded": n_symbols,
                "n_negative_symbols": int(len(neg)),
                "worst_symbol": worst_row["asset"],
                "worst_symbol_pnl": float(worst_row["pnl"]),
                "sum_negative_pnl": neg_sum,
                "worst_share_of_negative_pnl": worst_share,
            }
        )
    return pd.DataFrame(rows)


def worst_contributor_frequency_test(
    worst_table: pd.DataFrame, monthly_symbol: pd.DataFrame, negative_months
) -> pd.DataFrame:
    """Per simbolo: quante volte e' worst-contributor in un mese negativo, su
    quanti mesi negativi ha effettivamente tradato, confrontato con un test
    binomiale contro un tasso base uniforme (1 / n medio di simboli tradati
    al mese). Campione piccolo: solo un test semplice, come richiesto dalla
    spec (niente di piu' sofisticato)."""
    if worst_table.empty:
        return pd.DataFrame()

    counts = worst_table["worst_symbol"].value_counts()
    traded = monthly_symbol[monthly_symbol["month"].isin(negative_months)]
    symbol_traded_count = traded.groupby("asset")["month"].nunique()

    avg_n_symbols = worst_table["n_symbols_traded"].mean()
    base_rate = 1.0 / avg_n_symbols if avg_n_symbols else np.nan

    rows = []
    for symbol, traded_n in symbol_traded_count.items():
        worst_n = int(counts.get(symbol, 0))
        traded_n = int(traded_n)
        p_value = (
            stats.binomtest(worst_n, traded_n, base_rate, alternative="greater").pvalue
            if traded_n > 0
            else np.nan
        )
        rows.append(
            {
                "symbol": symbol,
                "negative_months_traded": traded_n,
                "times_worst_contributor": worst_n,
                "observed_rate": worst_n / traded_n if traded_n else np.nan,
                "base_rate_uniform": base_rate,
                "binomial_pvalue_greater": p_value,
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["times_worst_contributor", "observed_rate"], ascending=False
    )


def attach_regime_context(worst_table: pd.DataFrame) -> pd.DataFrame:
    """Annotate weak_theme_switch worst-contributor months with the
    static/dynamic regime label, as context only (see caveat in the plan:
    this CSV does not exactly match the live-traded panel, never used as
    ground truth for composition — only for interpretation)."""
    if worst_table.empty or not REGIME_DETAIL_CSV.exists():
        return worst_table
    detail = pd.read_csv(REGIME_DETAIL_CSV, parse_dates=["month"])
    detail["month"] = detail["month"].dt.to_period("M").astype(str)
    detail = detail[["month", "regime", "n"]].rename(columns={"n": "regime_universe_n"})
    return worst_table.merge(detail, on="month", how="left")


def run(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    static_neg = pd.read_csv(NEG_MONTHS_STATIC, parse_dates=["month"])
    static_neg_months = static_neg.loc[static_neg["is_negative"], "month"].dt.to_period("M")
    switch_neg = pd.read_csv(NEG_MONTHS_SWITCH, parse_dates=["month"])
    switch_neg_months = switch_neg.loc[switch_neg["is_negative"], "month"].dt.to_period("M")

    static_trades = load_policy_trades(STATIC_DIRS)
    switch_trades = load_policy_trades(SWITCH_DIRS, trade_floor=SWITCH_TRADE_FLOOR)

    static_monthly_symbol = monthly_symbol_pnl(static_trades)
    switch_monthly_symbol = monthly_symbol_pnl(switch_trades)

    static_worst = worst_contributor_table(static_monthly_symbol, static_neg_months)
    switch_worst = worst_contributor_table(switch_monthly_symbol, switch_neg_months)
    switch_worst = attach_regime_context(switch_worst)

    static_freq = worst_contributor_frequency_test(static_worst, static_monthly_symbol, static_neg_months)
    switch_freq = worst_contributor_frequency_test(switch_worst, switch_monthly_symbol, switch_neg_months)

    static_monthly_symbol.assign(month=lambda d: d["month"].astype(str)).to_csv(
        out_dir / "symbol_contribution_static.csv", index=False
    )
    switch_monthly_symbol.assign(month=lambda d: d["month"].astype(str)).to_csv(
        out_dir / "symbol_contribution_switch.csv", index=False
    )
    static_worst.to_csv(out_dir / "worst_contributors_static.csv", index=False)
    switch_worst.to_csv(out_dir / "worst_contributors_switch.csv", index=False)
    static_freq.to_csv(out_dir / "worst_contributors_summary_static.csv", index=False)
    switch_freq.to_csv(out_dir / "worst_contributors_summary_switch.csv", index=False)

    # Concentration diagnostic: is negative P&L usually 1-2 names or spread out?
    def concentration_summary(worst_table: pd.DataFrame) -> dict:
        if worst_table.empty:
            return {}
        share = worst_table["worst_share_of_negative_pnl"].dropna()
        return {
            "n_negative_months": int(len(worst_table)),
            "mean_worst_share_of_negative_pnl": float(share.mean()),
            "median_worst_share_of_negative_pnl": float(share.median()),
            "pct_months_worst_share_over_50pct": float((share > 0.5).mean() * 100),
            "pct_months_worst_share_over_75pct": float((share > 0.75).mean() * 100),
        }

    static_conc = concentration_summary(static_worst)
    switch_conc = concentration_summary(switch_worst)

    report = []
    report.append("# Fase 2 — Composizione del paniere nei mesi negativi")
    report.append("")
    report.append(
        "Trade parsing da `trades.json` (fonte primaria, mai `positions.csv` — "
        "vedi caveat gia' verificato: `positions.csv` per i run "
        "`native_switch_3mpos_*` ha solo 3 colonne, `Datetime,SPY,cash`, "
        "contro 99 simboli distinti visti in `trades.json`)."
    )
    report.append("")
    report.append("## Concentrazione P&L negativo (1-2 simboli vs distribuito)")
    report.append("### static-10")
    for k, v in static_conc.items():
        report.append(f"- {k}: {v}")
    report.append("")
    report.append("### weak_theme_switch")
    for k, v in switch_conc.items():
        report.append(f"- {k}: {v}")
    report.append("")
    report.append("## Worst-contributor ricorrenti (top 10 per conteggio)")
    report.append("### static-10")
    if not static_freq.empty:
        report.append(static_freq.head(10).to_markdown(index=False))
    report.append("")
    report.append("### weak_theme_switch")
    if not switch_freq.empty:
        report.append(switch_freq.head(10).to_markdown(index=False))
    report.append("")
    report.append(
        "Nota metodologica: `binomial_pvalue_greater` testa, per simbolo, se il "
        "tasso osservato di 'worst contributor' nei mesi negativi supera un "
        "tasso base uniforme (1 / n medio simboli tradati al mese quel "
        "regime). Campione piccolo (poche decine di mesi negativi): p-value "
        "indicativo, non usare come soglia dura di significativita' — vedi "
        "vincoli metodologici in `docs/context/ah_bad_months_study_spec.md`."
    )
    (out_dir / "summary_fase2.md").write_text("\n".join(report))

    print(f"wrote {out_dir}")
    print("static concentration:", static_conc)
    print("switch concentration:", switch_conc)
    print()
    print("static worst-contributor top10:")
    print(static_freq.head(10).to_string(index=False) if not static_freq.empty else "(empty)")
    print()
    print("switch worst-contributor top10:")
    print(switch_freq.head(10).to_string(index=False) if not switch_freq.empty else "(empty)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fase 2: composizione paniere nei mesi negativi OvernightAH")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args().out_dir)
