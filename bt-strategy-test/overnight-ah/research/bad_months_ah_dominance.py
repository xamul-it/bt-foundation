#!/usr/bin/env python3
"""Fase 2b dello studio mesi negativi OvernightAH: ipotesi utente
"AH-dominance vs rumore RTH" — i worst-contributor dei mesi negativi (Fase 2)
tendono a essere titoli mossi prevalentemente da RTH (non AH)?

Score `ah_dominance` rolling 24m ex-ante costruito da `daily_panel.csv` gia'
generato (nessun nuovo backtest). Stessa formula/spirito di `ah_pct` in
BoCSoO (`bt-strategy-test/BoCSoO/02_decompose.py`), ma su finestra rolling
ex-ante invece che su 5 finestre statiche a tutta storia (BoCSoO non e'
riusabile direttamente per questo, verificato: e' look-ahead).

Per ogni ticker/mese: solo barre con `date < month_start` (stesso pattern
ex-ante di `semis_monthly_features` in `build_regime_switch_universes.py`).
Componente AH: `known_ah_ret` (gia' risolto/noto al momento della barra,
niente ambiguita' col target forward — vedi gotcha `known_ah_ret`/
`target_ah_ret` nel piano). Componente RTH: `rth_ret` (open->close stesso
giorno, gia' noto a fine barra).

Usage:
  bt-core/.venv/bin/python bt-strategy-test/overnight-ah/research/bad_months_ah_dominance.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = Path(__file__).resolve().parent / "out" / "bad_months_study"
DAILY_PANEL = (
    ROOT
    / "bt-strategy-test"
    / "overnight-ah"
    / "research"
    / "out"
    / "edge_prediction_study_all_adj"
    / "daily_panel.csv"
)

ROLLING_WINDOW_DAYS = 504  # ~24 trading months
MIN_PERIODS_DAYS = 252  # require at least ~1y before computing a value

WORST_STATIC = OUT_DIR / "worst_contributors_static.csv"
WORST_SWITCH = OUT_DIR / "worst_contributors_switch.csv"
SYMBOL_CONTRIB_STATIC = OUT_DIR / "symbol_contribution_static.csv"
SYMBOL_CONTRIB_SWITCH = OUT_DIR / "symbol_contribution_switch.csv"


def load_daily_panel() -> pd.DataFrame:
    df = pd.read_csv(
        DAILY_PANEL,
        usecols=["date", "ticker", "known_ah_ret", "rth_ret"],
        parse_dates=["date"],
    )
    return df.sort_values(["ticker", "date"])


def rolling_ah_dominance_series(panel: pd.DataFrame) -> pd.DataFrame:
    """Per-ticker rolling |sum(known_ah_ret)| / (|sum(known_ah_ret)| + |sum(rth_ret)|)."""

    def _per_ticker(g: pd.DataFrame) -> pd.DataFrame:
        g = g.set_index("date")
        ah_sum = g["known_ah_ret"].rolling(ROLLING_WINDOW_DAYS, min_periods=MIN_PERIODS_DAYS).sum()
        rth_sum = g["rth_ret"].rolling(ROLLING_WINDOW_DAYS, min_periods=MIN_PERIODS_DAYS).sum()
        denom = ah_sum.abs() + rth_sum.abs()
        dom = ah_sum.abs() / denom
        return pd.DataFrame({"ah_dominance_rolling": dom})

    rolled = panel.groupby("ticker", group_keys=True).apply(_per_ticker, include_groups=False)
    return rolled.reset_index()  # columns: ticker, date, ah_dominance_rolling


def build_monthly_ah_dominance(rolled: pd.DataFrame) -> pd.DataFrame:
    """For each ticker/month, the ah_dominance value as of the last date
    strictly before month_start — same ex-ante convention used throughout
    the rest of the project."""
    months = pd.date_range(
        rolled["date"].min().to_period("M").to_timestamp(),
        rolled["date"].max().to_period("M").to_timestamp(),
        freq="MS",
    )
    rows = []
    for ticker, g in rolled.groupby("ticker"):
        g = g.sort_values("date")
        dates = g["date"].to_numpy()
        values = g["ah_dominance_rolling"].to_numpy()
        idx = np.searchsorted(dates, months.to_numpy(), side="left") - 1
        for month, i in zip(months, idx):
            if i < 0:
                continue
            v = values[i]
            if not np.isfinite(v):
                continue
            rows.append({"month": month.to_period("M"), "ticker": ticker, "ah_dominance": float(v)})
    return pd.DataFrame(rows)


def diagnostic_worst_vs_month_mean(
    worst_table: pd.DataFrame, symbol_contrib: pd.DataFrame, ah_dominance_monthly: pd.DataFrame
) -> pd.DataFrame:
    if worst_table.empty:
        return pd.DataFrame()

    dom_lookup = ah_dominance_monthly.set_index(["month", "ticker"])["ah_dominance"]
    symbol_contrib = symbol_contrib.copy()
    symbol_contrib["month_period"] = pd.PeriodIndex(symbol_contrib["month"], freq="M")

    rows = []
    for _, row in worst_table.iterrows():
        month_period = pd.Period(row["month"], freq="M")
        worst_symbol = row["worst_symbol"]
        traded_symbols = symbol_contrib.loc[
            symbol_contrib["month_period"] == month_period, "asset"
        ].unique()
        doms = [
            dom_lookup.get((month_period, s))
            for s in traded_symbols
            if (month_period, s) in dom_lookup.index
        ]
        doms = [d for d in doms if d is not None and np.isfinite(d)]
        if not doms:
            continue
        worst_dom = dom_lookup.get((month_period, worst_symbol))
        if worst_dom is None or not np.isfinite(worst_dom):
            continue
        month_mean = float(np.mean(doms))
        rows.append(
            {
                "month": str(month_period),
                "worst_symbol": worst_symbol,
                "worst_ah_dominance": float(worst_dom),
                "month_mean_ah_dominance": month_mean,
                "n_symbols_with_score": len(doms),
                "diff_worst_minus_mean": float(worst_dom) - month_mean,
            }
        )
    return pd.DataFrame(rows)


def summarize_diagnostic(diag: pd.DataFrame) -> dict:
    if diag.empty:
        return {}
    diff = diag["diff_worst_minus_mean"]
    n_below = int((diff < 0).sum())
    n_total = int(len(diff))
    sign_test = stats.binomtest(n_below, n_total, 0.5, alternative="greater") if n_total else None
    wilcoxon = None
    nonzero = diff[diff != 0]
    if len(nonzero) >= 5:
        try:
            wilcoxon = stats.wilcoxon(nonzero, alternative="less")
        except ValueError:
            wilcoxon = None
    return {
        "n_months": n_total,
        "mean_diff_worst_minus_mean": float(diff.mean()),
        "median_diff_worst_minus_mean": float(diff.median()),
        "pct_months_worst_below_month_mean": float(n_below / n_total * 100) if n_total else np.nan,
        "sign_test_pvalue_greater": float(sign_test.pvalue) if sign_test is not None else np.nan,
        "wilcoxon_pvalue_less": float(wilcoxon.pvalue) if wilcoxon is not None else np.nan,
    }


def run(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    panel = load_daily_panel()
    rolled = rolling_ah_dominance_series(panel)
    ah_dominance_monthly = build_monthly_ah_dominance(rolled)
    ah_dominance_monthly.assign(month=lambda d: d["month"].astype(str)).to_csv(
        out_dir / "ah_dominance_monthly.csv", index=False
    )

    static_worst = pd.read_csv(WORST_STATIC)
    switch_worst = pd.read_csv(WORST_SWITCH)
    static_symbols = pd.read_csv(SYMBOL_CONTRIB_STATIC)
    switch_symbols = pd.read_csv(SYMBOL_CONTRIB_SWITCH)

    static_diag = diagnostic_worst_vs_month_mean(static_worst, static_symbols, ah_dominance_monthly)
    switch_diag = diagnostic_worst_vs_month_mean(switch_worst, switch_symbols, ah_dominance_monthly)

    static_diag.to_csv(out_dir / "worst_contributor_ah_dominance_static.csv", index=False)
    switch_diag.to_csv(out_dir / "worst_contributor_ah_dominance_switch.csv", index=False)

    static_summary = summarize_diagnostic(static_diag)
    switch_summary = summarize_diagnostic(switch_diag)

    report = []
    report.append("# Fase 2b — Ipotesi AH-dominance vs rumore RTH")
    report.append("")
    report.append(
        "Ipotesi utente: i worst-contributor nei mesi negativi tendono ad avere "
        "`ah_dominance` (rolling 24m ex-ante) piu' basso della media del paniere "
        "tradato quel mese, cioe' sono titoli mossi prevalentemente da RTH la "
        "cui spinta ha 'sporcato' l'AH quel mese."
    )
    report.append("")
    report.append("## static-10")
    for k, v in static_summary.items():
        report.append(f"- {k}: {v}")
    report.append("")
    report.append("## weak_theme_switch")
    for k, v in switch_summary.items():
        report.append(f"- {k}: {v}")
    report.append("")
    report.append(
        "Lettura: `pct_months_worst_below_month_mean` > 50% con "
        "`sign_test_pvalue_greater`/`wilcoxon_pvalue_less` bassi supporta "
        "l'ipotesi (worst contributor sistematicamente meno AH-dominant della "
        "media). Valori vicini al 50% o p-value alti non la supportano — in "
        "quel caso non si procede al punto 3 (universo filtrato + proxy "
        "pandas), coerente col piano ('solo se il punto 2 mostra un pattern "
        "chiaro')."
    )
    report.append("")
    report.append("## Verdetto")
    report.append(
        "**Ipotesi NON supportata — anzi risultato opposto.** Su entrambe le "
        "policy, il worst-contributor ha `ah_dominance` **piu' alta** della "
        "media del paniere quel mese (solo 28.6% dei mesi su static-10 e "
        "35.7% su weak_theme_switch mostrano il worst sotto la media attesa "
        "dall'ipotesi; sign-test p≈0.99 e p≈0.96 contro l'ipotesi in "
        "entrambi i casi, Wilcoxon coerente). I titoli che sporcano i mesi "
        "negativi (AMD, NVDA — vedi Fase 2) non sono rumore RTH: sono "
        "proprio i nomi piu' AH-dominant del paniere, gli stessi selezionati "
        "perche' hanno il maggior edge AH storico. Interpretazione: quando "
        "un nome ad alta convinzione/alta esposizione AH va male, va male "
        "proprio sulla sua componente AH (coerente col fatto che e' li' che "
        "si concentra la sua varianza), non per un'infiltrazione di rumore "
        "RTH. **Non si procede al punto 3** (universo filtrato top50% "
        "ah_dominance): filtrare per ah_dominance alta non avrebbe escluso i "
        "worst-contributor storici, li avrebbe anzi confermati candidati "
        "principali."
    )
    (out_dir / "summary_fase2b.md").write_text("\n".join(report))

    print(f"wrote {out_dir}")
    print("static-10 diagnostic:", static_summary)
    print("weak_theme_switch diagnostic:", switch_summary)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fase 2b: ipotesi AH-dominance OvernightAH")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args().out_dir)
