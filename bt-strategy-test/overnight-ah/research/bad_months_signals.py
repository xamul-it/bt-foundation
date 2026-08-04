#!/usr/bin/env python3
"""Fase 3 dello studio mesi negativi OvernightAH: segnali premonitori
ex-ante, confrontati tra "mese prima di un mese negativo" e "mese prima di
un mese normale", per segmento train/validation/oos.

Riusa il piu' possibile codice/dati esistenti (nessun nuovo backtest):
- `semis_monthly_features()` da `build_regime_switch_universes.py`, puntato
  a `daily_panel.csv` gia' generato.
- `daily_panel.csv` per intraday_vol, c2c_ret, known_ah_ret gia' calcolati.
- I mesi negativi e la composizione paniere di Fase 1/2.

Costruisce inoltre due segnali nuovi non presenti nel repo (verificato prima
di scrivere questo script): drawdown SPY 3m ex-ante (replica pandas di
`_passes_spy_monthly_gate` in overnight_ah.py) e breadth di mercato
(`pct_down_1pct`, quota di titoli Nasdaq scesi oltre soglia in giornata).

Convenzione ex-ante: `semis_monthly_features` e la replica `spy_dd3m` sono
gia' "il valore noto all'inizio del mese M" (usano solo `date < month`), si
uniscono quindi direttamente sulla colonna `month` del mese target. I segnali
basket-specific (dispersione, vol aggregata, breadth) descrivono invece cosa
e' successo DURANTE un mese completato: si calcolano per il mese M-1 e si
uniscono al mese target M con uno shift di un periodo.

Usage:
  bt-core/.venv/bin/python bt-strategy-test/overnight-ah/research/bad_months_signals.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_regime_switch_universes import semis_monthly_features  # noqa: E402
from edge_prediction_study import load_symbol  # noqa: E402

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
YAHOO_ADJ_DIR = ROOT / "config-common" / "data" / "d" / "yahoo_adj"

NEG_MONTHS_STATIC = OUT_DIR / "monthly_returns_static.csv"
NEG_MONTHS_SWITCH = OUT_DIR / "monthly_returns_switch.csv"
SYMBOL_CONTRIB_STATIC = OUT_DIR / "symbol_contribution_static.csv"
SYMBOL_CONTRIB_SWITCH = OUT_DIR / "symbol_contribution_switch.csv"

SPY_DD_LOOKBACK_DAYS = 63  # same as monthly_universe_spy_dd3m_threshold in overnight_ah.py
BREADTH_DOWN_THRESHOLD = -0.01  # -1%, per l'ipotesi utente


# --------------------------------------------------------------------- #
# Segnali market-wide (uguali per entrambe le policy)
# --------------------------------------------------------------------- #


def load_daily_panel() -> pd.DataFrame:
    return pd.read_csv(
        DAILY_PANEL,
        usecols=["date", "ticker", "c2c_ret", "known_ah_ret", "intraday_vol"],
        parse_dates=["date"],
    )


def spy_dd3m_monthly() -> pd.DataFrame:
    """Ex-ante replica of overnight_ah.py:_passes_spy_monthly_gate — rolling
    63-trading-day max close, drawdown of the last known close vs that peak,
    computed with only data strictly before month_start."""
    spy = load_symbol(YAHOO_ADJ_DIR / "SPY.csv", "SPY").sort_values("date")
    closes = spy.set_index("date")["close"]

    months = pd.date_range(
        closes.index.min().to_period("M").to_timestamp(),
        closes.index.max().to_period("M").to_timestamp(),
        freq="MS",
    )
    dates = closes.index.to_numpy()
    values = closes.to_numpy()
    rows = []
    for month in months:
        idx = np.searchsorted(dates, np.datetime64(month), side="left")
        window = values[max(0, idx - SPY_DD_LOOKBACK_DAYS) : idx]
        if len(window) == 0:
            continue
        peak = window.max()
        if peak <= 0:
            continue
        dd = window[-1] / peak - 1.0
        rows.append({"month": month.to_period("M"), "spy_dd3m": float(dd)})
    return pd.DataFrame(rows)


def breadth_monthly(panel: pd.DataFrame) -> pd.DataFrame:
    """Daily breadth = fraction of the full Nasdaq universe (daily_panel)
    down beyond BREADTH_DOWN_THRESHOLD, aggregated to the completed calendar
    month (mean daily breadth that month) — describes month M-1, joined to
    target month M with a shift by the caller."""
    daily = panel.copy()
    daily["pct_down_c2c"] = daily.groupby("date")["c2c_ret"].transform(
        lambda s: (s < BREADTH_DOWN_THRESHOLD).mean()
    )
    daily["pct_down_ah"] = daily.groupby("date")["known_ah_ret"].transform(
        lambda s: (s < BREADTH_DOWN_THRESHOLD).mean()
    )
    per_day = daily.drop_duplicates("date")[["date", "pct_down_c2c", "pct_down_ah"]]
    per_day["month"] = per_day["date"].dt.to_period("M")
    monthly = per_day.groupby("month").agg(
        breadth_pct_down_c2c_mean=("pct_down_c2c", "mean"),
        breadth_pct_down_ah_mean=("pct_down_ah", "mean"),
        breadth_pct_down_c2c_max=("pct_down_c2c", "max"),
    )
    return monthly.reset_index()


# --------------------------------------------------------------------- #
# Segnali basket-specific (traded universe per policy/mese)
# --------------------------------------------------------------------- #


def basket_signals_monthly(panel: pd.DataFrame, symbol_contrib: pd.DataFrame) -> pd.DataFrame:
    """Per il paniere effettivamente tradato ogni mese (da Fase 2): livello
    aggregato di volatilita' intraday e dispersione cross-sectional
    (deviazione standard cross-sectional media giornaliera dei c2c_ret),
    calcolati sul mese completato M-1."""
    contrib = symbol_contrib.copy()
    contrib["month_period"] = pd.PeriodIndex(contrib["month"], freq="M")

    panel = panel.copy()
    panel["month_period"] = panel["date"].dt.to_period("M")

    rows = []
    for month_period, tickers in contrib.groupby("month_period")["asset"].unique().items():
        sub = panel[(panel["month_period"] == month_period) & (panel["ticker"].isin(tickers))]
        if sub.empty:
            continue
        daily_std = sub.groupby("date")["c2c_ret"].std()
        rows.append(
            {
                "month": month_period,
                "n_traded_symbols": len(tickers),
                "basket_intraday_vol_mean": float(sub["intraday_vol"].mean()),
                "basket_cross_sectional_std_mean": float(daily_std.mean()) if not daily_std.empty else np.nan,
            }
        )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------- #
# Confronto distribuzioni
# --------------------------------------------------------------------- #


def compare_signal(
    negative_months: pd.DataFrame,
    signal: pd.DataFrame,
    signal_cols: list[str],
    ex_ante: bool,
) -> pd.DataFrame:
    """negative_months: colonne month (Timestamp), is_negative, segment.
    signal: colonne month (Period) + signal_cols. Se ex_ante=False, il
    segnale descrive il mese M-1: join su month-1."""
    nm = negative_months.copy()
    nm["month_period"] = pd.PeriodIndex(nm["month"], freq="M")
    nm["lookup_period"] = nm["month_period"] if ex_ante else nm["month_period"] - 1

    merged = nm.merge(signal, left_on="lookup_period", right_on="month", how="left", suffixes=("", "_sig"))

    rows = []
    for col in signal_cols:
        for segment in ["train", "validation", "oos", "all"]:
            sub = merged if segment == "all" else merged[merged["segment"] == segment]
            pos = sub.loc[sub["is_negative"], col].dropna()
            neg = sub.loc[~sub["is_negative"], col].dropna()
            if len(pos) < 3 or len(neg) < 3:
                rows.append(
                    {
                        "signal": col,
                        "segment": segment,
                        "n_before_negative": len(pos),
                        "n_before_normal": len(neg),
                        "mean_before_negative": float(pos.mean()) if len(pos) else np.nan,
                        "mean_before_normal": float(neg.mean()) if len(neg) else np.nan,
                        "mannwhitney_pvalue": np.nan,
                        "note": "campione troppo piccolo (<3) per il test",
                    }
                )
                continue
            try:
                mw = stats.mannwhitneyu(pos, neg, alternative="two-sided")
                pvalue = float(mw.pvalue)
            except ValueError:
                pvalue = np.nan
            rows.append(
                {
                    "signal": col,
                    "segment": segment,
                    "n_before_negative": len(pos),
                    "n_before_normal": len(neg),
                    "mean_before_negative": float(pos.mean()),
                    "mean_before_normal": float(neg.mean()),
                    "mannwhitney_pvalue": pvalue,
                    "note": "",
                }
            )
    return pd.DataFrame(rows)


def run(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    panel = load_daily_panel()

    semis = semis_monthly_features(DAILY_PANEL)
    semis["month"] = semis["month"].dt.to_period("M")
    semis_cols = [c for c in semis.columns if c != "month"]

    spy_dd = spy_dd3m_monthly()

    breadth = breadth_monthly(panel)
    breadth["month"] = breadth["month"]  # already Period

    market_signal = semis.merge(spy_dd, on="month", how="outer")
    market_signal_ex_ante_cols = semis_cols + ["spy_dd3m"]

    static_neg = pd.read_csv(NEG_MONTHS_STATIC, parse_dates=["month"])
    switch_neg = pd.read_csv(NEG_MONTHS_SWITCH, parse_dates=["month"])
    static_symbols = pd.read_csv(SYMBOL_CONTRIB_STATIC)
    switch_symbols = pd.read_csv(SYMBOL_CONTRIB_SWITCH)

    static_basket = basket_signals_monthly(panel, static_symbols)
    switch_basket = basket_signals_monthly(panel, switch_symbols)

    results = []
    results.append(
        compare_signal(static_neg, market_signal, market_signal_ex_ante_cols, ex_ante=True).assign(policy="static")
    )
    results.append(
        compare_signal(switch_neg, market_signal, market_signal_ex_ante_cols, ex_ante=True).assign(policy="switch")
    )
    results.append(
        compare_signal(
            static_neg,
            breadth,
            ["breadth_pct_down_c2c_mean", "breadth_pct_down_ah_mean", "breadth_pct_down_c2c_max"],
            ex_ante=False,
        ).assign(policy="static")
    )
    results.append(
        compare_signal(
            switch_neg,
            breadth,
            ["breadth_pct_down_c2c_mean", "breadth_pct_down_ah_mean", "breadth_pct_down_c2c_max"],
            ex_ante=False,
        ).assign(policy="switch")
    )
    results.append(
        compare_signal(
            static_neg,
            static_basket,
            ["basket_intraday_vol_mean", "basket_cross_sectional_std_mean"],
            ex_ante=False,
        ).assign(policy="static")
    )
    results.append(
        compare_signal(
            switch_neg,
            switch_basket,
            ["basket_intraday_vol_mean", "basket_cross_sectional_std_mean"],
            ex_ante=False,
        ).assign(policy="switch")
    )

    signal_separation = pd.concat(results, ignore_index=True)
    signal_separation = signal_separation[
        ["policy", "signal", "segment", "n_before_negative", "n_before_normal",
         "mean_before_negative", "mean_before_normal", "mannwhitney_pvalue", "note"]
    ]
    signal_separation.to_csv(out_dir / "signal_separation_by_segment.csv", index=False)

    market_signal.assign(month=lambda d: d["month"].astype(str)).to_csv(
        out_dir / "signal_candidates_monthly_market.csv", index=False
    )
    breadth.assign(month=lambda d: d["month"].astype(str)).to_csv(
        out_dir / "signal_candidates_monthly_breadth.csv", index=False
    )
    static_basket.assign(month=lambda d: d["month"].astype(str)).to_csv(
        out_dir / "signal_candidates_monthly_basket_static.csv", index=False
    )
    switch_basket.assign(month=lambda d: d["month"].astype(str)).to_csv(
        out_dir / "signal_candidates_monthly_basket_switch.csv", index=False
    )

    # Robust candidates: separation consistent (same sign, p<0.20 given small
    # sample per spec constraints) on BOTH train and validation. OOS looked
    # at only for confirmation, never for selection (see plan Verifica).
    def sign(x: float) -> float:
        return np.sign(x) if pd.notna(x) else np.nan

    pivot = signal_separation[signal_separation["segment"].isin(["train", "validation"])].copy()
    pivot["diff"] = pivot["mean_before_negative"] - pivot["mean_before_normal"]
    robust_rows = []
    for (policy, sig), g in pivot.groupby(["policy", "signal"]):
        g = g.set_index("segment")
        if "train" not in g.index or "validation" not in g.index:
            continue
        train_row, val_row = g.loc["train"], g.loc["validation"]
        same_sign = pd.notna(train_row["diff"]) and pd.notna(val_row["diff"]) and (
            sign(train_row["diff"]) == sign(val_row["diff"]) and sign(train_row["diff"]) != 0
        )
        both_suggestive = (
            pd.notna(train_row["mannwhitney_pvalue"])
            and pd.notna(val_row["mannwhitney_pvalue"])
            and train_row["mannwhitney_pvalue"] < 0.20
            and val_row["mannwhitney_pvalue"] < 0.20
        )
        robust_rows.append(
            {
                "policy": policy,
                "signal": sig,
                "train_diff": train_row["diff"],
                "train_pvalue": train_row["mannwhitney_pvalue"],
                "validation_diff": val_row["diff"],
                "validation_pvalue": val_row["mannwhitney_pvalue"],
                "same_sign_train_val": bool(same_sign),
                "both_pvalue_lt_020": bool(both_suggestive),
                "robust_candidate": bool(same_sign and both_suggestive),
            }
        )
    robust = pd.DataFrame(robust_rows).sort_values(
        ["robust_candidate", "policy", "signal"], ascending=[False, True, True]
    )
    robust.to_csv(out_dir / "signal_robust_candidates.csv", index=False)

    report = []
    report.append("# Fase 3 — Segnali premonitori")
    report.append("")
    report.append(
        "Frequenza rifiuti margine (verificata a costo quasi zero, grep diretto "
        "su `orderhistory.json`/`orders.json`): **0 eventi Margin/Rejected su "
        "tutti e 6 i segmenti** (static train/val/oos, weak_theme_switch "
        "train/val/oos). Segnale non applicabile per questo studio — coerente "
        "col fix broker gia' applicato (`lessons_bt_broker_margin_reject`)."
    )
    report.append("")
    report.append("## Candidati robusti (stesso segno train/validation, entrambi p<0.20)")
    robust_hits = robust[robust["robust_candidate"]]
    if robust_hits.empty:
        report.append("Nessun segnale soddisfa entrambi i criteri su train E validation.")
    else:
        report.append(robust_hits.to_markdown(index=False))
    report.append("")
    report.append("## Tabella completa segnali per segmento")
    report.append(signal_separation.to_markdown(index=False))
    (out_dir / "summary_fase3.md").write_text("\n".join(report))

    print(f"wrote {out_dir}")
    print("robust candidates:")
    print(robust.to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fase 3: segnali premonitori OvernightAH")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args().out_dir)
