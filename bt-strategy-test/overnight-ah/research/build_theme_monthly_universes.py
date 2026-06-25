#!/usr/bin/env python3
"""Build thematic monthly universes for OvernightAH.

The base deployable score remains the ex-ante c2c/AH momentum blend. Theme
scores are meant to test whether a small semiconductor/AI exposure bonus
improves that baseline without replacing it.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
RESEARCH_OUT = ROOT / "bt-strategy-test" / "overnight-ah" / "research" / "out"
DEFAULT_PANEL = RESEARCH_OUT / "edge_prediction_study_all_adj" / "feature_target_panel.csv"
DEFAULT_DAILY = RESEARCH_OUT / "edge_prediction_study_all_adj" / "daily_panel.csv"
DEFAULT_OUT = RESEARCH_OUT / "edge_prediction_study_all_adj" / "monthly_universes_theme_weak"

SEMIS = [
    "NVDA",
    "AMD",
    "AVGO",
    "MU",
    "ASML",
    "MRVL",
    "ARM",
    "AMAT",
    "LRCX",
    "KLAC",
    "MCHP",
    "ADI",
    "TXN",
    "ON",
    "INTC",
    "GFS",
]


@dataclass(frozen=True)
class WindowSpec:
    name: str
    days: int


WINDOWS = [WindowSpec("3m", 63), WindowSpec("6m", 126), WindowSpec("12m", 252)]


def monthly_rank(series: pd.Series) -> pd.Series:
    return series.rank(pct=True, method="average")


def add_base_scores(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    out["rank_c2c6"] = out.groupby("month")["c2c_mean_6m"].transform(monthly_rank)
    out["rank_ah6"] = out.groupby("month")["ah_mean_6m"].transform(monthly_rank)
    out["score_c2c60_ah40"] = 0.6 * out["rank_c2c6"] + 0.4 * out["rank_ah6"]
    return out


def load_daily(path: Path) -> pd.DataFrame:
    cols = ["date", "ticker", "close", "c2c_ret"]
    daily = pd.read_csv(path, usecols=cols, parse_dates=["date"])
    daily = daily.dropna(subset=["date", "ticker"])
    daily["c2c_ret"] = pd.to_numeric(daily["c2c_ret"], errors="coerce")
    daily["close"] = pd.to_numeric(daily["close"], errors="coerce")
    return daily.sort_values(["date", "ticker"])


def semis_factor(daily: pd.DataFrame) -> pd.DataFrame:
    semis_daily = daily[daily["ticker"].isin(SEMIS)]
    factor = (
        semis_daily.groupby("date", as_index=False)["c2c_ret"]
        .mean()
        .rename(columns={"c2c_ret": "semis_factor_ret"})
    )
    return factor.dropna(subset=["semis_factor_ret"])


def spy_drawdown_gate(daily: pd.DataFrame, months: list[pd.Timestamp], window_days: int = 63, threshold: float = -0.10) -> pd.DataFrame:
    spy = daily[daily["ticker"] == "SPY"][["date", "close"]].dropna().sort_values("date")
    rows = []
    for month in months:
        hist = spy[spy["date"] < month].tail(window_days)
        if hist.empty:
            drawdown = np.nan
            pass_gate = True
        else:
            last_close = float(hist["close"].iloc[-1])
            peak = float(hist["close"].max())
            drawdown = last_close / peak - 1.0 if peak else np.nan
            pass_gate = bool(pd.isna(drawdown) or drawdown > threshold)
        rows.append({"month": month, "spy_dd3m": drawdown, "spy_dd3m_pass": pass_gate})
    return pd.DataFrame(rows)


def rolling_theme_features(panel: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    factor = semis_factor(daily)
    merged = daily[["date", "ticker", "c2c_ret"]].merge(factor, on="date", how="left")
    merged = merged.dropna(subset=["c2c_ret", "semis_factor_ret"])

    months = sorted(pd.Timestamp(m) for m in panel["month"].dropna().unique())
    tickers = sorted(panel["ticker"].dropna().unique())
    rows = []
    for month in months:
        hist = merged[merged["date"] < month]
        for ticker in tickers:
            ticker_hist = hist[hist["ticker"] == ticker]
            row = {"month": month, "ticker": ticker, "is_semis": ticker in SEMIS}
            for spec in WINDOWS:
                window = ticker_hist.tail(spec.days)
                if len(window) < max(20, spec.days // 3):
                    corr = np.nan
                    beta = np.nan
                else:
                    x = window["semis_factor_ret"].astype(float)
                    y = window["c2c_ret"].astype(float)
                    var = float(x.var(ddof=1))
                    corr = float(y.corr(x)) if x.std(ddof=1) and y.std(ddof=1) else np.nan
                    beta = float(y.cov(x) / var) if var else np.nan
                row[f"semis_corr_{spec.name}"] = corr
                row[f"semis_beta_{spec.name}"] = beta
            rows.append(row)
    return pd.DataFrame(rows)


def add_theme_scores(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    for col in ["semis_corr_3m", "semis_corr_6m", "semis_corr_12m", "semis_beta_3m", "semis_beta_6m", "semis_beta_12m"]:
        out[f"rank_{col}"] = out.groupby("month")[col].transform(monthly_rank)
    out["rank_semis_member"] = out["is_semis"].astype(float)

    out["theme_corr6"] = out["rank_semis_corr_6m"]
    out["theme_corr12"] = out["rank_semis_corr_12m"]
    out["theme_beta12"] = out["rank_semis_beta_12m"]
    out["theme_structural"] = (
        0.50 * out["rank_semis_corr_12m"].fillna(0.5)
        + 0.30 * out["rank_semis_beta_12m"].fillna(0.5)
        + 0.20 * out["rank_semis_member"]
    )

    for theme_col in ["theme_corr12", "theme_beta12", "theme_structural"]:
        for weight in [0.05, 0.10, 0.15]:
            pct = int(round(weight * 100))
            out[f"score_base{100 - pct}_{theme_col}{pct}"] = (
                (1.0 - weight) * out["score_c2c60_ah40"]
                + weight * out[theme_col].fillna(0.5)
            )
    return out


def write_universe(out_dir: Path, feature: str, top_n: int, scored: pd.DataFrame, gated: bool) -> dict[str, object]:
    rows = []
    details = []
    for month, group in scored.groupby("month"):
        g = group.copy()
        if gated and not bool(g["spy_dd3m_pass"].iloc[0]):
            chosen = g.iloc[0:0]
        else:
            chosen = g.dropna(subset=[feature]).sort_values([feature, "ticker"], ascending=[False, True]).head(top_n)
        symbols = chosen["ticker"].tolist()
        rows.append({"year": pd.Timestamp(month).year, "month": pd.Timestamp(month).month, "symbols": ",".join(symbols)})
        for rank, row in enumerate(chosen.itertuples(index=False), start=1):
            details.append(
                {
                    "month": pd.Timestamp(month),
                    "rank": rank,
                    "ticker": row.ticker,
                    "feature": feature,
                    "score": getattr(row, feature),
                    "spy_dd3m": row.spy_dd3m,
                }
            )

    suffix = "_gate_spy_dd3m_gt_neg10" if gated else ""
    base = f"{feature}_top{top_n}{suffix}"
    universe_path = out_dir / f"{base}.csv"
    detail_path = out_dir / f"{base}_detail.csv"
    pd.DataFrame(rows).to_csv(universe_path, sep=";", index=False)
    pd.DataFrame(details).to_csv(detail_path, index=False)
    return {
        "feature": f"{feature}{suffix}",
        "top_n": top_n,
        "monthly_universe_file": str(universe_path.resolve().relative_to(ROOT)),
        "detail_file": str(detail_path.resolve().relative_to(ROOT)),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build weak thematic OvernightAH monthly universes")
    parser.add_argument("--panel", type=Path, default=DEFAULT_PANEL)
    parser.add_argument("--daily", type=Path, default=DEFAULT_DAILY)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--top-n", type=int, nargs="+", default=[40, 50, 60])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    panel = pd.read_csv(args.panel, parse_dates=["month"])
    panel = add_base_scores(panel)
    daily = load_daily(args.daily)
    theme = rolling_theme_features(panel, daily)
    scored = panel.merge(theme, on=["month", "ticker"], how="left")
    gates = spy_drawdown_gate(daily, sorted(pd.Timestamp(m) for m in scored["month"].dropna().unique()))
    scored = scored.merge(gates, on="month", how="left")
    scored = add_theme_scores(scored)
    scored.to_csv(args.out_dir / "theme_weak_scores.csv", index=False)

    score_cols = ["score_c2c60_ah40"]
    score_cols.extend([c for c in scored.columns if c.startswith("score_base")])

    universe_index = []
    for score_col in score_cols:
        for top_n in args.top_n:
            universe_index.append(write_universe(args.out_dir, score_col, top_n, scored, gated=False))
            universe_index.append(write_universe(args.out_dir, score_col, top_n, scored, gated=True))

    index = pd.DataFrame(universe_index)
    index.to_csv(args.out_dir / "index.csv", index=False)
    print(args.out_dir / "index.csv")
    print(index.head(30).to_string(index=False))


if __name__ == "__main__":
    main()
