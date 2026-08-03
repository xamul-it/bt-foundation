#!/usr/bin/env python3
"""Counter-cyclical AH symbol study for OvernightAH — daily granularity.

Question: are there symbols whose daily AH return (open[t]/close[t-1]-1) is
negatively correlated with the static basket (`stable_ah_top10`) or with the
semis factor basket used by `weak_theme_switch`? I.e. symbols that tend to
have a positive overnight move on nights where the deployed basket has a bad
one.

v1 of this study used the monthly `feature_target_panel.csv` from
`edge_prediction_study.py` and found no robust candidate, but that panel
defaults to `--start 2016-01-04` (a script default, not a data limit) and
aggregates to ~30-120 monthly observations per ticker after the
`target_trades>=3` filter — too thin to trust a correlation. Most static-list
members and most candidate large-caps have `yahoo_adj` daily data back to
2000 (some individual names are naturally shorter: AVGO from 2009, MELI from
2007, CEG from 2022, ARM from 2023 — this is a real data-availability limit,
not a script choice).

This version reads `config-common/data/d/yahoo_adj/*.csv` directly and
correlates raw daily AH returns, starting 2000-01-01 by default, giving
~2000-6600 daily observations per ticker instead of ~30-120 monthly ones.

This is still a realized-return correlation study, not an ex-ante tradable
signal. Per `docs/context/strategy-analysis-methodology.md`, promoting any
candidate requires turning it into a trailing/known-before feature and
validating through Backtrader (`btmain.py`).

Usage:
  bt-core/.venv/bin/python bt-strategy-test/overnight-ah/research/counter_cyclical_symbol_study.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from edge_prediction_study import load_symbol  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TICKER_FILE = ROOT / "config-common" / "tickers" / "yahoo_adj_research_universe.json"
DEFAULT_STATIC_FILE = ROOT / "config-common" / "tickers" / "stable_ah_top10.json"
DEFAULT_DATA_DIR = ROOT / "config-common" / "data" / "d" / "yahoo_adj"
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "counter_cyclical_symbol_study_daily"

DEFAULT_SEMIS = [
    "NVDA", "AMD", "AVGO", "MU", "ASML", "MRVL", "ARM", "AMAT",
    "LRCX", "KLAC", "MCHP", "ADI", "TXN", "ON", "INTC", "GFS",
]


def load_ticker_list(path: Path) -> list[str]:
    return [str(x).strip().upper() for x in json.loads(path.read_text()) if str(x).strip().upper() != "SPY"]


def load_daily_ah_returns(data_dir: Path, tickers: list[str], start: str, end: str | None) -> tuple[pd.DataFrame, list[str]]:
    frames = []
    missing = []
    for ticker in tickers:
        path = data_dir / f"{ticker}.csv"
        if not path.exists():
            missing.append(ticker)
            continue
        df = load_symbol(path, ticker)
        df = df.sort_values("date")
        df["prev_close"] = df["close"].shift(1)
        df["known_ah_ret"] = df["open"] / df["prev_close"] - 1.0
        frames.append(df[["date", "ticker", "known_ah_ret"]])
    out = pd.concat(frames, ignore_index=True)
    out = out[out["date"] >= pd.Timestamp(start)]
    if end:
        out = out[out["date"] <= pd.Timestamp(end)]
    out = out.dropna(subset=["known_ah_ret"])
    return out, missing


def build_basket_series(daily: pd.DataFrame, members: list[str], min_members: int) -> pd.DataFrame:
    sub = daily[daily["ticker"].isin(members)]
    grouped = sub.groupby("date")["known_ah_ret"]
    counts = grouped.count()
    means = grouped.mean()
    out = pd.DataFrame({"n_members": counts, "basket_ret": means})
    out.loc[out["n_members"] < min_members, "basket_ret"] = np.nan
    return out.reset_index()


def segment_mask(dates: pd.Series, start: str | None, end: str | None) -> pd.Series:
    mask = pd.Series(True, index=dates.index)
    if start:
        mask &= dates >= pd.Timestamp(start)
    if end:
        mask &= dates <= pd.Timestamp(end)
    return mask


def correlation_table(
    daily: pd.DataFrame,
    basket: pd.DataFrame,
    basket_name: str,
    exclude: set[str],
    segments: dict[str, tuple[str | None, str | None]],
    min_overlap_days: int,
) -> pd.DataFrame:
    basket_series = basket.set_index("date")["basket_ret"]
    rows = []
    candidates = sorted(t for t in daily["ticker"].unique() if t not in exclude)
    for ticker in candidates:
        series = daily[daily["ticker"] == ticker].set_index("date")["known_ah_ret"]
        joined = pd.concat([series.rename("ticker_val"), basket_series.rename("basket_val")], axis=1).dropna()
        if len(joined) < min_overlap_days:
            continue
        row = {"ticker": ticker, "basket": basket_name, "n_days_full": len(joined)}
        for seg_name, (start, end) in segments.items():
            seg = joined[segment_mask(joined.index.to_series(), start, end)]
            min_seg = max(60, min_overlap_days // 8)
            if len(seg) < min_seg:
                row[f"pearson_{seg_name}"] = np.nan
                row[f"spearman_{seg_name}"] = np.nan
                row[f"n_{seg_name}"] = len(seg)
                continue
            row[f"pearson_{seg_name}"] = float(seg["ticker_val"].corr(seg["basket_val"], method="pearson"))
            row[f"spearman_{seg_name}"] = float(seg["ticker_val"].corr(seg["basket_val"], method="spearman"))
            row[f"n_{seg_name}"] = len(seg)
        rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("pearson_full", ascending=True)


def yearly_correlation_table(
    daily: pd.DataFrame,
    basket: pd.DataFrame,
    basket_name: str,
    tickers: list[str],
    min_days_per_year: int,
) -> pd.DataFrame:
    basket_series = basket.set_index("date")["basket_ret"]
    rows = []
    for ticker in tickers:
        series = daily[daily["ticker"] == ticker].set_index("date")["known_ah_ret"]
        joined = pd.concat([series.rename("ticker_val"), basket_series.rename("basket_val")], axis=1).dropna()
        if joined.empty:
            continue
        joined["year"] = joined.index.year
        for year, g in joined.groupby("year"):
            if len(g) < min_days_per_year:
                continue
            rows.append(
                {
                    "ticker": ticker,
                    "basket": basket_name,
                    "year": int(year),
                    "n_days": len(g),
                    "pearson": float(g["ticker_val"].corr(g["basket_val"], method="pearson")),
                }
            )
    return pd.DataFrame(rows)


def regime_conditional_table(
    daily: pd.DataFrame,
    basket: pd.DataFrame,
    basket_name: str,
    exclude: set[str],
    bad_threshold: float,
    min_bad_days: int,
) -> pd.DataFrame:
    basket_series = basket.set_index("date")["basket_ret"]
    bad_days = set(basket_series[basket_series < bad_threshold].index)
    good_days = set(basket_series[basket_series >= bad_threshold].dropna().index) - bad_days
    rows = []
    candidates = sorted(t for t in daily["ticker"].unique() if t not in exclude)
    for ticker in candidates:
        series = daily[daily["ticker"] == ticker].set_index("date")["known_ah_ret"]
        bad_vals = series[series.index.isin(bad_days)] * 10000.0
        good_vals = series[series.index.isin(good_days)] * 10000.0
        if len(bad_vals) < min_bad_days:
            continue
        row = {
            "ticker": ticker,
            "basket": basket_name,
            "n_bad_days": len(bad_vals),
            "n_good_days": len(good_vals),
            "bad_day_mean_bps": float(bad_vals.mean()),
            "bad_day_winratio": float((bad_vals > 0).mean()),
            "good_day_mean_bps": float(good_vals.mean()) if len(good_vals) else np.nan,
            "hedge_spread_bps": float(bad_vals.mean() - good_vals.mean()) if len(good_vals) else np.nan,
        }
        rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("bad_day_mean_bps", ascending=False)


def run(args: argparse.Namespace) -> None:
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    universe = load_ticker_list(args.ticker_file)
    static_members = load_ticker_list(args.static_file)
    semis_members = args.semis_tickers
    all_tickers = sorted(set(universe) | set(static_members) | set(semis_members))

    daily, missing = load_daily_ah_returns(args.data_dir, all_tickers, args.start, args.end)

    static_basket = build_basket_series(daily, static_members, args.min_basket_members)
    semis_basket = build_basket_series(daily, semis_members, args.min_basket_members)
    static_basket.to_csv(out_dir / "static_basket_daily.csv", index=False)
    semis_basket.to_csv(out_dir / "semis_basket_daily.csv", index=False)

    segments = {
        "full": (None, None),
        "train": (args.start, args.train_end),
        "validation": (args.validation_start, args.validation_end),
        "oos": (args.oos_start, None),
    }

    candidate_universe = [t for t in universe if t != "SPY"]

    corr_frames = []
    regime_frames = []
    yearly_frames = []
    for basket_name, basket_df, members in [
        ("static", static_basket, static_members),
        ("semis", semis_basket, semis_members),
    ]:
        exclude = set(members) | {"SPY"}
        corr_frames.append(
            correlation_table(daily, basket_df, basket_name, exclude, segments, args.min_overlap_days)
        )
        regime_frames.append(
            regime_conditional_table(daily, basket_df, basket_name, exclude, args.bad_day_threshold, args.min_bad_days)
        )
        yearly_frames.append(
            yearly_correlation_table(
                daily, basket_df, basket_name,
                [t for t in candidate_universe if t not in exclude],
                args.min_days_per_year,
            )
        )

    corr_table = pd.concat([d for d in corr_frames if not d.empty], ignore_index=True) if any(len(d) for d in corr_frames) else pd.DataFrame()
    regime_table = pd.concat([d for d in regime_frames if not d.empty], ignore_index=True) if any(len(d) for d in regime_frames) else pd.DataFrame()
    yearly_table = pd.concat([d for d in yearly_frames if not d.empty], ignore_index=True) if any(len(d) for d in yearly_frames) else pd.DataFrame()
    corr_table.to_csv(out_dir / "symbol_basket_correlation_daily.csv", index=False)
    regime_table.to_csv(out_dir / "symbol_regime_conditional_daily.csv", index=False)
    yearly_table.to_csv(out_dir / "symbol_basket_correlation_yearly.csv", index=False)

    # Stability check: how many years (out of the years with enough data) does
    # a candidate stay negatively correlated? A robust "hedge" should not flip
    # sign every other year.
    stability_rows = []
    if not yearly_table.empty:
        for (ticker, basket_name), g in yearly_table.groupby(["ticker", "basket"]):
            n_years = len(g)
            n_negative = int((g["pearson"] < 0).sum())
            stability_rows.append(
                {
                    "ticker": ticker,
                    "basket": basket_name,
                    "n_years": n_years,
                    "n_negative_years": n_negative,
                    "negative_year_pct": n_negative / n_years if n_years else np.nan,
                    "mean_pearson": float(g["pearson"].mean()),
                    "std_pearson": float(g["pearson"].std(ddof=1)) if n_years > 1 else np.nan,
                }
            )
    stability_table = pd.DataFrame(stability_rows)
    if not stability_table.empty:
        stability_table = stability_table.sort_values(["negative_year_pct", "mean_pearson"], ascending=[False, True])
    stability_table.to_csv(out_dir / "symbol_correlation_stability.csv", index=False)

    report = []
    report.append("# Counter-cyclical AH symbol study — daily granularity")
    report.append("")
    report.append(f"Data: `{args.data_dir}`; start={args.start}; end={args.end or 'latest'}; missing tickers: {missing}")
    report.append(f"Static basket ({len(static_members)}): {', '.join(static_members)}")
    report.append(f"Semis basket ({len(semis_members)}): {', '.join(semis_members)}")
    report.append(
        f"Filters: min_basket_members={args.min_basket_members}, min_overlap_days={args.min_overlap_days}, "
        f"bad_day_threshold={args.bad_day_threshold}, min_bad_days={args.min_bad_days}, "
        f"min_days_per_year={args.min_days_per_year}"
    )
    report.append("")

    for basket_name in ["static", "semis"]:
        if corr_table.empty:
            continue
        sub = corr_table[corr_table["basket"] == basket_name].sort_values("pearson_full").head(15)
        report.append(f"## Most negatively correlated vs {basket_name} basket (daily, full history)")
        report.append(sub.to_markdown(index=False))
        report.append("")

    for basket_name in ["static", "semis"]:
        if stability_table.empty:
            continue
        sub = stability_table[stability_table["basket"] == basket_name].head(15)
        report.append(f"## Most consistently negative year-by-year vs {basket_name} basket")
        report.append(sub.to_markdown(index=False))
        report.append("")

    for basket_name in ["static", "semis"]:
        if regime_table.empty:
            continue
        sub = regime_table[regime_table["basket"] == basket_name].sort_values("bad_day_mean_bps", ascending=False).head(15)
        report.append(f"## Best mean AH return specifically on {basket_name}-basket bad days (threshold {args.bad_day_threshold:.2%})")
        report.append(sub.to_markdown(index=False))
        report.append("")

    report.append(
        "Note: still a realized-return correlation study on raw AH returns "
        "(not filtered by OvernightAH's own entry filters, and not an ex-ante "
        "feature). A candidate must be re-expressed as a trailing/known-before "
        "feature and re-validated through Backtrader before any promotion."
    )
    (out_dir / "summary.md").write_text("\n".join(report))

    print(f"wrote {out_dir}")
    if not corr_table.empty:
        print("Most negative full-history correlation vs static basket:")
        print(corr_table[corr_table["basket"] == "static"].sort_values("pearson_full").head(10).to_string(index=False))
    if not stability_table.empty:
        print("Most consistently negative year-by-year vs static basket:")
        print(stability_table[stability_table["basket"] == "static"].head(10).to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OvernightAH counter-cyclical symbol study (daily)")
    parser.add_argument("--ticker-file", type=Path, default=DEFAULT_TICKER_FILE)
    parser.add_argument("--static-file", type=Path, default=DEFAULT_STATIC_FILE)
    parser.add_argument("--semis-tickers", nargs="+", default=DEFAULT_SEMIS)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--start", default="2000-01-01")
    parser.add_argument("--end", default=None)
    parser.add_argument("--train-end", default="2020-12-31")
    parser.add_argument("--validation-start", default="2021-01-01")
    parser.add_argument("--validation-end", default="2023-12-31")
    parser.add_argument("--oos-start", default="2024-01-01")
    parser.add_argument("--min-basket-members", type=int, default=5)
    parser.add_argument("--min-overlap-days", type=int, default=500)
    parser.add_argument("--min-days-per-year", type=int, default=150)
    parser.add_argument("--bad-day-threshold", type=float, default=-0.01)
    parser.add_argument("--min-bad-days", type=int, default=60)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
