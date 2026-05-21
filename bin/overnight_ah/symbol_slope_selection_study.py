#!/usr/bin/env python3
"""
symbol_slope_selection_study.py - Dynamic OvernightAH symbol selection.

Daily simulation:
  1. Load a broad universe from local daily cache.
  2. For each symbol/date, compute known AH returns up to the current open:
       known_ah[t] = open[t] / close[t-1] - 1
  3. Rank symbols by rolling linear-regression slope on cumulative known AH.
  4. Apply liquidity, AH stability, and current intraday volatility filters.
  5. Enter at close[t], exit at open[t+1].

All signals for date t use information available before the MOC entry on date t.
The script is a research simulator and does not change the live strategy.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_TICKERS = ROOT / "config-common" / "tickers" / "NASDAQ_100_US.json"
DEFAULT_BASELINE = ROOT / "config-common" / "tickers" / "stable_ah_top10.json"
DEFAULT_DATA = ROOT / "config-common" / "data" / "d" / "yahoo"
DEFAULT_OUT = Path(__file__).resolve().parent / "out"


def load_tickers(path: Path) -> list[str]:
    with open(path) as f:
        return [str(t).strip() for t in json.load(f) if str(t).strip() and str(t).strip() != "SPY"]


def _find_col(df: pd.DataFrame, names: list[str]) -> str | None:
    lookup = {str(c).lower(): c for c in df.columns}
    for name in names:
        if name.lower() in lookup:
            return lookup[name.lower()]
    return None


def load_symbol(path: Path, ticker: str, start: pd.Timestamp) -> pd.DataFrame:
    raw = pd.read_csv(path)
    date_col = _find_col(raw, ["Date", "timestamp", "datetime", "date"]) or raw.columns[0]
    raw[date_col] = pd.to_datetime(raw[date_col], utc=True, errors="coerce")
    raw = raw.dropna(subset=[date_col]).sort_values(date_col)
    raw["date"] = raw[date_col].dt.tz_localize(None).dt.normalize()
    raw = raw[raw["date"] >= start].copy()

    rename = {}
    for col in raw.columns:
        key = str(col).strip().lower().replace(" ", "_")
        if key in {"open", "high", "low", "close", "volume"}:
            rename[col] = key
        elif key in {"adj_close", "adjclose"}:
            rename[col] = "adj_close"
    raw = raw.rename(columns=rename)
    if not {"open", "high", "low", "close", "volume"}.issubset(raw.columns):
        return pd.DataFrame()

    for col in ["open", "high", "low", "close", "volume", "adj_close"]:
        if col in raw.columns:
            raw[col] = pd.to_numeric(raw[col], errors="coerce")
    raw = raw.dropna(subset=["open", "high", "low", "close", "volume"])
    if raw.empty:
        return pd.DataFrame()

    raw["ticker"] = ticker
    raw["rth_vol"] = (raw["high"] - raw["low"]) / raw["open"]
    raw["rth_ret"] = (raw["close"] - raw["open"]) / raw["open"]
    raw["ah_ret"] = (raw["open"].shift(-1) - raw["close"]) / raw["close"]
    raw["known_ah"] = (raw["open"] - raw["close"].shift(1)) / raw["close"].shift(1)
    raw["dollar_volume"] = raw["close"] * raw["volume"]
    raw["adv20"] = raw["dollar_volume"].rolling(20).median().shift(1)

    # Indicator return uses adjusted close if present. Open is adjusted with the
    # same same-day factor, enough to avoid split/dividend breaks in features.
    if "adj_close" in raw.columns:
        factor = raw["adj_close"] / raw["close"].replace(0, np.nan)
        adj_open = raw["open"] * factor
        raw["ind_known_ah"] = (adj_open - raw["adj_close"].shift(1)) / raw["adj_close"].shift(1)
    else:
        raw["ind_known_ah"] = raw["known_ah"]

    return raw


def rolling_slope_features(returns: pd.Series, window: int) -> pd.DataFrame:
    arr = returns.to_numpy(float)
    slope = np.full(len(arr), np.nan)
    tstat = np.full(len(arr), np.nan)
    r2 = np.full(len(arr), np.nan)
    mean = np.full(len(arr), np.nan)
    std = np.full(len(arr), np.nan)
    hit = np.full(len(arr), np.nan)
    maxdd = np.full(len(arr), np.nan)

    x = np.arange(window, dtype=float)
    x_center = x - x.mean()
    sxx = float((x_center ** 2).sum())

    for i in range(window - 1, len(arr)):
        w = arr[i - window + 1 : i + 1]
        if np.isnan(w).any():
            continue
        y = np.cumsum(w)
        y_center = y - y.mean()
        beta = float((x_center * y_center).sum() / sxx)
        fitted = y.mean() + beta * x_center
        resid = y - fitted
        ss_res = float((resid ** 2).sum())
        ss_tot = float(((y - y.mean()) ** 2).sum())
        se2 = ss_res / (window - 2) if window > 2 else np.nan
        stderr = math.sqrt(se2 / sxx) if se2 >= 0 else np.nan
        eq = np.cumprod(1 + w)
        dd = eq / np.maximum.accumulate(eq) - 1

        slope[i] = beta
        tstat[i] = beta / stderr if stderr and stderr > 0 else np.nan
        r2[i] = 1 - ss_res / ss_tot if ss_tot > 0 else np.nan
        mean[i] = float(np.mean(w))
        std[i] = float(np.std(w, ddof=1))
        hit[i] = float(np.mean(w > 0))
        maxdd[i] = float(np.min(dd))

    # Shift one bar: decision at date t can use known_ah through t-1 only.
    return pd.DataFrame(
        {
            "slope": slope,
            "tstat": tstat,
            "r2": r2,
            "ah_mean": mean,
            "ah_std": std,
            "hit_rate": hit,
            "ah_maxdd": maxdd,
        },
        index=returns.index,
    ).shift(1)


def build_feature_panel(tickers: list[str], data_dir: Path, start: str, window: int) -> pd.DataFrame:
    start_ts = pd.Timestamp(start) - pd.Timedelta(days=max(260, window * 4))
    frames = []
    for ticker in tickers:
        path = data_dir / f"{ticker}.csv"
        if not path.exists():
            print(f"[warn] missing {ticker}: {path}")
            continue
        df = load_symbol(path, ticker, start_ts)
        if df.empty:
            print(f"[warn] unusable {ticker}: {path}")
            continue
        feats = rolling_slope_features(df["ind_known_ah"], window)
        df = pd.concat([df.reset_index(drop=True), feats.reset_index(drop=True)], axis=1)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    panel = pd.concat(frames).reset_index(drop=True)
    return panel[panel["date"] >= pd.Timestamp(start)].copy()


def simulate(
    panel: pd.DataFrame,
    universe_order: list[str],
    top: int,
    min_vol: float,
    max_vol: float,
    min_adv: float,
    min_price: float,
    ah_lag1_threshold: float,
    min_slope: float,
    min_tstat: float,
    min_r2: float,
    min_hit: float,
    max_ah_dd: float,
    max_ah_std: float | None,
    score_mode: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    order = {ticker: i for i, ticker in enumerate(universe_order)}
    rows = []
    selected_rows = []

    for date, day in panel.groupby("date"):
        candidates = day[
            (day["rth_vol"] >= min_vol)
            & (day["rth_vol"] <= max_vol)
            & (day["adv20"] >= min_adv)
            & (day["close"] >= min_price)
            & (day["known_ah"] >= ah_lag1_threshold)
            & (day["slope"] >= min_slope)
            & (day["tstat"] >= min_tstat)
            & (day["r2"] >= min_r2)
            & (day["hit_rate"] >= min_hit)
            & (day["ah_maxdd"] >= max_ah_dd)
        ].copy()
        if max_ah_std is not None:
            candidates = candidates[candidates["ah_std"] <= max_ah_std]
        candidates = candidates.dropna(subset=["ah_ret", "slope", "tstat", "r2"])
        if candidates.empty:
            continue

        if score_mode == "slope":
            candidates["score"] = candidates["slope"]
        elif score_mode == "slope_r2":
            candidates["score"] = candidates["slope"] * candidates["r2"].clip(lower=0)
        elif score_mode == "slope_t":
            candidates["score"] = candidates["slope"] * candidates["tstat"].clip(lower=0)
        else:
            candidates["score"] = candidates["slope"] * candidates["r2"].clip(lower=0) * candidates["hit_rate"]

        candidates["order"] = candidates["ticker"].map(order)
        selected = candidates.sort_values(["score", "order"], ascending=[False, True]).head(top).copy()
        if selected.empty:
            continue
        selected_rows.append(selected.assign(signal_date=date))
        rows.append(
            {
                "date": date,
                "n": len(selected),
                "ret": selected["ah_ret"].mean(),
                "score": selected["score"].mean(),
                "tickers": ",".join(selected["ticker"].tolist()),
            }
        )

    daily = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    selected = pd.concat(selected_rows).reset_index(drop=True) if selected_rows else pd.DataFrame()
    return daily, selected


def simulate_static_baseline(
    panel: pd.DataFrame,
    baseline_tickers: list[str],
    top: int,
    min_vol: float,
    max_vol: float,
) -> pd.DataFrame:
    order = {ticker: i for i, ticker in enumerate(baseline_tickers)}
    base = panel[panel["ticker"].isin(order)].copy()
    rows = []
    for date, day in base.groupby("date"):
        candidates = day[(day["rth_vol"] >= min_vol) & (day["rth_vol"] <= max_vol)].copy()
        candidates = candidates.dropna(subset=["ah_ret"])
        if candidates.empty:
            continue
        candidates["order"] = candidates["ticker"].map(order)
        selected = candidates.sort_values("order").head(top)
        rows.append({"date": date, "n": len(selected), "ret": selected["ah_ret"].mean()})
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def simulate_universe_baseline(
    panel: pd.DataFrame,
    universe_order: list[str],
    top: int,
    min_vol: float,
    max_vol: float,
    min_adv: float,
    min_price: float,
    ah_lag1_threshold: float,
    rank_mode: str,
) -> pd.DataFrame:
    order = {ticker: i for i, ticker in enumerate(universe_order)}
    rows = []
    for date, day in panel.groupby("date"):
        candidates = day[
            (day["rth_vol"] >= min_vol)
            & (day["rth_vol"] <= max_vol)
            & (day["adv20"] >= min_adv)
            & (day["close"] >= min_price)
            & (day["known_ah"] >= ah_lag1_threshold)
        ].copy()
        candidates = candidates.dropna(subset=["ah_ret"])
        if candidates.empty:
            continue

        candidates["order"] = candidates["ticker"].map(order)
        if rank_mode == "adv":
            candidates = candidates.sort_values(["adv20", "order"], ascending=[False, True])
        elif rank_mode == "rth_vol":
            candidates = candidates.sort_values(["rth_vol", "order"], ascending=[False, True])
        else:
            candidates = candidates.sort_values("order")

        selected = candidates.head(top)
        rows.append(
            {
                "date": date,
                "n": len(selected),
                "ret": selected["ah_ret"].mean(),
                "tickers": ",".join(selected["ticker"].tolist()),
            }
        )
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def max_drawdown(returns: pd.Series) -> float:
    eq = (1 + returns.fillna(0)).cumprod()
    return float((eq / eq.cummax() - 1).min())


def sharpe(returns: pd.Series) -> float:
    std = returns.std()
    if std == 0 or pd.isna(std):
        return np.nan
    return float(returns.mean() / std * np.sqrt(252))


def metrics(daily: pd.DataFrame, label: str) -> dict:
    r = daily["ret"].dropna()
    return {
        "strategy": label,
        "days": int(len(r)),
        "avg_n": float(daily["n"].mean()) if "n" in daily else np.nan,
        "total_pct": float(((1 + r).prod() - 1) * 100),
        "mean_bps": float(r.mean() * 10000),
        "std_bps": float(r.std() * 10000),
        "sharpe": sharpe(r),
        "maxdd_pct": max_drawdown(r) * 100,
        "win_rate_pct": float((r > 0).mean() * 100),
    }


def annual(daily: pd.DataFrame, label: str) -> pd.DataFrame:
    tmp = daily.copy()
    tmp["year"] = pd.to_datetime(tmp["date"]).dt.year
    out = tmp.groupby("year").agg(
        days=("ret", "count"),
        total_pct=("ret", lambda x: ((1 + x).prod() - 1) * 100),
        avg_n=("n", "mean"),
    ).reset_index()
    out["strategy"] = label
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Dynamic AH slope selection study")
    parser.add_argument("--ticker-file", type=Path, default=DEFAULT_TICKERS)
    parser.add_argument("--baseline-ticker-file", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--start", default="2010-01-01")
    parser.add_argument("--window", type=int, default=60)
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--min-vol", type=float, default=0.025)
    parser.add_argument("--max-vol", type=float, default=0.045)
    parser.add_argument("--min-adv", type=float, default=20_000_000)
    parser.add_argument("--min-price", type=float, default=5.0)
    parser.add_argument("--ah-lag1-threshold", type=float, default=-0.10)
    parser.add_argument("--min-slope", type=float, default=0.0)
    parser.add_argument("--min-tstat", type=float, default=1.0)
    parser.add_argument("--min-r2", type=float, default=0.05)
    parser.add_argument("--min-hit", type=float, default=0.50)
    parser.add_argument("--max-ah-dd", type=float, default=-0.20)
    parser.add_argument("--max-ah-std", type=float, default=None)
    parser.add_argument("--score-mode", choices=["slope", "slope_r2", "slope_t", "composite"], default="slope_t")
    parser.add_argument("--universe-baseline-rank", choices=["order", "adv", "rth_vol"], default="adv")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    tickers = load_tickers(args.ticker_file)
    baseline_tickers = load_tickers(args.baseline_ticker_file)

    panel = build_feature_panel(tickers, args.data_dir, args.start, args.window)
    if panel.empty:
        raise SystemExit("No usable data")

    dynamic, selected = simulate(
        panel,
        tickers,
        args.top,
        args.min_vol,
        args.max_vol,
        args.min_adv,
        args.min_price,
        args.ah_lag1_threshold,
        args.min_slope,
        args.min_tstat,
        args.min_r2,
        args.min_hit,
        args.max_ah_dd,
        args.max_ah_std,
        args.score_mode,
    )
    baseline = simulate_static_baseline(panel, baseline_tickers, args.top, args.min_vol, args.max_vol)
    universe_base = simulate_universe_baseline(
        panel,
        tickers,
        args.top,
        args.min_vol,
        args.max_vol,
        args.min_adv,
        args.min_price,
        args.ah_lag1_threshold,
        args.universe_baseline_rank,
    )

    metrics_df = pd.DataFrame([
        metrics(baseline, "static_baseline"),
        metrics(universe_base, f"universe_baseline_{args.universe_baseline_rank}"),
        metrics(dynamic, "dynamic_slope"),
    ])
    annual_df = pd.concat([
        annual(baseline, "static_baseline"),
        annual(universe_base, f"universe_baseline_{args.universe_baseline_rank}"),
        annual(dynamic, "dynamic_slope"),
    ]).sort_values(["year", "strategy"])

    metrics_path = args.out_dir / "symbol_slope_selection_metrics.csv"
    daily_path = args.out_dir / "symbol_slope_selection_daily.csv"
    universe_daily_path = args.out_dir / "symbol_slope_selection_universe_baseline_daily.csv"
    selected_path = args.out_dir / "symbol_slope_selection_selected.csv"
    annual_path = args.out_dir / "symbol_slope_selection_annual.csv"
    panel_path = args.out_dir / "symbol_slope_selection_panel_sample.csv"

    metrics_df.to_csv(metrics_path, index=False)
    dynamic.to_csv(daily_path, index=False)
    universe_base.to_csv(universe_daily_path, index=False)
    selected.to_csv(selected_path, index=False)
    annual_df.to_csv(annual_path, index=False)
    panel.head(5000).to_csv(panel_path, index=False)

    print(f"Universe: {len(tickers)} tickers, panel rows={len(panel)}")
    if not dynamic.empty:
        print(f"Dynamic period: {dynamic['date'].min().date()} - {dynamic['date'].max().date()}")
    print("\nMETRICS")
    print(metrics_df.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("\nANNUAL")
    print(annual_df.pivot(index="year", columns="strategy", values="total_pct").to_string(float_format=lambda x: f"{x:.2f}"))
    print("\nTOP SELECTED TICKERS")
    if not selected.empty:
        print(selected["ticker"].value_counts().head(25).to_string())
    print(f"\nOutputs:\n  {metrics_path}\n  {daily_path}\n  {selected_path}\n  {annual_path}\n  {panel_path}")


if __name__ == "__main__":
    main()
