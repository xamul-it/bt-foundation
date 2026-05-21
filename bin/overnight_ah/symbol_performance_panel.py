#!/usr/bin/env python3
"""
Build per-symbol OvernightAH performance panels.

This is step 1 for monthly universe rotation research: every Nasdaq-100
symbol is evaluated independently with the current OvernightAH filters.
No monthly selection is applied here.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_TICKERS = ROOT / "config-common" / "tickers" / "NASDAQ_100_US.json"
DEFAULT_DATA = ROOT / "config-common" / "data" / "d" / "yahoo"
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "symbol_performance_panel"


def load_tickers(path: Path) -> list[str]:
    with open(path) as f:
        return [str(t).strip() for t in json.load(f) if str(t).strip() and str(t).strip() != "SPY"]


def find_col(df: pd.DataFrame, names: list[str]) -> str | None:
    lookup = {str(c).strip().lower().replace(" ", "_"): c for c in df.columns}
    for name in names:
        key = name.strip().lower().replace(" ", "_")
        if key in lookup:
            return lookup[key]
    return None


def load_symbol(path: Path, ticker: str, start: pd.Timestamp) -> pd.DataFrame:
    raw = pd.read_csv(path)
    date_col = find_col(raw, ["date", "datetime", "timestamp"]) or raw.columns[0]
    raw[date_col] = pd.to_datetime(raw[date_col], utc=True, errors="coerce")
    raw = raw.dropna(subset=[date_col]).sort_values(date_col)
    raw["date"] = raw[date_col].dt.tz_localize(None).dt.normalize()
    raw = raw[raw["date"] >= start].copy()

    rename = {}
    for col in raw.columns:
        key = str(col).strip().lower().replace(" ", "_")
        if key in {"open", "high", "low", "close", "volume"}:
            rename[col] = key
    raw = raw.rename(columns=rename)
    required = {"open", "high", "low", "close", "volume"}
    if not required.issubset(raw.columns):
        return pd.DataFrame()

    for col in required:
        raw[col] = pd.to_numeric(raw[col], errors="coerce")
    raw = raw.dropna(subset=list(required))
    if raw.empty:
        return pd.DataFrame()

    raw["ticker"] = ticker
    raw["rth_vol"] = (raw["high"] - raw["low"]) / raw["open"]
    raw["known_ah"] = (raw["open"] - raw["close"].shift(1)) / raw["close"].shift(1)
    raw["ah_ret"] = (raw["open"].shift(-1) - raw["close"]) / raw["close"]
    raw["dollar_volume"] = raw["close"] * raw["volume"]
    raw["adv20"] = raw["dollar_volume"].rolling(20).mean().shift(1)
    return raw


def max_drawdown(returns: pd.Series) -> float:
    if returns.empty:
        return np.nan
    eq = (1 + returns.fillna(0)).cumprod()
    return float((eq / eq.cummax() - 1).min())


def sharpe(returns: pd.Series) -> float:
    std = returns.std(ddof=1)
    if std == 0 or pd.isna(std):
        return np.nan
    return float(returns.mean() / std * np.sqrt(252))


def sortino(returns: pd.Series) -> float:
    downside = returns[returns < 0]
    downside_std = downside.std(ddof=1)
    if downside_std == 0 or pd.isna(downside_std):
        return np.nan
    return float(returns.mean() / downside_std * np.sqrt(252))


def perf_metrics(returns: pd.Series) -> dict[str, float]:
    returns = returns.dropna()
    return {
        "trades": int(len(returns)),
        "total_pct": float(((1 + returns).prod() - 1) * 100) if len(returns) else np.nan,
        "mean_bps": float(returns.mean() * 10000) if len(returns) else np.nan,
        "std_bps": float(returns.std(ddof=1) * 10000) if len(returns) > 1 else np.nan,
        "sharpe": sharpe(returns),
        "sortino": sortino(returns),
        "maxdd_pct": max_drawdown(returns) * 100 if len(returns) else np.nan,
        "win_rate_pct": float((returns > 0).mean() * 100) if len(returns) else np.nan,
    }


def build_panel(
    tickers: list[str],
    data_dir: Path,
    start: str,
    min_vol: float,
    max_vol: float,
    ah_lag1_threshold: float,
    min_adv: float,
    min_price: float,
) -> pd.DataFrame:
    frames = []
    start_ts = pd.Timestamp(start) - pd.Timedelta(days=60)
    for ticker in tickers:
        path = data_dir / f"{ticker}.csv"
        if not path.exists():
            print(f"[warn] missing {ticker}: {path}")
            continue
        df = load_symbol(path, ticker, start_ts)
        if df.empty:
            print(f"[warn] unusable {ticker}: {path}")
            continue

        df["passes_filters"] = (
            (df["rth_vol"] >= min_vol)
            & (df["rth_vol"] <= max_vol)
            & (df["adv20"] >= min_adv)
            & (df["close"] >= min_price)
            & (df["known_ah"] >= ah_lag1_threshold)
            & df["ah_ret"].notna()
        )
        df["strategy_ret"] = np.where(df["passes_filters"], df["ah_ret"], np.nan)
        frames.append(df[df["date"] >= pd.Timestamp(start)])

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values(["date", "ticker"])


def symbol_summary(trades: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for ticker, group in trades.groupby("ticker"):
        row = {"ticker": ticker}
        row.update(perf_metrics(group["strategy_ret"]))
        row["first_trade"] = group["date"].min()
        row["last_trade"] = group["date"].max()
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["sharpe", "total_pct"], ascending=[False, False])


def monthly_summary(trades: pd.DataFrame) -> pd.DataFrame:
    tmp = trades.copy()
    tmp["month"] = tmp["date"].dt.to_period("M").dt.to_timestamp("M")
    rows = []
    for (month, ticker), group in tmp.groupby(["month", "ticker"]):
        row = {"month": month, "ticker": ticker}
        row.update(perf_metrics(group["strategy_ret"]))
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["month", "sharpe", "total_pct"], ascending=[True, False, False])


def rolling_6m_summary(monthly: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    months = sorted(monthly["month"].dropna().unique())
    rows = []
    for month in months:
        start = pd.Timestamp(month) - pd.DateOffset(months=6) + pd.offsets.MonthBegin(1)
        end = pd.Timestamp(month)
        sample = trades[(trades["date"] >= start) & (trades["date"] <= end)]
        for ticker, group in sample.groupby("ticker"):
            row = {
                "rank_month": pd.Timestamp(month),
                "window_start": start,
                "window_end": end,
                "ticker": ticker,
            }
            row.update(perf_metrics(group["strategy_ret"]))
            rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["rank_sharpe"] = out.groupby("rank_month")["sharpe"].rank(ascending=False, method="first")
    out["rank_total"] = out.groupby("rank_month")["total_pct"].rank(ascending=False, method="first")
    out["rank_sortino"] = out.groupby("rank_month")["sortino"].rank(ascending=False, method="first")
    return out.sort_values(["rank_month", "rank_sharpe", "rank_total"])


def expanding_summary(monthly: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    months = sorted(monthly["month"].dropna().unique())
    rows = []
    for month in months:
        end = pd.Timestamp(month)
        sample = trades[trades["date"] <= end]
        for ticker, group in sample.groupby("ticker"):
            row = {
                "rank_month": pd.Timestamp(month),
                "window_start": group["date"].min(),
                "window_end": end,
                "ticker": ticker,
            }
            row.update(perf_metrics(group["strategy_ret"]))
            rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["rank_sharpe"] = out.groupby("rank_month")["sharpe"].rank(ascending=False, method="first")
    out["rank_total"] = out.groupby("rank_month")["total_pct"].rank(ascending=False, method="first")
    out["rank_sortino"] = out.groupby("rank_month")["sortino"].rank(ascending=False, method="first")
    return out.sort_values(["rank_month", "rank_sharpe", "rank_total"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Per-symbol OvernightAH performance panel")
    parser.add_argument("--ticker-file", type=Path, default=DEFAULT_TICKERS)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--start", default="2010-01-01")
    parser.add_argument("--min-vol", type=float, default=0.025)
    parser.add_argument("--max-vol", type=float, default=0.045)
    parser.add_argument("--ah-lag1-threshold", type=float, default=-0.10)
    parser.add_argument("--min-adv", type=float, default=100_000_000)
    parser.add_argument("--min-price", type=float, default=0.0)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    tickers = load_tickers(args.ticker_file)
    panel = build_panel(
        tickers,
        args.data_dir,
        args.start,
        args.min_vol,
        args.max_vol,
        args.ah_lag1_threshold,
        args.min_adv,
        args.min_price,
    )
    if panel.empty:
        raise SystemExit("No usable data")

    trades = panel[panel["passes_filters"]].copy()
    summary = symbol_summary(trades)
    monthly = monthly_summary(trades)
    rolling = rolling_6m_summary(monthly, trades)
    expanding = expanding_summary(monthly, trades)

    panel_path = args.out_dir / "symbol_daily_panel.csv"
    trades_path = args.out_dir / "symbol_trades.csv"
    summary_path = args.out_dir / "symbol_summary.csv"
    monthly_path = args.out_dir / "symbol_monthly.csv"
    rolling_path = args.out_dir / "symbol_rolling_6m.csv"
    expanding_path = args.out_dir / "symbol_expanding.csv"

    panel.to_csv(panel_path, index=False)
    trades.to_csv(trades_path, index=False)
    summary.to_csv(summary_path, index=False)
    monthly.to_csv(monthly_path, index=False)
    rolling.to_csv(rolling_path, index=False)
    expanding.to_csv(expanding_path, index=False)

    print(f"Universe tickers: {len(tickers)}")
    print(f"Panel rows: {len(panel)}, trade rows: {len(trades)}")
    print("\nTOP SYMBOLS OVER FULL SAMPLE")
    cols = ["ticker", "trades", "total_pct", "mean_bps", "sharpe", "sortino", "maxdd_pct", "win_rate_pct"]
    print(summary[cols].head(25).to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    if not rolling.empty:
        print("\nLATEST 6M RANKING BY SHARPE")
        latest = rolling[rolling["rank_month"] == rolling["rank_month"].max()]
        print(latest[["rank_month", "ticker", "trades", "total_pct", "mean_bps", "sharpe", "sortino", "maxdd_pct", "win_rate_pct"]].head(20).to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("\nOutputs:")
    for path in [panel_path, trades_path, summary_path, monthly_path, rolling_path, expanding_path]:
        print(f"  {path}")


if __name__ == "__main__":
    main()
