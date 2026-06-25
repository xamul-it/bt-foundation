#!/usr/bin/env python3
"""Research harness for AH-only Tech Momentum.

This studies whether the same 21/63/126 momentum idea works when both the
signal and the traded return are restricted to the close-to-next-open leg.
The first pass uses Yahoo daily OHLC data:

    signal at close[t] = rolling sums of AH returns close[t-1] -> open[t]
    traded return      = close[t] -> open[t+1]

So the ranking is ex-ante for the evening entry.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data" / "d" / "yahoo"
NASDAQ_FILE = REPO_ROOT / "config-common" / "tickers" / "NASDAQ_100_US.json"
OUT_ROOT = Path(__file__).resolve().parent
BENCHMARKS = ["QQQ", "SPY"]


@dataclass(frozen=True)
class Variant:
    name: str
    top_n: int
    absolute_momentum: bool = False
    vol_adjusted: bool = False


def read_tickers(path: Path) -> list[str]:
    return list(dict.fromkeys(json.loads(path.read_text())))


def read_yahoo_csv(ticker: str) -> pd.DataFrame | None:
    path = DATA_DIR / f"{ticker}.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path)
    if df.empty:
        return None
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    date_col = "date" if "date" in df.columns else "timestamp"
    df[date_col] = pd.to_datetime(df[date_col], utc=True, errors="coerce").dt.tz_localize(None)
    df = df.dropna(subset=[date_col]).sort_values(date_col).drop_duplicates(date_col, keep="last")
    df = df.set_index(date_col)
    for col in ["open", "high", "low", "close", "adj_close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "open" not in df.columns or "close" not in df.columns or "adj_close" not in df.columns:
        return None
    ratio = df["adj_close"] / df["close"]
    df["adj_open"] = df["open"] * ratio
    return df[["adj_open", "adj_close", "volume"]].dropna(subset=["adj_open", "adj_close"])


def load_panel(tickers: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    frames = {}
    missing = []
    for ticker in tickers:
        df = read_yahoo_csv(ticker)
        if df is None:
            missing.append(ticker)
        else:
            frames[ticker] = df
    if not frames:
        raise RuntimeError("No data loaded")
    adj_open = pd.concat({k: v["adj_open"] for k, v in frames.items()}, axis=1).sort_index()
    adj_close = pd.concat({k: v["adj_close"] for k, v in frames.items()}, axis=1).sort_index()
    volume = pd.concat({k: v["volume"] for k, v in frames.items()}, axis=1).sort_index()
    return adj_open, adj_close, volume, missing


def rebalance_dates(index: pd.DatetimeIndex, frequency: str) -> pd.DatetimeIndex:
    dates = pd.Series(index=index, data=index)
    if frequency == "daily":
        return pd.DatetimeIndex(index)
    if frequency == "weekly":
        keys = pd.MultiIndex.from_arrays([dates.index.isocalendar().year, dates.index.isocalendar().week])
    elif frequency == "monthly":
        keys = pd.MultiIndex.from_arrays([dates.index.year, dates.index.month])
    else:
        raise ValueError(f"Unsupported frequency: {frequency}")
    return pd.DatetimeIndex(dates.groupby(keys).first().values)


def compute_ah_features(adj_open: pd.DataFrame, adj_close: pd.DataFrame) -> dict[str, pd.DataFrame]:
    ah_ret = adj_open / adj_close.shift(1) - 1.0
    trade_ret = adj_open.shift(-1) / adj_close - 1.0
    mom_21 = ah_ret.rolling(21, min_periods=21).sum()
    mom_63 = ah_ret.rolling(63, min_periods=63).sum()
    mom_126 = ah_ret.rolling(126, min_periods=126).sum()
    raw_score = mom_21 + mom_63 + mom_126
    vol_63 = ah_ret.rolling(63, min_periods=63).std() * math.sqrt(252)
    return {
        "ah_ret": ah_ret,
        "trade_ret": trade_ret,
        "raw_score": raw_score,
        "vol_63": vol_63,
    }


def weights_for_date(date: pd.Timestamp, variant: Variant, features: dict[str, pd.DataFrame]) -> tuple[pd.Series, dict]:
    score = features["raw_score"].loc[date].dropna()
    if variant.absolute_momentum:
        score = score[score > 0]
    if variant.vol_adjusted:
        denom = features["vol_63"].loc[date].reindex(score.index).replace(0, np.nan)
        score = (score / denom).dropna()
    ranked = score.sort_values(ascending=False)
    meta = {
        "candidate_count": int(features["raw_score"].loc[date].notna().sum()),
        "eligible_count": int(ranked.shape[0]),
        "top_1": ranked.index[0] if len(ranked) else None,
        "top_1_score": float(ranked.iloc[0]) if len(ranked) else np.nan,
    }
    if ranked.empty:
        return pd.Series(dtype=float), meta
    selected = ranked.index[: variant.top_n]
    return pd.Series(1.0 / len(selected), index=selected, dtype=float), meta


def simulate_variant(
    variant: Variant,
    adj_open: pd.DataFrame,
    adj_close: pd.DataFrame,
    frequency: str,
    start: str,
    end: str,
    cost_bps_side: float,
) -> tuple[pd.Series, pd.DataFrame, pd.DataFrame]:
    features = compute_ah_features(adj_open, adj_close)
    idx = adj_close.loc[start:end].index
    rebal_dates = set(rebalance_dates(idx, frequency))
    current_weights = pd.Series(dtype=float)
    returns = []
    decisions = []
    holdings = []

    for date in idx[:-1]:
        if date in rebal_dates:
            current_weights, meta = weights_for_date(date, variant, features)
            row = {
                "date": date.date().isoformat(),
                "variant": variant.name,
                "selected": ",".join(current_weights.index),
                "weights": ",".join(f"{ticker}:{weight:.4f}" for ticker, weight in current_weights.items()),
                **meta,
            }
            ranked = features["raw_score"].loc[date].dropna().sort_values(ascending=False)
            for rank, ticker in enumerate(ranked.index[:10], start=1):
                row[f"rank_{rank}"] = ticker
            decisions.append(row)

        gross = float(current_weights.sum()) if not current_weights.empty else 0.0
        day_ret = 0.0
        if gross > 0:
            asset_ret = features["trade_ret"].loc[date].reindex(current_weights.index).fillna(0.0)
            day_ret = float((current_weights * asset_ret).sum())
        # Every overnight trade has an entry at the close and an exit at the next open.
        day_ret -= gross * 2.0 * (cost_bps_side / 10_000.0)
        returns.append((date, day_ret))
        holdings.append(
            {
                "date": date.date().isoformat(),
                "variant": variant.name,
                "position_count": int((current_weights > 0).sum()),
                "gross_weight": gross,
                "cash_weight": max(0.0, 1.0 - gross),
            }
        )

    return pd.Series(dict(returns)).sort_index(), pd.DataFrame(decisions), pd.DataFrame(holdings)


def performance_stats(returns: pd.Series, name: str, benchmark: pd.Series | None = None) -> dict:
    returns = returns.dropna()
    if returns.empty:
        return {"name": name}
    equity = (1.0 + returns).cumprod()
    years = max((returns.index[-1] - returns.index[0]).days / 365.25, 1e-9)
    total_return = equity.iloc[-1] - 1.0
    cagr = equity.iloc[-1] ** (1.0 / years) - 1.0
    vol = returns.std() * math.sqrt(252)
    sharpe = returns.mean() / returns.std() * math.sqrt(252) if returns.std() else np.nan
    dd = equity / equity.cummax() - 1.0
    stats = {
        "name": name,
        "start": returns.index[0].date().isoformat(),
        "end": returns.index[-1].date().isoformat(),
        "days": int(returns.shape[0]),
        "total_return": total_return,
        "cagr": cagr,
        "ann_vol": vol,
        "sharpe_0rf": sharpe,
        "max_drawdown": dd.min(),
        "positive_days_pct": (returns > 0).mean(),
        "final_equity": equity.iloc[-1],
    }
    if benchmark is not None:
        aligned = pd.concat([returns, benchmark], axis=1, join="inner").dropna()
        aligned.columns = ["strategy", "benchmark"]
        if not aligned.empty:
            alpha = aligned["strategy"] - aligned["benchmark"]
            stats["alpha_sum_bps"] = alpha.sum() * 10_000
            stats["alpha_mean_day_bps"] = alpha.mean() * 10_000
            stats["alpha_positive_days_pct"] = (alpha > 0).mean()
            stats["alpha_net_2bps"] = stats["alpha_sum_bps"] - 2 * len(aligned)
            stats["alpha_net_5bps"] = stats["alpha_sum_bps"] - 5 * len(aligned)
            stats["alpha_net_10bps"] = stats["alpha_sum_bps"] - 10 * len(aligned)
    return stats


def buy_hold_returns(ticker: str, adj_close: pd.DataFrame, start: str, end: str) -> pd.Series:
    prices = adj_close[ticker].loc[start:end].dropna()
    return prices.pct_change().dropna()


def ah_benchmark_returns(ticker: str, adj_open: pd.DataFrame, adj_close: pd.DataFrame, start: str, end: str, cost_bps_side: float) -> pd.Series:
    idx = adj_close[ticker].loc[start:end].dropna().index
    ret = (adj_open[ticker].shift(-1) / adj_close[ticker] - 1.0).reindex(idx).dropna()
    return ret - 2.0 * (cost_bps_side / 10_000.0)


def equal_weight_ah_returns(tickers: list[str], adj_open: pd.DataFrame, adj_close: pd.DataFrame, start: str, end: str, cost_bps_side: float) -> pd.Series:
    idx = adj_close.loc[start:end].index
    ret = (adj_open.shift(-1) / adj_close - 1.0).reindex(idx)
    out = ret[tickers].mean(axis=1, skipna=True).dropna()
    return out - 2.0 * (cost_bps_side / 10_000.0)


def add_operating_stats(summary: pd.DataFrame, decisions_df: pd.DataFrame, holdings_df: pd.DataFrame) -> pd.DataFrame:
    if decisions_df.empty or holdings_df.empty:
        return summary
    decisions = decisions_df.groupby("variant").size().reset_index(name="rebalance_count")
    holdings = holdings_df.groupby("variant").agg(
        avg_position_count=("position_count", "mean"),
        avg_gross_weight=("gross_weight", "mean"),
        avg_cash_weight=("cash_weight", "mean"),
    ).reset_index()
    ops = decisions.merge(holdings, on="variant", how="outer").rename(columns={"variant": "name"})
    return summary.merge(ops, on="name", how="left")


def period_stats(returns_by_name: dict[str, pd.Series], benchmark: pd.Series, periods: dict[str, tuple[str, str]]) -> pd.DataFrame:
    rows = []
    for period_name, (start, end) in periods.items():
        for name, returns in returns_by_name.items():
            row = performance_stats(returns.loc[start:end], name, benchmark.loc[start:end])
            row["period"] = period_name
            rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2014-01-01")
    parser.add_argument("--end", default="2026-06-18")
    parser.add_argument("--frequency", choices=["daily", "weekly", "monthly"], default="monthly")
    parser.add_argument("--cost-bps-side", type=float, default=1.0)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    out_dir = Path(args.out) if args.out else OUT_ROOT / f"out_{args.frequency}"
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    nasdaq_tickers = read_tickers(NASDAQ_FILE)
    required = sorted(set(nasdaq_tickers + BENCHMARKS))
    all_open, all_close, all_volume, missing = load_panel(required)
    available_nasdaq = [ticker for ticker in nasdaq_tickers if ticker in all_close.columns]
    adj_open = all_open[available_nasdaq].dropna(how="all")
    adj_close = all_close[available_nasdaq].reindex(adj_open.index)

    variants = [
        Variant("ah_nasdaq_top1", top_n=1),
        Variant("ah_nasdaq_top3", top_n=3),
        Variant("ah_nasdaq_top5", top_n=5),
        Variant("ah_nasdaq_top3_abs", top_n=3, absolute_momentum=True),
        Variant("ah_nasdaq_top3_vol_adj", top_n=3, absolute_momentum=True, vol_adjusted=True),
    ]

    returns_by_name: dict[str, pd.Series] = {}
    decisions = []
    holdings = []
    for variant in variants:
        ret, decision_df, holding_df = simulate_variant(
            variant,
            adj_open,
            adj_close,
            args.frequency,
            args.start,
            args.end,
            args.cost_bps_side,
        )
        returns_by_name[variant.name] = ret
        decisions.append(decision_df)
        holdings.append(holding_df)

    qqq_buy_hold = buy_hold_returns("QQQ", all_close, args.start, args.end)
    qqq_ah = ah_benchmark_returns("QQQ", all_open, all_close, args.start, args.end, args.cost_bps_side)
    spy_buy_hold = buy_hold_returns("SPY", all_close, args.start, args.end)
    ew_nasdaq_ah = equal_weight_ah_returns(available_nasdaq, all_open, all_close, args.start, args.end, args.cost_bps_side)
    returns_by_name["benchmark_QQQ_buy_hold"] = qqq_buy_hold
    returns_by_name["benchmark_QQQ_AH"] = qqq_ah
    returns_by_name["benchmark_SPY_buy_hold"] = spy_buy_hold
    returns_by_name["benchmark_equal_weight_nasdaq_AH"] = ew_nasdaq_ah

    decisions_df = pd.concat(decisions, ignore_index=True)
    holdings_df = pd.concat(holdings, ignore_index=True)
    summary = pd.DataFrame(
        performance_stats(ret, name, qqq_buy_hold if name != "benchmark_QQQ_buy_hold" else None)
        for name, ret in returns_by_name.items()
    )
    summary = add_operating_stats(summary, decisions_df, holdings_df).sort_values("cagr", ascending=False)
    equity_df = pd.DataFrame({name: (1.0 + ret).cumprod() for name, ret in returns_by_name.items()})

    periods = {
        "qc_original_window_2014_2020": ("2014-01-01", "2020-09-11"),
        "rate_shock_2021_2022": ("2021-01-01", "2022-12-31"),
        "recent_2023_2026": ("2023-01-01", args.end),
        "full_2014_2026": (args.start, args.end),
    }
    period_df = period_stats(returns_by_name, qqq_buy_hold, periods)

    selection_counts = (
        decisions_df.assign(selected_one=decisions_df["selected"].fillna("").str.split(","))
        .explode("selected_one")
        .query("selected_one != ''")
        .groupby(["variant", "selected_one"])
        .size()
        .reset_index(name="rebalance_count")
        .sort_values(["variant", "rebalance_count"], ascending=[True, False])
    )

    manifest = {
        "study": "AH-only Tech Momentum",
        "data_provider": "yahoo",
        "data_root": str(DATA_DIR.relative_to(REPO_ROOT)),
        "universe_file": str(NASDAQ_FILE.relative_to(REPO_ROOT)),
        "nasdaq_tickers_in_file": len(nasdaq_tickers),
        "nasdaq_tickers_available": len(available_nasdaq),
        "missing_required_tickers": missing,
        "start": args.start,
        "end": args.end,
        "frequency": args.frequency,
        "cost_bps_side": args.cost_bps_side,
        "signal_model": "rolling sums of close[t-1] -> open[t] AH returns over 21/63/126 days",
        "execution_model": "enter at close[t], exit at open[t+1], flat during RTH",
    }

    summary.to_csv(out_dir / "summary.csv", index=False)
    period_df.to_csv(out_dir / "period_metrics.csv", index=False)
    decisions_df.to_csv(out_dir / "rebalance_decisions.csv", index=False)
    holdings_df.to_csv(out_dir / "daily_holdings.csv", index=False)
    selection_counts.to_csv(out_dir / "selection_counts.csv", index=False)
    equity_df.to_csv(out_dir / "equity_curves.csv")
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    print("Wrote:")
    for path in [
        out_dir / "manifest.json",
        out_dir / "summary.csv",
        out_dir / "period_metrics.csv",
        out_dir / "rebalance_decisions.csv",
        out_dir / "selection_counts.csv",
        out_dir / "equity_curves.csv",
    ]:
        print(f"  {path.relative_to(REPO_ROOT)}")
    print()
    cols = [
        "name",
        "total_return",
        "cagr",
        "ann_vol",
        "sharpe_0rf",
        "max_drawdown",
        "alpha_sum_bps",
        "alpha_net_5bps",
    ]
    print(summary[[c for c in cols if c in summary.columns]].to_string(index=False))


if __name__ == "__main__":
    main()
