#!/usr/bin/env python3
"""Research harness for the Tech Momentum strategy draft.

The source strategy is a QuantConnect/Lean sketch. This script reproduces the
idea with local Yahoo daily data, then studies broader Nasdaq-100 variants before
any Backtrader finalization.
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
OUT_DIR = Path(__file__).resolve().parent / "out"

ORIGINAL_TICKERS = ["AMD", "TSLA", "AMZN", "AAPL", "SPXL"]
ORIGINAL_NO_ETF_TICKERS = ["AMD", "TSLA", "AMZN", "AAPL"]
BENCHMARKS = ["QQQ", "SPY"]


@dataclass(frozen=True)
class Variant:
    name: str
    universe_name: str
    top_n: int = 1
    score_mode: str = "raw"
    absolute_momentum: bool = False
    stability_filter: bool = False
    capacity_aware: bool = False
    capital: float = 100_000.0
    liquidity_cap_pct: float = 0.01
    max_names: int = 10


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
    if "adj_close" not in df.columns or "close" not in df.columns or "open" not in df.columns:
        return None
    ratio = df["adj_close"] / df["close"]
    df["adj_open"] = df["open"] * ratio
    df["ticker"] = ticker
    return df[["ticker", "adj_open", "adj_close", "volume"]].dropna(subset=["adj_open", "adj_close"])


def load_panel(tickers: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    frames = {}
    missing = []
    for ticker in tickers:
        df = read_yahoo_csv(ticker)
        if df is None:
            missing.append(ticker)
            continue
        frames[ticker] = df
    if not frames:
        raise RuntimeError("No data loaded")
    adj_close = pd.concat({k: v["adj_close"] for k, v in frames.items()}, axis=1).sort_index()
    adj_open = pd.concat({k: v["adj_open"] for k, v in frames.items()}, axis=1).sort_index()
    volume = pd.concat({k: v["volume"] for k, v in frames.items()}, axis=1).sort_index()
    return adj_open, adj_close, volume, missing


def rebalance_dates(index: pd.DatetimeIndex, frequency: str) -> pd.DatetimeIndex:
    dates = pd.Series(index=index, data=index)
    if frequency == "weekly":
        # First available trading day in each ISO week. Usually Monday, adjusted for holidays.
        keys = pd.MultiIndex.from_arrays([dates.index.isocalendar().year, dates.index.isocalendar().week])
    elif frequency == "monthly":
        keys = pd.MultiIndex.from_arrays([dates.index.year, dates.index.month])
    elif frequency == "quarterly":
        keys = pd.MultiIndex.from_arrays([dates.index.year, dates.index.quarter])
    else:
        raise ValueError(f"Unsupported frequency: {frequency}")
    return pd.DatetimeIndex(dates.groupby(keys).first().values)


def compute_features(adj_open: pd.DataFrame, adj_close: pd.DataFrame, volume: pd.DataFrame) -> dict[str, pd.DataFrame]:
    ret_21 = adj_close / adj_close.shift(21) - 1.0
    ret_63 = adj_close / adj_close.shift(63) - 1.0
    ret_126 = adj_close / adj_close.shift(126) - 1.0
    raw_score = ret_21 + ret_63 + ret_126
    daily_ret = adj_close.pct_change()
    vol_63 = daily_ret.rolling(63).std() * math.sqrt(252)
    dd_63 = adj_close / adj_close.rolling(63).max() - 1.0
    adv20 = (adj_close * volume).rolling(20).mean()
    next_open_ret = adj_open.shift(-1) / adj_open - 1.0
    return {
        "ret_21": ret_21.shift(1),
        "ret_63": ret_63.shift(1),
        "ret_126": ret_126.shift(1),
        "raw_score": raw_score.shift(1),
        "vol_63": vol_63.shift(1),
        "dd_63": dd_63.shift(1),
        "adv20": adv20.shift(1),
        "next_open_ret": next_open_ret,
    }


def weights_for_date(date: pd.Timestamp, variant: Variant, features: dict[str, pd.DataFrame]) -> tuple[pd.Series, dict]:
    score = features["raw_score"].loc[date].dropna()
    ret_63 = features["ret_63"].loc[date].reindex(score.index)
    ret_126 = features["ret_126"].loc[date].reindex(score.index)
    vol_63 = features["vol_63"].loc[date].reindex(score.index)
    dd_63 = features["dd_63"].loc[date].reindex(score.index)
    adv20 = features["adv20"].loc[date].reindex(score.index)

    eligible = score.copy()
    if variant.absolute_momentum:
        eligible = eligible[eligible > 0]
    if variant.stability_filter:
        mask = (
            (ret_63.reindex(eligible.index) > 0)
            & (ret_126.reindex(eligible.index) > 0)
            & (vol_63.reindex(eligible.index) <= 0.80)
            & (dd_63.reindex(eligible.index) >= -0.35)
        )
        eligible = eligible[mask.fillna(False)]
    if variant.score_mode == "vol_adjusted":
        denom = vol_63.reindex(eligible.index).replace(0, np.nan)
        eligible = (eligible / denom).dropna()
    elif variant.score_mode != "raw":
        raise ValueError(f"Unsupported score mode: {variant.score_mode}")

    ranked = eligible.sort_values(ascending=False)
    meta = {
        "candidate_count": int(score.notna().sum()),
        "eligible_count": int(ranked.shape[0]),
        "top_1": ranked.index[0] if len(ranked) else None,
        "top_1_score": float(ranked.iloc[0]) if len(ranked) else np.nan,
    }
    if ranked.empty:
        return pd.Series(dtype=float), meta

    if variant.capacity_aware:
        remaining = variant.capital
        weights: dict[str, float] = {}
        for ticker in ranked.index[: variant.max_names]:
            cap_value = adv20.get(ticker, np.nan) * variant.liquidity_cap_pct
            if not np.isfinite(cap_value) or cap_value <= 0:
                continue
            allocation = min(remaining, cap_value)
            if allocation > 0:
                weights[ticker] = allocation / variant.capital
                remaining -= allocation
            if remaining <= 1e-9:
                break
        meta["cash_weight"] = max(0.0, remaining / variant.capital)
        return pd.Series(weights, dtype=float), meta

    selected = ranked.index[: variant.top_n]
    weights = pd.Series(1.0 / len(selected), index=selected, dtype=float)
    meta["cash_weight"] = 0.0
    return weights, meta


def simulate_variant(
    variant: Variant,
    universe_tickers: list[str],
    all_open: pd.DataFrame,
    all_close: pd.DataFrame,
    all_volume: pd.DataFrame,
    frequency: str,
    start: str,
    end: str,
    cost_bps: float,
) -> tuple[pd.Series, pd.DataFrame, pd.DataFrame]:
    adj_open = all_open[universe_tickers].dropna(how="all")
    adj_close = all_close[universe_tickers].reindex(adj_open.index)
    volume = all_volume[universe_tickers].reindex(adj_open.index)
    features = compute_features(adj_open, adj_close, volume)
    idx = adj_open.loc[start:end].index
    rebal_dates = set(rebalance_dates(idx, frequency))
    daily_asset_ret = features["next_open_ret"].reindex(idx)

    current_weights = pd.Series(dtype=float)
    prev_weights = pd.Series(dtype=float)
    returns = []
    decisions = []
    holdings = []

    for date in idx[:-1]:
        turnover = 0.0
        if date in rebal_dates:
            current_weights, meta = weights_for_date(date, variant, features)
            union = prev_weights.index.union(current_weights.index)
            turnover = float((current_weights.reindex(union, fill_value=0) - prev_weights.reindex(union, fill_value=0)).abs().sum())
            prev_weights = current_weights.copy()
            row = {
                "date": date.date().isoformat(),
                "variant": variant.name,
                "selected": ",".join(current_weights.index),
                "weights": ",".join(f"{t}:{w:.4f}" for t, w in current_weights.items()),
                "turnover": turnover,
                **meta,
            }
            for rank, ticker in enumerate(features["raw_score"].loc[date].dropna().sort_values(ascending=False).index[:10], start=1):
                row[f"rank_{rank}"] = ticker
            decisions.append(row)
        day_ret = 0.0
        if not current_weights.empty:
            asset_ret = daily_asset_ret.loc[date].reindex(current_weights.index).fillna(0.0)
            day_ret = float((current_weights * asset_ret).sum())
        day_ret -= turnover * (cost_bps / 10_000.0)
        returns.append((date, day_ret))
        holdings.append(
            {
                "date": date.date().isoformat(),
                "variant": variant.name,
                "position_count": int((current_weights > 0).sum()),
                "cash_weight": max(0.0, 1.0 - float(current_weights.sum())) if not current_weights.empty else 1.0,
                "gross_weight": float(current_weights.sum()) if not current_weights.empty else 0.0,
            }
        )

    ret = pd.Series(dict(returns)).sort_index()
    return ret, pd.DataFrame(decisions), pd.DataFrame(holdings)


def performance_stats(returns: pd.Series, name: str, benchmark: pd.Series | None = None) -> dict:
    returns = returns.dropna()
    equity = (1.0 + returns).cumprod()
    if returns.empty:
        return {"name": name}
    years = max((returns.index[-1] - returns.index[0]).days / 365.25, 1e-9)
    total_return = equity.iloc[-1] - 1.0
    cagr = equity.iloc[-1] ** (1.0 / years) - 1.0
    vol = returns.std() * math.sqrt(252)
    sharpe = (returns.mean() / returns.std() * math.sqrt(252)) if returns.std() else np.nan
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


def add_operating_stats(summary: pd.DataFrame, decisions_df: pd.DataFrame, holdings_df: pd.DataFrame) -> pd.DataFrame:
    if decisions_df.empty:
        return summary
    turnover = decisions_df.groupby("variant")["turnover"].agg(
        rebalance_count="count",
        avg_rebalance_turnover="mean",
        total_turnover="sum",
    )
    holdings = holdings_df.groupby("variant").agg(
        avg_position_count=("position_count", "mean"),
        avg_cash_weight=("cash_weight", "mean"),
        avg_gross_weight=("gross_weight", "mean"),
    )
    ops = turnover.join(holdings, how="outer").reset_index().rename(columns={"variant": "name"})
    return summary.merge(ops, on="name", how="left")


def buy_hold_returns(ticker: str, all_open: pd.DataFrame, start: str, end: str) -> pd.Series:
    prices = all_open[ticker].loc[start:end].dropna()
    return (prices.shift(-1) / prices - 1.0).dropna()


def equal_weight_returns(tickers: list[str], all_open: pd.DataFrame, start: str, end: str) -> pd.Series:
    prices = all_open[tickers].loc[start:end]
    rets = prices.shift(-1) / prices - 1.0
    return rets.mean(axis=1, skipna=True).dropna()


def period_stats(returns_by_name: dict[str, pd.Series], benchmark: pd.Series, periods: dict[str, tuple[str, str]]) -> pd.DataFrame:
    rows = []
    for period_name, (start, end) in periods.items():
        for name, ret in returns_by_name.items():
            sliced = ret.loc[start:end]
            bench = benchmark.loc[start:end]
            row = performance_stats(sliced, name, bench)
            row["period"] = period_name
            rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2014-01-01")
    parser.add_argument("--end", default="2026-06-18")
    parser.add_argument("--frequency", choices=["weekly", "monthly", "quarterly"], default="weekly")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    nasdaq_tickers = read_tickers(NASDAQ_FILE)
    required = sorted(set(nasdaq_tickers + ORIGINAL_TICKERS + BENCHMARKS))
    all_open, all_close, all_volume, missing = load_panel(required)

    available_nasdaq = [t for t in nasdaq_tickers if t in all_open.columns]
    universes = {
        "original": [t for t in ORIGINAL_TICKERS if t in all_open.columns],
        "original_no_etf": [t for t in ORIGINAL_NO_ETF_TICKERS if t in all_open.columns],
        "nasdaq100_local": available_nasdaq,
    }

    variants = [
        Variant("original_top1", "original", top_n=1),
        Variant("original_no_etf_top1", "original_no_etf", top_n=1),
        Variant("nasdaq_top1", "nasdaq100_local", top_n=1),
        Variant("nasdaq_top3", "nasdaq100_local", top_n=3),
        Variant("nasdaq_top5", "nasdaq100_local", top_n=5),
        Variant("nasdaq_top3_abs", "nasdaq100_local", top_n=3, absolute_momentum=True),
        Variant("nasdaq_top3_stable", "nasdaq100_local", top_n=3, absolute_momentum=True, stability_filter=True),
        Variant("nasdaq_top3_vol_adj", "nasdaq100_local", top_n=3, score_mode="vol_adjusted", absolute_momentum=True),
        Variant("nasdaq_capacity_10m_1pct_adv", "nasdaq100_local", capacity_aware=True, capital=10_000_000),
        Variant("nasdaq_capacity_100m_1pct_adv", "nasdaq100_local", capacity_aware=True, capital=100_000_000),
        Variant("nasdaq_capacity_1b_1pct_adv", "nasdaq100_local", capacity_aware=True, capital=1_000_000_000),
    ]

    returns_by_name: dict[str, pd.Series] = {}
    decisions = []
    holdings = []
    for variant in variants:
        ret, decision_df, holding_df = simulate_variant(
            variant,
            universes[variant.universe_name],
            all_open,
            all_close,
            all_volume,
            args.frequency,
            args.start,
            args.end,
            args.cost_bps,
        )
        returns_by_name[variant.name] = ret
        decisions.append(decision_df)
        holdings.append(holding_df)

    qqq = buy_hold_returns("QQQ", all_open, args.start, args.end)
    spy = buy_hold_returns("SPY", all_open, args.start, args.end)
    ew_nasdaq = equal_weight_returns(available_nasdaq, all_open, args.start, args.end)
    returns_by_name["benchmark_QQQ"] = qqq
    returns_by_name["benchmark_SPY"] = spy
    returns_by_name["benchmark_equal_weight_nasdaq"] = ew_nasdaq

    summary_rows = []
    for name, ret in returns_by_name.items():
        summary_rows.append(performance_stats(ret, name, qqq if name != "benchmark_QQQ" else None))

    periods = {
        "qc_original_window_2014_2020": ("2014-01-01", "2020-09-11"),
        "rate_shock_2021_2022": ("2021-01-01", "2022-12-31"),
        "recent_2023_2026": ("2023-01-01", args.end),
        "full_2014_2026": (args.start, args.end),
    }
    period_df = period_stats(returns_by_name, qqq, periods)

    decisions_df = pd.concat(decisions, ignore_index=True)
    holdings_df = pd.concat(holdings, ignore_index=True)
    summary = pd.DataFrame(summary_rows)
    summary = add_operating_stats(summary, decisions_df, holdings_df).sort_values("cagr", ascending=False)
    equity_df = pd.DataFrame({name: (1 + ret).cumprod() for name, ret in returns_by_name.items()})

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
        "source": "bt-strategy-test/techmomentum/main.py",
        "data_provider": "yahoo",
        "data_root": str(DATA_DIR.relative_to(REPO_ROOT)),
        "universe_file": str(NASDAQ_FILE.relative_to(REPO_ROOT)),
        "nasdaq_tickers_in_file": len(nasdaq_tickers),
        "nasdaq_tickers_available": len(available_nasdaq),
        "missing_required_tickers": missing,
        "start": args.start,
        "end": args.end,
        "frequency": args.frequency,
        "execution_model": "score from previous adjusted close; enter/hold using adjusted open-to-open returns",
        "cost_bps_per_turnover_1x": args.cost_bps,
    }

    summary.to_csv(OUT_DIR / "summary.csv", index=False)
    period_df.to_csv(OUT_DIR / "period_metrics.csv", index=False)
    decisions_df.to_csv(OUT_DIR / "rebalance_decisions.csv", index=False)
    holdings_df.to_csv(OUT_DIR / "daily_holdings.csv", index=False)
    selection_counts.to_csv(OUT_DIR / "selection_counts.csv", index=False)
    equity_df.to_csv(OUT_DIR / "equity_curves.csv")
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    print("Wrote:")
    for path in [
        OUT_DIR / "manifest.json",
        OUT_DIR / "summary.csv",
        OUT_DIR / "period_metrics.csv",
        OUT_DIR / "rebalance_decisions.csv",
        OUT_DIR / "selection_counts.csv",
        OUT_DIR / "equity_curves.csv",
    ]:
        print(f"  {path.relative_to(REPO_ROOT)}")
    print()
    print(summary[["name", "total_return", "cagr", "ann_vol", "sharpe_0rf", "max_drawdown", "alpha_sum_bps", "alpha_net_5bps"]].to_string(index=False))


if __name__ == "__main__":
    main()
