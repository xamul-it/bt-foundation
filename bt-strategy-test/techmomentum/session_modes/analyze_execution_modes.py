#!/usr/bin/env python3
"""Compare execution modes for the same TechMomentum strategy.

The study keeps the TechMomentum base strategy fixed and varies execution mode.
The base strategy is a Nasdaq cross-sectional momentum rotation using the same
21/63/126 rolling-sum score. The execution modes are:

* AH:  close[t] -> open[t+1], selected at close[t]
* RTH: open[t]  -> close[t], selected before open[t]
* C2C: close[t] -> close[t+1], reference full-session rotation

Daily/weekly/monthly rebalance controls when the shortlist is refreshed. For
AH and RTH the portfolio still enters and exits every session; it does not hold
through the other leg.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = REPO_ROOT / "data" / "d" / "yahoo"
NASDAQ_FILE = REPO_ROOT / "config-common" / "tickers" / "NASDAQ_100_US.json"
OUT_ROOT = Path(__file__).resolve().parent
BENCHMARKS = ["QQQ", "SPY"]
BASE_STRATEGY = "techmomentum"


@dataclass(frozen=True)
class Variant:
    session: str
    frequency: str
    top_n: int
    absolute_momentum: bool = False
    vol_adjusted: bool = False

    @property
    def name(self) -> str:
        suffix = ""
        if self.absolute_momentum:
            suffix += "_abs"
        if self.vol_adjusted:
            suffix += "_vol_adj"
        return f"{self.session}_{self.frequency}_top{self.top_n}{suffix}"

    @property
    def construction(self) -> str:
        suffix = ""
        if self.absolute_momentum:
            suffix += "_abs"
        if self.vol_adjusted:
            suffix += "_vol_adj"
        return f"top{self.top_n}{suffix}"

    def metadata(self) -> dict:
        return {
            "base_strategy": BASE_STRATEGY,
            "variant_type": "execution_mode",
            "execution_mode": self.session,
            "rebalance": self.frequency,
            "construction": self.construction,
            "top_n": self.top_n,
            "absolute_momentum": bool(self.absolute_momentum),
            "vol_adjusted": bool(self.vol_adjusted),
        }


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
    required = {"open", "close", "adj_close", "volume"}
    if not required.issubset(df.columns):
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
            continue
        frames[ticker] = df
    if not frames:
        raise RuntimeError("No data loaded")
    adj_open = pd.concat({ticker: df["adj_open"] for ticker, df in frames.items()}, axis=1).sort_index()
    adj_close = pd.concat({ticker: df["adj_close"] for ticker, df in frames.items()}, axis=1).sort_index()
    volume = pd.concat({ticker: df["volume"] for ticker, df in frames.items()}, axis=1).sort_index()
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


def leg_returns(adj_open: pd.DataFrame, adj_close: pd.DataFrame, session: str) -> pd.DataFrame:
    if session == "ah":
        return adj_open.shift(-1) / adj_close - 1.0
    if session == "rth":
        return adj_close / adj_open - 1.0
    if session == "c2c":
        return adj_close.shift(-1) / adj_close - 1.0
    raise ValueError(f"Unsupported session: {session}")


def signal_returns(adj_open: pd.DataFrame, adj_close: pd.DataFrame, session: str) -> pd.DataFrame:
    if session == "ah":
        # Known at close[t]: the night that ended at open[t].
        return adj_open / adj_close.shift(1) - 1.0
    if session == "rth":
        # Known before open[t]: RTH returns through t-1.
        return (adj_close / adj_open - 1.0).shift(1)
    if session == "c2c":
        # Known at close[t]: close-to-close through t.
        return adj_close.pct_change()
    raise ValueError(f"Unsupported session: {session}")


def compute_features(adj_open: pd.DataFrame, adj_close: pd.DataFrame, session: str) -> dict[str, pd.DataFrame]:
    sig = signal_returns(adj_open, adj_close, session)
    mom_21 = sig.rolling(21, min_periods=21).sum()
    mom_63 = sig.rolling(63, min_periods=63).sum()
    mom_126 = sig.rolling(126, min_periods=126).sum()
    raw_score = mom_21 + mom_63 + mom_126
    vol_63 = sig.rolling(63, min_periods=63).std() * math.sqrt(252)
    return {
        "signal_ret": sig,
        "trade_ret": leg_returns(adj_open, adj_close, session),
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
    start: str,
    end: str,
    cost_bps_side: float,
) -> tuple[pd.Series, pd.DataFrame, pd.DataFrame]:
    features = compute_features(adj_open, adj_close, variant.session)
    idx = adj_close.loc[start:end].index
    rebal_dates = set(rebalance_dates(idx, variant.frequency))
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
                **variant.metadata(),
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
        # AH and RTH always open and close each session. C2C approximates a
        # daily rotation with two execution sides as well.
        day_ret -= gross * 2.0 * (cost_bps_side / 10_000.0)
        returns.append((date, day_ret))
        holdings.append(
            {
                "date": date.date().isoformat(),
                "variant": variant.name,
                **variant.metadata(),
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


def benchmark_metadata(name: str) -> dict:
    execution_mode = "buy_hold"
    if name.endswith("_AH"):
        execution_mode = "ah"
    elif name.endswith("_RTH"):
        execution_mode = "rth"
    return {
        "base_strategy": "benchmark",
        "variant_type": "benchmark",
        "execution_mode": execution_mode,
        "rebalance": "none",
        "construction": name.replace("benchmark_", ""),
        "top_n": np.nan,
        "absolute_momentum": False,
        "vol_adjusted": False,
    }


def benchmark_returns(
    name: str,
    adj_open: pd.DataFrame,
    adj_close: pd.DataFrame,
    tickers: list[str],
    start: str,
    end: str,
    cost_bps_side: float,
) -> pd.Series:
    if name == "QQQ_buy_hold":
        return adj_close["QQQ"].loc[start:end].pct_change().dropna()
    if name == "SPY_buy_hold":
        return adj_close["SPY"].loc[start:end].pct_change().dropna()
    if name == "QQQ_AH":
        ret = leg_returns(adj_open[["QQQ"]], adj_close[["QQQ"]], "ah")["QQQ"].loc[start:end].dropna()
        return ret - 2.0 * (cost_bps_side / 10_000.0)
    if name == "QQQ_RTH":
        ret = leg_returns(adj_open[["QQQ"]], adj_close[["QQQ"]], "rth")["QQQ"].loc[start:end].dropna()
        return ret - 2.0 * (cost_bps_side / 10_000.0)
    if name == "equal_weight_AH":
        ret = leg_returns(adj_open[tickers], adj_close[tickers], "ah").loc[start:end].mean(axis=1, skipna=True).dropna()
        return ret - 2.0 * (cost_bps_side / 10_000.0)
    if name == "equal_weight_RTH":
        ret = leg_returns(adj_open[tickers], adj_close[tickers], "rth").loc[start:end].mean(axis=1, skipna=True).dropna()
        return ret - 2.0 * (cost_bps_side / 10_000.0)
    raise ValueError(f"Unsupported benchmark: {name}")


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
    parser.add_argument("--cost-bps-side", type=float, default=1.0)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    out_dir = Path(args.out) if args.out else OUT_ROOT / f"out_cost{args.cost_bps_side:g}"
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    nasdaq_tickers = read_tickers(NASDAQ_FILE)
    required = sorted(set(nasdaq_tickers + BENCHMARKS))
    all_open, all_close, all_volume, missing = load_panel(required)
    available_nasdaq = [ticker for ticker in nasdaq_tickers if ticker in all_close.columns]
    trade_open = all_open[available_nasdaq].dropna(how="all")
    trade_close = all_close[available_nasdaq].reindex(trade_open.index)

    variants = []
    for session in ["ah", "rth", "c2c"]:
        for frequency in ["daily", "weekly", "monthly"]:
            for top_n in [1, 3, 5]:
                variants.append(Variant(session=session, frequency=frequency, top_n=top_n))
            variants.append(Variant(session=session, frequency=frequency, top_n=3, absolute_momentum=True))
            variants.append(Variant(session=session, frequency=frequency, top_n=3, absolute_momentum=True, vol_adjusted=True))

    returns_by_name: dict[str, pd.Series] = {}
    metadata_by_name: dict[str, dict] = {}
    decisions = []
    holdings = []
    for variant in variants:
        ret, decision_df, holding_df = simulate_variant(
            variant,
            trade_open,
            trade_close,
            args.start,
            args.end,
            args.cost_bps_side,
        )
        returns_by_name[variant.name] = ret
        metadata_by_name[variant.name] = variant.metadata()
        decisions.append(decision_df)
        holdings.append(holding_df)

    benchmark_names = [
        "QQQ_buy_hold",
        "SPY_buy_hold",
        "QQQ_AH",
        "QQQ_RTH",
        "equal_weight_AH",
        "equal_weight_RTH",
    ]
    for name in benchmark_names:
        benchmark_key = f"benchmark_{name}"
        returns_by_name[benchmark_key] = benchmark_returns(
            name,
            all_open,
            all_close,
            available_nasdaq,
            args.start,
            args.end,
            args.cost_bps_side,
        )
        metadata_by_name[benchmark_key] = benchmark_metadata(benchmark_key)

    qqq_buy_hold = returns_by_name["benchmark_QQQ_buy_hold"]
    decisions_df = pd.concat(decisions, ignore_index=True)
    holdings_df = pd.concat(holdings, ignore_index=True)
    summary_rows = []
    for name, ret in returns_by_name.items():
        row = performance_stats(ret, name, qqq_buy_hold if name != "benchmark_QQQ_buy_hold" else None)
        row.update(metadata_by_name.get(name, {}))
        summary_rows.append(row)
    summary = pd.DataFrame(summary_rows)
    summary = add_operating_stats(summary, decisions_df, holdings_df)
    summary = summary.sort_values("cagr", ascending=False)

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
        .groupby(["variant", "base_strategy", "execution_mode", "rebalance", "construction", "selected_one"])
        .size()
        .reset_index(name="rebalance_count")
        .sort_values(["variant", "rebalance_count"], ascending=[True, False])
    )

    comparison_rows = []
    for execution_mode in ["ah", "rth", "c2c"]:
        subset = summary[
            (summary["base_strategy"] == BASE_STRATEGY)
            & (summary["execution_mode"] == execution_mode)
        ].copy()
        if subset.empty:
            continue
        best_return = subset.sort_values("cagr", ascending=False).iloc[0]
        best_sharpe = subset.sort_values("sharpe_0rf", ascending=False).iloc[0]
        low_dd = subset.sort_values("max_drawdown", ascending=False).iloc[0]
        for label, row in [("best_cagr", best_return), ("best_sharpe", best_sharpe), ("lowest_drawdown", low_dd)]:
            out = row.to_dict()
            out["execution_mode"] = execution_mode
            out["selection_rule"] = label
            comparison_rows.append(out)
    comparison = pd.DataFrame(comparison_rows)

    manifest = {
        "study": "TechMomentum execution-mode study",
        "base_strategy": BASE_STRATEGY,
        "variant_dimensions": [
            "execution_mode",
            "rebalance",
            "construction",
            "cost_bps_side",
        ],
        "data_provider": "yahoo",
        "data_root": str(DATA_DIR.relative_to(REPO_ROOT)),
        "universe_file": str(NASDAQ_FILE.relative_to(REPO_ROOT)),
        "nasdaq_tickers_in_file": len(nasdaq_tickers),
        "nasdaq_tickers_available": len(available_nasdaq),
        "missing_required_tickers": missing,
        "start": args.start,
        "end": args.end,
        "cost_bps_side": args.cost_bps_side,
        "execution_modes": {
            "ah": "enter close[t], exit open[t+1]; signal uses close[t-1] -> open[t]",
            "rth": "enter open[t], exit close[t]; signal uses open -> close through t-1",
            "c2c": "reference close-to-close rotation; signal uses close-to-close through t",
        },
        "frequencies": ["daily", "weekly", "monthly"],
        "top_n": [1, 3, 5],
    }

    summary.to_csv(out_dir / "summary.csv", index=False)
    period_df.to_csv(out_dir / "period_metrics.csv", index=False)
    decisions_df.to_csv(out_dir / "rebalance_decisions.csv", index=False)
    holdings_df.to_csv(out_dir / "daily_holdings.csv", index=False)
    selection_counts.to_csv(out_dir / "selection_counts.csv", index=False)
    comparison.to_csv(out_dir / "comparison_best.csv", index=False)
    equity_df.to_csv(out_dir / "equity_curves.csv")
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    print("Wrote:")
    for path in [
        out_dir / "manifest.json",
        out_dir / "summary.csv",
        out_dir / "period_metrics.csv",
        out_dir / "comparison_best.csv",
        out_dir / "rebalance_decisions.csv",
        out_dir / "selection_counts.csv",
        out_dir / "equity_curves.csv",
    ]:
        print(f"  {path.relative_to(REPO_ROOT)}")
    print()
    cols = ["name", "total_return", "cagr", "ann_vol", "sharpe_0rf", "max_drawdown", "alpha_sum_bps", "alpha_net_5bps"]
    print(summary[[c for c in cols if c in summary.columns]].head(25).to_string(index=False))


if __name__ == "__main__":
    main()
