#!/usr/bin/env python3
"""
Period validation for SuperTrend event filters versus simple market benchmarks.

Uses the event-level dataset produced by 04_supertrend_meta_filter_study.py and
minute bars from the configured Alpaca SIP cache.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ROOT = SCRIPT_DIR.parents[1]
DEFAULT_META = SCRIPT_DIR / "out/supertrend_meta_filter/supertrend_rth_event_dataset.parquet"
DEFAULT_OUT = SCRIPT_DIR / "out/supertrend_meta_filter/period_benchmark"
DEFAULT_TICKERS = DEFAULT_ROOT / "config-common/tickers/rth_stable_candidates_10.json"
DEFAULT_MINUTE_DIR = DEFAULT_ROOT / "config-common/data/m/alpaca/sip"


FILTERS = {
    "st_no_filter": lambda d: pd.Series(True, index=d.index),
    "rth_logvol_z_20_top50": lambda d: d["rth_logvol_z_20"] >= -0.125998,
    "rth_rvol_rank_top30": lambda d: d["rth_rvol_5_20_rank_pct"] >= 0.70,
    "rth_winrate63_bottom20": lambda d: d["rth_winrate_63"] <= 0.460317,
    "rth_rangeexp_rank_top30__rvol_rank_top30": lambda d: (
        (d["rth_range_exp_5_20_rank_pct"] >= 0.70)
        & (d["rth_rvol_5_20_rank_pct"] >= 0.70)
    ),
    "rth_rvol_rank_top30__downside20_top30": lambda d: (
        (d["rth_rvol_5_20_rank_pct"] >= 0.70)
        & (d["rth_downside_20"] >= 0.010953)
    ),
}


def summarize_pnl(pnl: pd.Series) -> dict:
    pnl = pd.to_numeric(pnl, errors="coerce").dropna()
    if pnl.empty:
        return {
            "n": 0,
            "edge_bps": None,
            "win_rate": None,
            "avg_win_bps": None,
            "avg_loss_bps": None,
            "sum_bps": 0.0,
        }
    winners = pnl[pnl > 0]
    losers = pnl[pnl < 0]
    return {
        "n": int(len(pnl)),
        "edge_bps": round(float(pnl.mean()), 3),
        "win_rate": round(float((pnl > 0).mean()), 4),
        "avg_win_bps": round(float(winners.mean()) if len(winners) else 0.0, 3),
        "avg_loss_bps": round(float(losers.mean()) if len(losers) else 0.0, 3),
        "sum_bps": round(float(pnl.sum()), 3),
    }


def load_events(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df["trading_date"] = pd.to_datetime(df["trading_date"]).dt.tz_localize(None).dt.normalize()
    df["entry_dt"] = pd.to_datetime(df["entry_dt"])
    df["year"] = df["trading_date"].dt.year
    return df


def period_mask(df: pd.DataFrame, start: str, end: str) -> pd.Series:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    return (df["trading_date"] >= start_ts) & (df["trading_date"] <= end_ts)


def event_filter_table(events: pd.DataFrame, periods: dict[str, tuple[str, str]]) -> pd.DataFrame:
    rows = []
    for period, (start, end) in periods.items():
        p = events[period_mask(events, start, end)].copy()
        for name, fn in FILTERS.items():
            try:
                mask = fn(p).fillna(False)
            except KeyError:
                continue
            st = summarize_pnl(p.loc[mask, "pnl_bps"])
            rows.append({"period": period, "filter": name, **st})
    return pd.DataFrame(rows)


def daily_filter_indicators(events: pd.DataFrame, bench_daily: pd.DataFrame, periods: dict[str, tuple[str, str]]) -> pd.DataFrame:
    rows = []
    daily_rows = []
    for period, (start, end) in periods.items():
        p = events[period_mask(events, start, end)].copy()
        b = bench_daily[period_mask(bench_daily, start, end)].copy()
        if b.empty:
            continue
        b = b[["trading_date", "benchmark_sum_bps", "benchmark_mean_bps", "benchmark_symbols"]].drop_duplicates("trading_date")
        for name, fn in FILTERS.items():
            try:
                mask = fn(p).fillna(False)
            except KeyError:
                continue
            selected = p.loc[mask].copy()
            if selected.empty:
                strat_daily = pd.DataFrame(columns=["trading_date", "strategy_sum_bps", "trade_count"])
            else:
                strat_daily = selected.groupby("trading_date").agg(
                    strategy_sum_bps=("pnl_bps", "sum"),
                    trade_count=("pnl_bps", "size"),
                ).reset_index()

            daily = b.merge(strat_daily, on="trading_date", how="left")
            daily["strategy_sum_bps"] = daily["strategy_sum_bps"].fillna(0.0)
            daily["trade_count"] = daily["trade_count"].fillna(0).astype(int)
            daily["alpha_bps"] = daily["strategy_sum_bps"] - daily["benchmark_sum_bps"]
            daily["strategy_edge_day_bps"] = np.where(
                daily["trade_count"] > 0,
                daily["strategy_sum_bps"] / daily["trade_count"],
                np.nan,
            )
            daily["period"] = period
            daily["filter"] = name
            daily_rows.append(daily)

            trade_pnl = selected["pnl_bps"] if not selected.empty else pd.Series(dtype=float)
            trade_stats = summarize_pnl(trade_pnl)
            trading_days = int(len(daily))
            active_days = int((daily["trade_count"] > 0).sum())
            alpha_sum = float(daily["alpha_bps"].sum())
            alpha_mean = float(daily["alpha_bps"].mean())
            alpha_std = float(daily["alpha_bps"].std(ddof=0))
            alpha_pos = float((daily["alpha_bps"] > 0).mean())
            avg_trades_day = float(daily["trade_count"].mean())
            avg_trades_active = float(daily.loc[daily["trade_count"] > 0, "trade_count"].mean()) if active_days else 0.0
            edge_per_trade = trade_stats["edge_bps"]
            edge_series = daily.loc[daily["trade_count"] > 0, "strategy_edge_day_bps"].dropna()
            edge_mean_day = float(edge_series.mean()) if len(edge_series) else np.nan
            edge_std_day = float(edge_series.std(ddof=0)) if len(edge_series) else np.nan
            edge_cv = abs(edge_std_day / edge_mean_day) if np.isfinite(edge_mean_day) and abs(edge_mean_day) > 1e-12 else np.nan
            trades_std_day = float(daily["trade_count"].std(ddof=0))
            trades_cv = trades_std_day / avg_trades_day if avg_trades_day > 1e-12 else np.nan
            intrinsic_risk_index = (
                1.0 / (1.0 + max(0.0, edge_cv) + max(0.0, trades_cv))
                if np.isfinite(edge_cv) and np.isfinite(trades_cv)
                else np.nan
            )
            trade_quality_index = (
                float(edge_per_trade) * float(np.sqrt(avg_trades_day))
                if edge_per_trade is not None and avg_trades_day > 0
                else np.nan
            )
            break_even_cost_bps = alpha_sum / trade_stats["n"] if trade_stats["n"] else np.nan
            trade_goodness_index = (
                float(edge_per_trade) * avg_trades_day
                if edge_per_trade is not None and avg_trades_day > 0
                else np.nan
            )
            intrinsic_risk_penalty = (
                max(0.0, edge_cv) + max(0.0, trades_cv)
                if np.isfinite(edge_cv) and np.isfinite(trades_cv)
                else np.nan
            )

            rows.append({
                "period": period,
                "filter": name,
                "indicator_1_alpha_vs_benchmark_bps": round(alpha_sum, 3),
                "indicator_2_trade_goodness": round(float(trade_goodness_index), 3) if np.isfinite(trade_goodness_index) else np.nan,
                "indicator_3_intrinsic_stability": round(float(intrinsic_risk_index), 4) if np.isfinite(intrinsic_risk_index) else np.nan,
                "indicator_3_intrinsic_risk_penalty": round(float(intrinsic_risk_penalty), 3) if np.isfinite(intrinsic_risk_penalty) else np.nan,
                "trading_days": trading_days,
                "active_days": active_days,
                "active_days_pct": round(active_days / trading_days, 4) if trading_days else np.nan,
                "trade_count": trade_stats["n"],
                "avg_trades_per_day": round(avg_trades_day, 3),
                "avg_trades_per_active_day": round(avg_trades_active, 3),
                "edge_bps_per_trade": trade_stats["edge_bps"],
                "strategy_sum_bps": trade_stats["sum_bps"],
                "benchmark_sum_bps": round(float(daily["benchmark_sum_bps"].sum()), 3),
                "alpha_sum_bps": round(alpha_sum, 3),
                "alpha_mean_day_bps": round(alpha_mean, 3),
                "alpha_day_std_bps": round(alpha_std, 3),
                "alpha_positive_days_pct": round(alpha_pos, 4),
                "break_even_cost_bps_per_trade": round(float(break_even_cost_bps), 3) if np.isfinite(break_even_cost_bps) else np.nan,
                "daily_trade_production_bps": round(float(trade_goodness_index), 3) if np.isfinite(trade_goodness_index) else np.nan,
                "trade_quality_index": round(float(trade_quality_index), 3) if np.isfinite(trade_quality_index) else np.nan,
                "daily_edge_mean_bps": round(edge_mean_day, 3) if np.isfinite(edge_mean_day) else np.nan,
                "daily_edge_std_bps": round(edge_std_day, 3) if np.isfinite(edge_std_day) else np.nan,
                "edge_cv": round(float(edge_cv), 3) if np.isfinite(edge_cv) else np.nan,
                "daily_trades_std": round(trades_std_day, 3),
                "trades_cv": round(float(trades_cv), 3) if np.isfinite(trades_cv) else np.nan,
                "intrinsic_risk_index": round(float(intrinsic_risk_index), 4) if np.isfinite(intrinsic_risk_index) else np.nan,
                "alpha_net_2bps": round(alpha_sum - 2.0 * trade_stats["n"], 3),
                "alpha_net_5bps": round(alpha_sum - 5.0 * trade_stats["n"], 3),
                "alpha_net_10bps": round(alpha_sum - 10.0 * trade_stats["n"], 3),
            })
    return pd.DataFrame(rows), pd.concat(daily_rows, ignore_index=True) if daily_rows else pd.DataFrame()


def read_minute_symbol(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=["timestamp", "open", "close"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("America/New_York")
    df = df[(df["timestamp"].dt.time >= pd.Timestamp("09:30").time()) & (df["timestamp"].dt.time <= pd.Timestamp("16:00").time())]
    df["trading_date"] = df["timestamp"].dt.tz_localize(None).dt.normalize()
    return df.sort_values("timestamp")


def build_daily_benchmark(tickers: list[str], minute_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily_parts = []
    bh_parts = []
    for symbol in tickers:
        path = minute_dir / f"{symbol}.csv"
        if not path.exists():
            continue
        bars = read_minute_symbol(path)
        if bars.empty:
            continue
        daily = bars.groupby("trading_date").agg(
            day_open=("open", "first"),
            day_close=("close", "last"),
        ).reset_index()
        daily["symbol"] = symbol
        daily["bosoc_bps"] = (daily["day_close"] / daily["day_open"] - 1.0) * 10000.0
        daily_parts.append(daily)

    if not daily_parts:
        return pd.DataFrame(), pd.DataFrame()

    all_daily = pd.concat(daily_parts, ignore_index=True)
    bench_daily = all_daily.groupby("trading_date").agg(
        benchmark_sum_bps=("bosoc_bps", "sum"),
        benchmark_mean_bps=("bosoc_bps", "mean"),
        benchmark_symbols=("symbol", "nunique"),
    ).reset_index()
    return all_daily, bench_daily


def benchmark_table(tickers: list[str], minute_dir: Path, periods: dict[str, tuple[str, str]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_daily, bench_daily = build_daily_benchmark(tickers, minute_dir)
    if all_daily.empty:
        return pd.DataFrame(), pd.DataFrame()

    bh_parts = []
    for symbol, daily in all_daily.groupby("symbol"):
        for period, (start, end) in periods.items():
            p = daily[period_mask(daily, start, end)]
            if p.empty:
                continue
            bh_bps = (float(p["day_close"].iloc[-1]) / float(p["day_open"].iloc[0]) - 1.0) * 10000.0
            bh_parts.append({"period": period, "symbol": symbol, "buyhold_bps": bh_bps})

    rows = []
    for period, (start, end) in periods.items():
        p = all_daily[period_mask(all_daily, start, end)]
        st = summarize_pnl(p["bosoc_bps"])
        rows.append({"period": period, "benchmark": "buy_open_sell_close_daily", **st})

    if bh_parts:
        bh = pd.DataFrame(bh_parts)
        for period, p in bh.groupby("period"):
            st = summarize_pnl(p["buyhold_bps"])
            rows.append({"period": period, "benchmark": "buy_and_hold_symbol_period", **st})

    return pd.DataFrame(rows), bench_daily


def default_periods(events: pd.DataFrame) -> dict[str, tuple[str, str]]:
    min_date = events["trading_date"].min()
    max_date = events["trading_date"].max().strftime("%Y-%m-%d")
    periods = {}
    if min_date <= pd.Timestamp("2023-12-31"):
        periods["2023"] = ("2023-01-01", "2023-12-31")
    periods.update({
        "2024": ("2024-01-01", "2024-12-31"),
        "2025": ("2025-01-01", "2025-12-31"),
        "2026_ytd": ("2026-01-01", max_date),
        "2024_2025": ("2024-01-01", "2025-12-31"),
        "all": (min_date.strftime("%Y-%m-%d"), max_date),
    })
    return periods


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=Path, default=DEFAULT_META)
    ap.add_argument("--tickers", type=Path, default=DEFAULT_TICKERS)
    ap.add_argument("--minute-dir", type=Path, default=DEFAULT_MINUTE_DIR)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()

    events = load_events(args.events)
    periods = default_periods(events)
    tickers = json.loads(args.tickers.read_text())

    args.out.mkdir(parents=True, exist_ok=True)
    filters = event_filter_table(events, periods)
    bench, bench_daily = benchmark_table(tickers, args.minute_dir, periods)
    indicators, daily_alpha = daily_filter_indicators(events, bench_daily, periods)

    filters.to_csv(args.out / "supertrend_filter_periods.csv", index=False)
    bench.to_csv(args.out / "benchmarks_periods.csv", index=False)
    indicators.to_csv(args.out / "supertrend_strategy_indicators.csv", index=False)
    daily_alpha.to_csv(args.out / "supertrend_daily_alpha.csv", index=False)

    print("SuperTrend filters")
    print(filters.sort_values(["period", "sum_bps"], ascending=[True, False]).to_string(index=False))
    print("\nBenchmarks")
    print(bench.sort_values(["period", "benchmark"]).to_string(index=False))
    print("\nStrategy indicators")
    show_cols = [
        "period", "filter",
        "indicator_1_alpha_vs_benchmark_bps",
        "indicator_2_trade_goodness",
        "indicator_3_intrinsic_stability",
        "indicator_3_intrinsic_risk_penalty",
        "alpha_mean_day_bps", "alpha_positive_days_pct",
        "trade_count", "edge_bps_per_trade", "avg_trades_per_day",
        "break_even_cost_bps_per_trade", "alpha_net_5bps",
    ]
    print(indicators.sort_values(["period", "indicator_1_alpha_vs_benchmark_bps"], ascending=[True, False])[show_cols].to_string(index=False))
    print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
