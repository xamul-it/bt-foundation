#!/usr/bin/env python3
"""Ex-ante asset downselect study for OvernightAH.

Builds a symbol-month dataset where the target is the next month OvernightAH
single-symbol performance and the features are available before that month.
Then it measures cross-sectional rank IC and simulates monthly top-N rotations.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TICKERS = ROOT / "config-common" / "tickers" / "NASDAQ_100_US.json"
DEFAULT_STATIC = ROOT / "config-common" / "tickers" / "stable_ah_top10.json"
DEFAULT_DATA = ROOT / "config-common" / "data" / "d" / "yahoo_adj"
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "edge_prediction_study"


@dataclass(frozen=True)
class StrategyParams:
    min_intraday_vol: float
    max_intraday_vol: float
    ah_lag1_threshold: float
    min_adv: float
    min_price: float
    max_concurrent: int
    max_exposure: float


def load_tickers(path: Path) -> list[str]:
    return [str(x).strip() for x in json.loads(path.read_text()) if str(x).strip()]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename = {}
    for col in df.columns:
        key = str(col).strip().lower().replace("_", " ")
        if key == "date":
            rename[col] = "date"
        elif key in {"open", "high", "low", "close", "volume"}:
            rename[col] = key
        elif key == "raw dollar volume":
            rename[col] = "raw_dollar_volume"
    return df.rename(columns=rename)


def load_symbol(path: Path, ticker: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = normalize_columns(df)
    required = {"date", "open", "high", "low", "close", "volume"}
    if not required.issubset(df.columns):
        raise ValueError(f"{ticker}: missing {sorted(required.difference(df.columns))}")
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce").dt.tz_localize(None).dt.normalize()
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["date", "open", "high", "low", "close", "volume"])
    df = df.sort_values("date").drop_duplicates("date", keep="last")
    df["ticker"] = ticker
    return df


def add_daily_fields(df: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
    out = df.copy()
    out["prev_close"] = out.groupby("ticker")["close"].shift(1)
    out["next_open"] = out.groupby("ticker")["open"].shift(-1)
    out["c2c_ret"] = out["close"] / out["prev_close"] - 1.0
    out["rth_ret"] = out["close"] / out["open"] - 1.0
    out["known_ah_ret"] = out["open"] / out["prev_close"] - 1.0
    out["target_ah_ret"] = out["next_open"] / out["close"] - 1.0
    out["intraday_vol"] = (out["high"] - out["low"]) / out["open"]
    out["down_day"] = out["close"] < out["open"]
    avg_vol = out.groupby("ticker")["volume"].transform(lambda s: s.rolling(20).mean().shift(1))
    out["adv_dollar"] = avg_vol * out["close"]
    out["passes_filters"] = (
        (out["intraday_vol"] >= params.min_intraday_vol)
        & (out["intraday_vol"] <= params.max_intraday_vol)
        & (out["known_ah_ret"] >= params.ah_lag1_threshold)
        & (out["adv_dollar"] >= params.min_adv)
        & (out["close"] >= params.min_price)
        & out["target_ah_ret"].notna()
    )
    out["strategy_ret"] = np.where(out["passes_filters"], out["target_ah_ret"], np.nan)
    out["month"] = out["date"].dt.to_period("M").dt.to_timestamp()
    return out


def perf_from_returns(ret: pd.Series) -> dict[str, float]:
    ret = pd.to_numeric(ret, errors="coerce").dropna()
    n = int(len(ret))
    if n == 0:
        return {
            "trades": 0,
            "edge_mean_bps": np.nan,
            "win_ratio": np.nan,
            "total_bps": 0.0,
            "std_bps": np.nan,
            "p05_bps": np.nan,
            "loss_mean_bps": np.nan,
        }
    losses = ret[ret < 0]
    return {
        "trades": n,
        "edge_mean_bps": float(ret.mean() * 10000),
        "win_ratio": float((ret > 0).mean()),
        "total_bps": float(ret.sum() * 10000),
        "std_bps": float(ret.std(ddof=1) * 10000) if n > 1 else np.nan,
        "p05_bps": float(ret.quantile(0.05) * 10000) if n > 1 else np.nan,
        "loss_mean_bps": float(losses.mean() * 10000) if len(losses) else np.nan,
    }


def build_monthly_targets(daily: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (month, ticker), group in daily.groupby(["month", "ticker"], sort=True):
        row = {"month": month, "ticker": ticker}
        row.update({f"target_{k}": v for k, v in perf_from_returns(group["strategy_ret"]).items()})
        rows.append(row)
    return pd.DataFrame(rows)


def linear_slope(values: pd.Series) -> float:
    arr = pd.to_numeric(values, errors="coerce").dropna().to_numpy()
    if len(arr) < 5 or np.any(arr <= 0):
        return np.nan
    y = np.log(arr)
    x = np.arange(len(y), dtype=float)
    slope = np.polyfit(x, y, 1)[0]
    return float(slope * 25200)  # annualized log-slope in percent-ish units


def feature_block(hist: pd.DataFrame, lookback: int) -> dict[str, float]:
    ret = hist["strategy_ret"]
    perf = perf_from_returns(ret)
    out = {
        f"strat_edge_{lookback}m": perf["edge_mean_bps"],
        f"strat_win_{lookback}m": perf["win_ratio"],
        f"strat_trades_{lookback}m": perf["trades"],
        f"strat_total_{lookback}m": perf["total_bps"],
        f"strat_p05_{lookback}m": perf["p05_bps"],
        f"pass_rate_{lookback}m": float(hist["passes_filters"].mean()) if len(hist) else np.nan,
        f"c2c_mean_{lookback}m": float(hist["c2c_ret"].mean() * 10000),
        f"c2c_total_{lookback}m": float(hist["c2c_ret"].sum() * 10000),
        f"c2c_vol_{lookback}m": float(hist["c2c_ret"].std(ddof=1) * 10000),
        f"ah_mean_{lookback}m": float(hist["known_ah_ret"].mean() * 10000),
        f"ah_total_{lookback}m": float(hist["known_ah_ret"].sum() * 10000),
        f"rth_mean_{lookback}m": float(hist["rth_ret"].mean() * 10000),
        f"rth_total_{lookback}m": float(hist["rth_ret"].sum() * 10000),
        f"intraday_vol_mean_{lookback}m": float(hist["intraday_vol"].mean()),
        f"down_day_pct_{lookback}m": float(hist["down_day"].mean()),
        f"adv_mean_{lookback}m": float(hist["adv_dollar"].mean()),
        f"close_slope_{lookback}m": linear_slope(hist["close"]),
    }
    closes = hist["close"].dropna()
    if len(closes) >= 2:
        out[f"price_mom_{lookback}m"] = float(closes.iloc[-1] / closes.iloc[0] - 1.0)
    else:
        out[f"price_mom_{lookback}m"] = np.nan
    if len(closes) >= 20:
        out[f"ma20_ratio_{lookback}m"] = float(closes.iloc[-1] / closes.tail(20).mean() - 1.0)
    else:
        out[f"ma20_ratio_{lookback}m"] = np.nan
    if len(closes) >= 50:
        out[f"ma50_ratio_{lookback}m"] = float(closes.iloc[-1] / closes.tail(50).mean() - 1.0)
    else:
        out[f"ma50_ratio_{lookback}m"] = np.nan
    return out


def build_feature_panel(daily: pd.DataFrame, targets: pd.DataFrame, lookbacks: list[int]) -> pd.DataFrame:
    months = sorted(targets["month"].dropna().unique())
    rows = []
    grouped = {ticker: g.sort_values("date") for ticker, g in daily.groupby("ticker")}
    for month in months:
        month_start = pd.Timestamp(month)
        for ticker, sym_daily in grouped.items():
            row = {"month": month_start, "ticker": ticker}
            for lb in lookbacks:
                start = month_start - pd.DateOffset(months=lb)
                hist = sym_daily[(sym_daily["date"] >= start) & (sym_daily["date"] < month_start)]
                if hist.empty:
                    for key in feature_block(sym_daily.iloc[:0], lb):
                        row[key] = np.nan
                else:
                    row.update(feature_block(hist, lb))
            rows.append(row)
    features = pd.DataFrame(rows)
    return features.merge(targets, on=["month", "ticker"], how="left")


def spearman_by_month(panel: pd.DataFrame, feature: str, target: str, min_target_trades: int) -> pd.DataFrame:
    rows = []
    for month, group in panel.groupby("month"):
        g = group[(group["target_trades"] >= min_target_trades)][[feature, target]].dropna()
        if len(g) < 8 or g[feature].nunique() < 3 or g[target].nunique() < 3:
            continue
        rows.append({"month": month, "feature": feature, "target": target, "ic": g[feature].corr(g[target], method="spearman"), "n": len(g)})
    return pd.DataFrame(rows)


def ic_summary(panel: pd.DataFrame, features: list[str], target: str, min_target_trades: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_ic = []
    for feature in features:
        ic = spearman_by_month(panel, feature, target, min_target_trades)
        if not ic.empty:
            all_ic.append(ic)
    ic_df = pd.concat(all_ic, ignore_index=True) if all_ic else pd.DataFrame()
    if ic_df.empty:
        return ic_df, pd.DataFrame()
    rows = []
    for feature, g in ic_df.groupby("feature"):
        vals = g["ic"].dropna()
        rows.append(
            {
                "feature": feature,
                "target": target,
                "months": int(len(vals)),
                "mean_ic": float(vals.mean()),
                "median_ic": float(vals.median()),
                "positive_ic_pct": float((vals > 0).mean()),
                "ic_tstat": float(vals.mean() / (vals.std(ddof=1) / math.sqrt(len(vals)))) if len(vals) > 1 and vals.std(ddof=1) else np.nan,
                "mean_n": float(g["n"].mean()),
            }
        )
    summary = pd.DataFrame(rows).sort_values(["mean_ic", "positive_ic_pct"], ascending=[False, False])
    return ic_df, summary


def decile_spread(panel: pd.DataFrame, feature: str, target: str, min_target_trades: int) -> dict[str, float]:
    spreads = []
    top_vals = []
    bottom_vals = []
    for _, group in panel.groupby("month"):
        g = group[group["target_trades"] >= min_target_trades][[feature, target]].dropna()
        if len(g) < 20 or g[feature].nunique() < 10:
            continue
        g = g.sort_values(feature)
        n = max(2, len(g) // 10)
        bottom = g.head(n)[target].mean()
        top = g.tail(n)[target].mean()
        spreads.append(top - bottom)
        top_vals.append(top)
        bottom_vals.append(bottom)
    if not spreads:
        return {"decile_months": 0, "top_minus_bottom": np.nan, "top_avg": np.nan, "bottom_avg": np.nan}
    return {
        "decile_months": len(spreads),
        "top_minus_bottom": float(np.mean(spreads)),
        "top_avg": float(np.mean(top_vals)),
        "bottom_avg": float(np.mean(bottom_vals)),
    }


def portfolio_metrics(daily_returns: pd.Series) -> dict[str, float]:
    r = daily_returns.fillna(0.0)
    equity = (1.0 + r).cumprod()
    years = max((r.index.max() - r.index.min()).days / 365.25, 1e-9) if len(r) else np.nan
    cagr = float(equity.iloc[-1] ** (1 / years) - 1) if len(r) else np.nan
    dd = equity / equity.cummax() - 1.0 if len(r) else pd.Series(dtype=float)
    sharpe = float(r.mean() / r.std(ddof=1) * np.sqrt(252)) if len(r) > 1 and r.std(ddof=1) else np.nan
    return {
        "days": int(len(r)),
        "total_return_pct": float((equity.iloc[-1] - 1) * 100) if len(r) else np.nan,
        "cagr_pct": cagr * 100 if pd.notna(cagr) else np.nan,
        "maxdd_pct": float(dd.min() * 100) if len(dd) else np.nan,
        "sharpe": sharpe,
        "final_equity": float(equity.iloc[-1]) if len(equity) else np.nan,
    }


def simulate_rotation(
    daily: pd.DataFrame,
    panel: pd.DataFrame,
    feature: str,
    top_n: int,
    min_lookback_trades: int,
    params: StrategyParams,
) -> pd.Series:
    score_by_month: dict[pd.Timestamp, dict[str, float]] = {}
    trade_col = feature.replace("edge_", "trades_").replace("win_", "trades_").replace("total_", "trades_")
    for month, group in panel.groupby("month"):
        g = group.dropna(subset=[feature]).copy()
        if trade_col in g.columns:
            g = g[g[trade_col] >= min_lookback_trades]
        g = g.sort_values([feature, "ticker"], ascending=[False, True]).head(top_n)
        score_by_month[pd.Timestamp(month)] = dict(zip(g["ticker"], g[feature]))

    rows = []
    for date, day in daily[daily["passes_filters"]].groupby("date"):
        month = pd.Timestamp(date).to_period("M").to_timestamp()
        scores = score_by_month.get(month, {})
        if not scores:
            rows.append((date, 0.0, 0))
            continue
        candidates = day[day["ticker"].isin(scores)].copy()
        if candidates.empty:
            rows.append((date, 0.0, 0))
            continue
        candidates["score"] = candidates["ticker"].map(scores)
        candidates = candidates.sort_values(["score", "ticker"], ascending=[False, True]).head(params.max_concurrent)
        n = len(candidates)
        day_ret = params.max_exposure * float(candidates["strategy_ret"].mean()) if n else 0.0
        rows.append((date, day_ret, n))
    out = pd.DataFrame(rows, columns=["date", "ret", "positions"]).set_index("date").sort_index()
    return out["ret"]


def ranked_monthly_symbols(
    panel: pd.DataFrame,
    feature: str,
    top_n: int,
    min_lookback_trades: int,
) -> pd.DataFrame:
    trade_col = feature.replace("edge_", "trades_").replace("win_", "trades_").replace("total_", "trades_")
    rows = []
    detail_rows = []
    for month, group in panel.groupby("month"):
        g = group.dropna(subset=[feature]).copy()
        if trade_col in g.columns:
            g = g[g[trade_col] >= min_lookback_trades]
        g = g.sort_values([feature, "ticker"], ascending=[False, True]).head(top_n)
        symbols = g["ticker"].tolist()
        rows.append({"year": pd.Timestamp(month).year, "month": pd.Timestamp(month).month, "symbols": ",".join(symbols)})
        for rank, row in enumerate(g.itertuples(index=False), start=1):
            detail_rows.append(
                {
                    "month": pd.Timestamp(month),
                    "rank": rank,
                    "ticker": row.ticker,
                    "feature": feature,
                    "score": getattr(row, feature),
                }
            )
    universe = pd.DataFrame(rows)
    detail = pd.DataFrame(detail_rows)
    return universe, detail


def safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in value)


def simulate_static(daily: pd.DataFrame, tickers: list[str], params: StrategyParams) -> pd.Series:
    order = {ticker: idx for idx, ticker in enumerate([t for t in tickers if t != "SPY"])}
    rows = []
    for date, day in daily[daily["passes_filters"] & daily["ticker"].isin(order)].groupby("date"):
        candidates = day.copy()
        candidates["order"] = candidates["ticker"].map(order)
        candidates = candidates.sort_values(["order"]).head(params.max_concurrent)
        n = len(candidates)
        day_ret = params.max_exposure * float(candidates["strategy_ret"].mean()) if n else 0.0
        rows.append((date, day_ret, n))
    out = pd.DataFrame(rows, columns=["date", "ret", "positions"]).set_index("date").sort_index()
    return out["ret"]


def segment_metrics(name: str, returns: pd.Series, start: str | None, end: str | None) -> dict[str, float | str]:
    r = returns.copy()
    if start:
        r = r[r.index >= pd.Timestamp(start)]
    if end:
        r = r[r.index <= pd.Timestamp(end)]
    row = {"segment": name}
    row.update(portfolio_metrics(r))
    return row


def run(args: argparse.Namespace) -> None:
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    params = StrategyParams(
        min_intraday_vol=args.min_intraday_vol,
        max_intraday_vol=args.max_intraday_vol,
        ah_lag1_threshold=args.ah_lag1_threshold,
        min_adv=args.min_adv,
        min_price=args.min_price,
        max_concurrent=args.max_concurrent,
        max_exposure=args.max_exposure,
    )

    selection_tickers = load_tickers(args.ticker_file)
    static_tickers = load_tickers(args.static_ticker_file)
    tickers = list(dict.fromkeys(selection_tickers + static_tickers))
    frames = []
    missing = []
    for ticker in tickers:
        path = args.data_dir / f"{ticker}.csv"
        if not path.exists():
            missing.append(ticker)
            continue
        frames.append(load_symbol(path, ticker))
    if not frames:
        raise SystemExit("no input data")
    daily = add_daily_fields(pd.concat(frames, ignore_index=True), params)
    daily = daily[daily["date"] >= pd.Timestamp(args.start)].copy()
    if args.end:
        daily = daily[daily["date"] <= pd.Timestamp(args.end)].copy()

    selection_daily = daily[daily["ticker"].isin(selection_tickers)].copy()
    targets = build_monthly_targets(selection_daily)
    panel = build_feature_panel(selection_daily, targets, args.lookbacks)
    feature_cols = [c for c in panel.columns if any(c.endswith(f"_{lb}m") for lb in args.lookbacks)]

    ic_edge, ic_edge_summary = ic_summary(panel, feature_cols, "target_edge_mean_bps", args.min_target_trades)
    ic_win, ic_win_summary = ic_summary(panel, feature_cols, "target_win_ratio", args.min_target_trades)
    deciles = []
    for feature in feature_cols:
        row = {"feature": feature, "target": "target_edge_mean_bps"}
        row.update(decile_spread(panel, feature, "target_edge_mean_bps", args.min_target_trades))
        deciles.append(row)
    decile_df = pd.DataFrame(deciles).sort_values("top_minus_bottom", ascending=False)

    daily.to_csv(out_dir / "daily_panel.csv", index=False)
    targets.to_csv(out_dir / "monthly_targets.csv", index=False)
    panel.to_csv(out_dir / "feature_target_panel.csv", index=False)
    ic_edge.to_csv(out_dir / "ic_edge_by_month.csv", index=False)
    ic_edge_summary.to_csv(out_dir / "ic_edge_summary.csv", index=False)
    ic_win.to_csv(out_dir / "ic_win_by_month.csv", index=False)
    ic_win_summary.to_csv(out_dir / "ic_win_summary.csv", index=False)
    decile_df.to_csv(out_dir / "decile_spreads.csv", index=False)

    candidate_features = (
        ic_edge_summary.merge(decile_df[["feature", "top_minus_bottom", "top_avg", "bottom_avg"]], on="feature", how="left")
        .sort_values(["mean_ic", "top_minus_bottom"], ascending=[False, False])
        .head(args.max_features)
    )

    rotation_rows = []
    rotation_daily = {}
    static_returns = simulate_static(daily, static_tickers, params)
    for segment, start, end in [
        ("static_full", args.start, args.end),
        ("static_train", args.start, args.train_end),
        ("static_validation", args.validation_start, args.validation_end),
        ("static_oos", args.oos_start, args.end),
    ]:
        row = {"model": "static_stable_ah_top10", "feature": "static", "top_n": len([t for t in static_tickers if t != "SPY"])}
        row.update(segment_metrics(segment.replace("static_", ""), static_returns, start, end))
        rotation_rows.append(row)

    for _, feat_row in candidate_features.iterrows():
        feature = feat_row["feature"]
        for top_n in args.top_n:
            returns = simulate_rotation(daily, panel, feature, top_n, args.min_lookback_trades, params)
            rotation_daily[f"{feature}_top{top_n}"] = returns
            for segment, start, end in [
                ("full", args.start, args.end),
                ("train", args.start, args.train_end),
                ("validation", args.validation_start, args.validation_end),
                ("oos", args.oos_start, args.end),
            ]:
                row = {"model": "monthly_rotation", "feature": feature, "top_n": top_n}
                row.update(segment_metrics(segment, returns, start, end))
                rotation_rows.append(row)

    rotation_summary = pd.DataFrame(rotation_rows)
    rotation_summary.to_csv(out_dir / "rotation_summary.csv", index=False)
    if rotation_daily:
        pd.DataFrame(rotation_daily).to_csv(out_dir / "rotation_daily_returns.csv")

    universe_dir = out_dir / "monthly_universes"
    universe_dir.mkdir(exist_ok=True)
    universe_index = []
    for _, feat_row in candidate_features.iterrows():
        feature = feat_row["feature"]
        for top_n in args.top_n:
            universe, detail = ranked_monthly_symbols(panel, feature, top_n, args.min_lookback_trades)
            base = f"{safe_name(feature)}_top{top_n}"
            universe_path = universe_dir / f"{base}.csv"
            detail_path = universe_dir / f"{base}_detail.csv"
            universe.to_csv(universe_path, sep=";", index=False)
            detail.to_csv(detail_path, index=False)
            universe_index.append(
                {
                    "feature": feature,
                    "top_n": top_n,
                    "monthly_universe_file": str(universe_path.resolve().relative_to(ROOT)),
                    "detail_file": str(detail_path.resolve().relative_to(ROOT)),
                }
            )
    pd.DataFrame(universe_index).to_csv(universe_dir / "index.csv", index=False)

    report = []
    report.append("# OvernightAH Edge Prediction Study")
    report.append("")
    report.append(f"Universe: `{args.ticker_file}`; data: `{args.data_dir}`; symbols loaded: {daily['ticker'].nunique()}; missing: {missing}")
    report.append(f"Strategy filters: min_vol={args.min_intraday_vol}, max_vol={args.max_intraday_vol}, ah_lag1>={args.ah_lag1_threshold}, min_adv={args.min_adv:.0f}")
    report.append("")
    report.append("## Top Edge IC Features")
    report.append(candidate_features.head(15).to_markdown(index=False))
    report.append("")
    report.append("## Top Decile Spreads")
    report.append(decile_df.head(15).to_markdown(index=False))
    report.append("")
    report.append("## Rotation OOS Top")
    oos = rotation_summary[rotation_summary["segment"] == "oos"].sort_values(["final_equity", "sharpe"], ascending=[False, False])
    report.append(oos.head(20).to_markdown(index=False))
    report.append("")
    report.append("## Monthly Universe Exports")
    index_rel = (universe_dir / "index.csv").resolve().relative_to(ROOT)
    report.append(f"Wrote `{index_rel}` for exact Backtrader validation via `monthly_universe_file`.")
    (out_dir / "summary.md").write_text("\n".join(report))

    print(f"wrote {out_dir}")
    print("Top edge IC:")
    print(candidate_features.head(10).to_string(index=False))
    print("Top OOS rotations:")
    print(oos.head(10).to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OvernightAH monthly edge prediction/downselect study")
    parser.add_argument("--ticker-file", type=Path, default=DEFAULT_TICKERS)
    parser.add_argument("--static-ticker-file", type=Path, default=DEFAULT_STATIC)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--start", default="2016-01-04")
    parser.add_argument("--end", default=None)
    parser.add_argument("--train-end", default="2020-12-31")
    parser.add_argument("--validation-start", default="2021-01-01")
    parser.add_argument("--validation-end", default="2023-12-31")
    parser.add_argument("--oos-start", default="2024-01-01")
    parser.add_argument("--lookbacks", type=int, nargs="+", default=[1, 3, 6, 12])
    parser.add_argument("--top-n", type=int, nargs="+", default=[5, 10, 15, 20])
    parser.add_argument("--max-features", type=int, default=12)
    parser.add_argument("--min-target-trades", type=int, default=2)
    parser.add_argument("--min-lookback-trades", type=int, default=8)
    parser.add_argument("--min-intraday-vol", type=float, default=0.025)
    parser.add_argument("--max-intraday-vol", type=float, default=0.045)
    parser.add_argument("--ah-lag1-threshold", type=float, default=-0.1)
    parser.add_argument("--min-adv", type=float, default=100_000_000)
    parser.add_argument("--min-price", type=float, default=0.0)
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument("--max-exposure", type=float, default=2.0)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
