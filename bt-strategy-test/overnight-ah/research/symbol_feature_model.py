#!/usr/bin/env python3
"""
Feature-model study for OvernightAH candidate selection.

Builds features available before the MOC entry and tests whether simple
walk-forward models can choose better top-5 candidates than static/ADV ranking.
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
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "symbol_feature_model"
DEFAULT_STATIC = ROOT / "config-common" / "tickers" / "stable_ah_top10.json"


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
    if not {"open", "high", "low", "close", "volume"}.issubset(raw.columns):
        return pd.DataFrame()
    for col in ["open", "high", "low", "close", "volume"]:
        raw[col] = pd.to_numeric(raw[col], errors="coerce")
    raw = raw.dropna(subset=["open", "high", "low", "close", "volume"])
    if raw.empty:
        return pd.DataFrame()

    raw["ticker"] = ticker
    raw["rth_vol"] = (raw["high"] - raw["low"]) / raw["open"]
    raw["rth_ret"] = (raw["close"] - raw["open"]) / raw["open"]
    intraday_range = (raw["high"] - raw["low"]).replace(0, np.nan)
    raw["close_pos"] = (raw["close"] - raw["low"]) / intraday_range
    raw["known_ah"] = (raw["open"] - raw["close"].shift(1)) / raw["close"].shift(1)
    raw["ah_ret"] = (raw["open"].shift(-1) - raw["close"]) / raw["close"]
    raw["cc_ret_3"] = raw["close"].pct_change(3)
    raw["cc_ret_5"] = raw["close"].pct_change(5)
    raw["cc_ret_10"] = raw["close"].pct_change(10)
    raw["cc_ret_20"] = raw["close"].pct_change(20)
    raw["dollar_volume"] = raw["close"] * raw["volume"]
    raw["adv20"] = raw["dollar_volume"].rolling(20).mean().shift(1)
    raw["rel_volume"] = raw["volume"] / raw["volume"].rolling(20).mean().shift(1)

    # Recent realized overnight behavior, shifted to avoid using target.
    valid_ah = raw["ah_ret"].shift(1)
    for win in [5, 10, 20, 60]:
        raw[f"ah_mean_{win}"] = valid_ah.rolling(win).mean()
        raw[f"ah_sum_{win}"] = valid_ah.rolling(win).sum()
        raw[f"ah_win_{win}"] = (valid_ah > 0).rolling(win).mean()
        eq = (1 + valid_ah.fillna(0)).rolling(win).apply(
            lambda x: np.min(np.cumprod(1 + x) / np.maximum.accumulate(np.cumprod(1 + x)) - 1),
            raw=True,
        )
        raw[f"ah_dd_{win}"] = eq
    return raw


def max_drawdown(returns: pd.Series) -> float:
    eq = (1 + returns.fillna(0)).cumprod()
    return float((eq / eq.cummax() - 1).min()) if len(eq) else np.nan


def sharpe(returns: pd.Series) -> float:
    std = returns.std(ddof=1)
    if std == 0 or pd.isna(std):
        return np.nan
    return float(returns.mean() / std * np.sqrt(252))


def metrics(daily: pd.DataFrame, label: str) -> dict:
    r = daily["ret"].dropna()
    return {
        "strategy": label,
        "days": int(len(r)),
        "avg_n": float(daily["n"].mean()) if len(daily) else np.nan,
        "total_pct": float(((1 + r).prod() - 1) * 100) if len(r) else np.nan,
        "mean_bps": float(r.mean() * 10000) if len(r) else np.nan,
        "std_bps": float(r.std(ddof=1) * 10000) if len(r) > 1 else np.nan,
        "sharpe": sharpe(r),
        "maxdd_pct": max_drawdown(r) * 100 if len(r) else np.nan,
        "win_rate_pct": float((r > 0).mean() * 100) if len(r) else np.nan,
    }


def build_dataset(
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
    start_ts = pd.Timestamp(start) - pd.Timedelta(days=360)
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
            & (df["known_ah"] >= ah_lag1_threshold)
            & (df["adv20"] >= min_adv)
            & (df["close"] >= min_price)
            & df["ah_ret"].notna()
        )
        frames.append(df[df["date"] >= pd.Timestamp(start)])
    if not frames:
        return pd.DataFrame()
    panel = pd.concat(frames, ignore_index=True).sort_values(["date", "ticker"])
    panel["log_adv20"] = np.log(panel["adv20"].replace(0, np.nan))
    return panel[panel["passes_filters"]].copy()


def add_cross_sectional_features(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        out[f"{col}_rank"] = out.groupby("date")[col].rank(pct=True)
    return out


def annual(daily: pd.DataFrame, label: str) -> pd.DataFrame:
    tmp = daily.copy()
    tmp["year"] = tmp["date"].dt.year
    out = tmp.groupby("year").agg(
        days=("ret", "count"),
        total_pct=("ret", lambda x: ((1 + x).prod() - 1) * 100),
        avg_n=("n", "mean"),
    ).reset_index()
    out["strategy"] = label
    return out


def simulate_rank(df: pd.DataFrame, score_col: str, top: int, label: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    selected = []
    for date, day in df.dropna(subset=[score_col, "ah_ret"]).groupby("date"):
        sel = day.sort_values(score_col, ascending=False).head(top).copy()
        if sel.empty:
            continue
        sel["strategy"] = label
        selected.append(sel)
        rows.append({"date": date, "n": len(sel), "ret": sel["ah_ret"].mean(), "strategy": label, "tickers": ",".join(sel["ticker"])})
    return pd.DataFrame(rows), pd.concat(selected, ignore_index=True) if selected else pd.DataFrame()


def fit_predict_walkforward(df: pd.DataFrame, features: list[str], top: int, train_months: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    data = df.dropna(subset=["ah_ret"]).copy()
    data["month"] = data["date"].dt.to_period("M").dt.to_timestamp("M")
    months = sorted(data["month"].unique())
    preds = []
    coefs = []

    models = {
        "ridge": make_pipeline(SimpleImputer(strategy="median"), StandardScaler(), Ridge(alpha=10.0)),
        "hgb": make_pipeline(SimpleImputer(strategy="median"), HistGradientBoostingRegressor(max_iter=80, learning_rate=0.05, max_leaf_nodes=8, l2_regularization=1.0, random_state=42)),
    }

    for month in months:
        train_start = pd.Timestamp(month) - pd.DateOffset(months=train_months)
        train = data[(data["month"] < month) & (data["month"] >= train_start)]
        test = data[data["month"] == month].copy()
        if len(train) < 1000 or test.empty:
            continue
        x_train = train[features]
        y_train = train["ah_ret"]
        x_test = test[features]
        for name, model in models.items():
            fitted = model.fit(x_train, y_train)
            tmp = test[["date", "ticker", "ah_ret"] + features].copy()
            tmp["score"] = fitted.predict(x_test)
            tmp["model"] = name
            preds.append(tmp)
            if name == "ridge":
                ridge = fitted.named_steps["ridge"]
                for feature, coef in zip(features, ridge.coef_):
                    coefs.append({"month": month, "feature": feature, "coef": coef})

    pred_df = pd.concat(preds, ignore_index=True) if preds else pd.DataFrame()
    daily_frames = []
    selected_frames = []
    for model_name, group in pred_df.groupby("model"):
        daily, selected = simulate_rank(group, "score", top, f"model_{model_name}")
        daily_frames.append(daily)
        selected_frames.append(selected)
    daily_out = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
    selected_out = pd.concat(selected_frames, ignore_index=True) if selected_frames else pd.DataFrame()
    coef_out = pd.DataFrame(coefs)
    return daily_out, selected_out, coef_out


def fit_predict_subset_walkforward(
    train_df: pd.DataFrame,
    select_df: pd.DataFrame,
    features: list[str],
    top: int,
    train_months: int,
    label_prefix: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    train_data = train_df.dropna(subset=["ah_ret"]).copy()
    select_data = select_df.dropna(subset=["ah_ret"]).copy()
    train_data["month"] = train_data["date"].dt.to_period("M").dt.to_timestamp("M")
    select_data["month"] = select_data["date"].dt.to_period("M").dt.to_timestamp("M")
    months = sorted(select_data["month"].unique())
    preds = []
    model = make_pipeline(SimpleImputer(strategy="median"), StandardScaler(), Ridge(alpha=10.0))

    for month in months:
        train_start = pd.Timestamp(month) - pd.DateOffset(months=train_months)
        train = train_data[(train_data["month"] < month) & (train_data["month"] >= train_start)]
        test = select_data[select_data["month"] == month].copy()
        if len(train) < 500 or test.empty:
            continue
        fitted = model.fit(train[features], train["ah_ret"])
        tmp = test[["date", "ticker", "ah_ret"] + features].copy()
        tmp["score"] = fitted.predict(test[features])
        tmp["model"] = f"{label_prefix}_ridge"
        preds.append(tmp)

    pred_df = pd.concat(preds, ignore_index=True) if preds else pd.DataFrame()
    if pred_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    return simulate_rank(pred_df, "score", top, f"{label_prefix}_ridge")


def simulate_fixed_order_subset(df: pd.DataFrame, tickers: list[str], top: int, label: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    order = {ticker: i for i, ticker in enumerate(tickers)}
    rows = []
    selected = []
    base = df[df["ticker"].isin(order)].copy()
    base["order"] = base["ticker"].map(order)
    for date, day in base.groupby("date"):
        sel = day.sort_values("order").head(top).copy()
        if sel.empty:
            continue
        sel["strategy"] = label
        selected.append(sel)
        rows.append({"date": date, "n": len(sel), "ret": sel["ah_ret"].mean(), "strategy": label, "tickers": ",".join(sel["ticker"])})
    return pd.DataFrame(rows), pd.concat(selected, ignore_index=True) if selected else pd.DataFrame()


def main() -> None:
    parser = argparse.ArgumentParser(description="OvernightAH feature model study")
    parser.add_argument("--ticker-file", type=Path, default=DEFAULT_TICKERS)
    parser.add_argument("--static-ticker-file", type=Path, default=DEFAULT_STATIC)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--start", default="2010-01-01")
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--train-months", type=int, default=24)
    parser.add_argument("--min-vol", type=float, default=0.025)
    parser.add_argument("--max-vol", type=float, default=0.045)
    parser.add_argument("--ah-lag1-threshold", type=float, default=-0.10)
    parser.add_argument("--min-adv", type=float, default=100_000_000)
    parser.add_argument("--min-price", type=float, default=0.0)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    tickers = load_tickers(args.ticker_file)
    data = build_dataset(tickers, args.data_dir, args.start, args.min_vol, args.max_vol, args.ah_lag1_threshold, args.min_adv, args.min_price)
    if data.empty:
        raise SystemExit("No usable data")

    cs_cols = ["rth_vol", "rth_ret", "close_pos", "known_ah", "cc_ret_5", "cc_ret_20", "log_adv20", "rel_volume", "ah_mean_20", "ah_sum_20", "ah_win_20", "ah_dd_20"]
    data = add_cross_sectional_features(data, cs_cols)
    features = cs_cols + [f"{c}_rank" for c in cs_cols] + ["ah_mean_5", "ah_sum_5", "ah_win_5", "ah_dd_5", "ah_mean_10", "ah_sum_10", "ah_win_10", "ah_dd_10", "ah_mean_60", "ah_sum_60", "ah_win_60", "ah_dd_60"]

    baselines = []
    selected_frames = []
    for score_col, label in [
        ("adv20", "baseline_adv"),
        ("ah_sum_20", "baseline_recent_edge20"),
        ("rth_ret", "baseline_rth_ret"),
        ("close_pos", "baseline_close_pos"),
    ]:
        daily, selected = simulate_rank(data, score_col, args.top, label)
        baselines.append(daily)
        selected_frames.append(selected)

    model_daily, model_selected, coefs = fit_predict_walkforward(data, features, args.top, args.train_months)
    static_tickers = load_tickers(args.static_ticker_file)
    stable_data = data[data["ticker"].isin(static_tickers)].copy()
    stable_order_daily, stable_order_selected = simulate_fixed_order_subset(stable_data, static_tickers, args.top, "stable_order")
    stable_adv_daily, stable_adv_selected = simulate_rank(stable_data, "adv20", args.top, "stable_adv")
    stable_edge_daily, stable_edge_selected = simulate_rank(stable_data, "ah_sum_20", args.top, "stable_recent_edge20")
    stable_ridge_daily, stable_ridge_selected = fit_predict_subset_walkforward(data, stable_data, features, args.top, args.train_months, "stable")

    stable_frames = [stable_order_daily, stable_adv_daily, stable_edge_daily]
    stable_selected_frames = [stable_order_selected, stable_adv_selected, stable_edge_selected]
    if not stable_ridge_daily.empty:
        stable_frames.append(stable_ridge_daily)
        stable_selected_frames.append(stable_ridge_selected)

    daily_all = pd.concat(baselines + stable_frames + ([model_daily] if not model_daily.empty else []), ignore_index=True)
    selected_all = pd.concat(selected_frames + stable_selected_frames + ([model_selected] if not model_selected.empty else []), ignore_index=True)
    metrics_df = pd.DataFrame([metrics(g, name) for name, g in daily_all.groupby("strategy")]).sort_values(["sharpe", "total_pct"], ascending=False)
    annual_df = pd.concat([annual(g, name) for name, g in daily_all.groupby("strategy")]).sort_values(["year", "strategy"])

    data.to_csv(args.out_dir / "feature_candidates.csv", index=False)
    daily_all.to_csv(args.out_dir / "feature_model_daily.csv", index=False)
    selected_all.to_csv(args.out_dir / "feature_model_selected.csv", index=False)
    metrics_df.to_csv(args.out_dir / "feature_model_metrics.csv", index=False)
    annual_df.to_csv(args.out_dir / "feature_model_annual.csv", index=False)
    coefs.to_csv(args.out_dir / "ridge_coefficients.csv", index=False)

    print("METRICS")
    print(metrics_df.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("\nANNUAL TOTAL %")
    print(annual_df.pivot(index="year", columns="strategy", values="total_pct").to_string(float_format=lambda x: f"{x:.2f}"))
    if not coefs.empty:
        coef_summary = coefs.groupby("feature")["coef"].mean().sort_values(key=lambda s: s.abs(), ascending=False).head(25)
        print("\nRIDGE AVG COEF TOP ABS")
        print(coef_summary.to_string(float_format=lambda x: f"{x:.6f}"))
    print("\nOutputs:")
    for name in ["feature_candidates.csv", "feature_model_daily.csv", "feature_model_selected.csv", "feature_model_metrics.csv", "feature_model_annual.csv", "ridge_coefficients.csv"]:
        print(f"  {args.out_dir / name}")


if __name__ == "__main__":
    main()
