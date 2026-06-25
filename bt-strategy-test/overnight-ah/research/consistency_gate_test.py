#!/usr/bin/env python3
"""
Consistency gates for OvernightAH portfolio returns.

Tests rules like:
  OFF when the latest N portfolio returns fall into the lower tail of the
  historical distribution; resume when the recent statistic returns to a
  higher quantile.

All gate decisions are shifted one bar: today's exposure only uses returns
known before today's MOC entry.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_DAILY = Path(__file__).resolve().parent / "out" / "symbol_feature_model_stable" / "feature_model_daily.csv"
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "consistency_gate"


def max_drawdown(returns: pd.Series) -> float:
    eq = (1 + returns.fillna(0)).cumprod()
    return float((eq / eq.cummax() - 1).min()) if len(eq) else np.nan


def sharpe(returns: pd.Series) -> float:
    std = returns.std(ddof=1)
    if std == 0 or pd.isna(std):
        return np.nan
    return float(returns.mean() / std * np.sqrt(252))


def metrics(daily: pd.DataFrame, label: str) -> dict:
    r = daily["gated_ret"].dropna()
    exposure = daily["exposure"]
    return {
        "strategy": label,
        "days": int(len(r)),
        "active_pct": float((exposure > 0).mean() * 100),
        "avg_exposure_pct": float(exposure.mean() * 100),
        "total_pct": float(((1 + r).prod() - 1) * 100),
        "mean_bps": float(r.mean() * 10000),
        "std_bps": float(r.std(ddof=1) * 10000),
        "sharpe": sharpe(r),
        "maxdd_pct": max_drawdown(r) * 100,
        "win_rate_pct": float((r > 0).mean() * 100),
        "switches_off": int(daily["switch_off"].sum()),
    }


def rolling_t_pvalue(sample: pd.Series, hist_mean: pd.Series, hist_std: pd.Series, window: int) -> pd.Series:
    try:
        from scipy import stats
    except Exception:
        return pd.Series(np.nan, index=sample.index)
    t_stat = (sample - hist_mean) / (hist_std / np.sqrt(window))
    return pd.Series(stats.t.cdf(t_stat, df=max(window - 1, 1)), index=sample.index)


def apply_gate(signal: pd.Series, resume: pd.Series, off_exposure: float) -> pd.Series:
    state = []
    on = True
    for sig, res in zip(signal.fillna(False), resume.fillna(False)):
        if on and bool(sig):
            on = False
        elif not on and bool(res):
            on = True
        state.append(1.0 if on else off_exposure)
    return pd.Series(state, index=signal.index)


def evaluate_quantile(
    base: pd.DataFrame,
    windows: list[int],
    off_qs: list[float],
    on_qs: list[float],
    off_exposures: list[float],
    min_train: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    daily_frames = []
    ret = base["ret"].reset_index(drop=True)

    for window in windows:
        recent_sum = ret.rolling(window).sum().shift(1)
        recent_mean = ret.rolling(window).mean().shift(1)
        for stat_name, stat in [("sum", recent_sum), ("mean", recent_mean)]:
            hist_source = ret.rolling(window).sum() if stat_name == "sum" else ret.rolling(window).mean()
            for off_q in off_qs:
                low = hist_source.expanding(min_periods=min_train).quantile(off_q).shift(1)
                for on_q in on_qs:
                    high = hist_source.expanding(min_periods=min_train).quantile(on_q).shift(1)
                    signal = stat < low
                    resume = stat > high
                    for off_exp in off_exposures:
                        exposure = apply_gate(signal, resume, off_exp)
                        out = base.copy()
                        out["exposure"] = exposure.to_numpy()
                        out["gated_ret"] = out["ret"] * out["exposure"]
                        out["switch_off"] = ((exposure < 1) & (exposure.shift(1, fill_value=1) == 1)).astype(int)
                        label = f"q_{stat_name}_w{window}_offq{off_q}_onq{on_q}_off{off_exp}"
                        row = metrics(out, label)
                        row.update({"gate": "quantile", "stat": stat_name, "window": window, "off_q": off_q, "on_q": on_q, "off_exposure": off_exp})
                        rows.append(row)
                        out["strategy"] = label
                        daily_frames.append(out)
    return pd.DataFrame(rows), pd.concat(daily_frames, ignore_index=True)


def evaluate_ttest(
    base: pd.DataFrame,
    windows: list[int],
    pvalues: list[float],
    resume_pvalues: list[float],
    off_exposures: list[float],
    min_train: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    daily_frames = []
    ret = base["ret"].reset_index(drop=True)
    hist_mean = ret.expanding(min_periods=min_train).mean().shift(1)
    hist_std = ret.expanding(min_periods=min_train).std().shift(1)
    for window in windows:
        recent_mean = ret.rolling(window).mean().shift(1)
        p = rolling_t_pvalue(recent_mean, hist_mean, hist_std, window)
        for pv in pvalues:
            for resume_pv in resume_pvalues:
                signal = p < pv
                resume = p > resume_pv
                for off_exp in off_exposures:
                    exposure = apply_gate(signal, resume, off_exp)
                    out = base.copy()
                    out["exposure"] = exposure.to_numpy()
                    out["gated_ret"] = out["ret"] * out["exposure"]
                    out["switch_off"] = ((exposure < 1) & (exposure.shift(1, fill_value=1) == 1)).astype(int)
                    label = f"ttest_w{window}_p{pv}_resume{resume_pv}_off{off_exp}"
                    row = metrics(out, label)
                    row.update({"gate": "ttest", "stat": "mean", "window": window, "off_q": pv, "on_q": resume_pv, "off_exposure": off_exp})
                    rows.append(row)
                    out["strategy"] = label
                    daily_frames.append(out)
    return pd.DataFrame(rows), pd.concat(daily_frames, ignore_index=True)


def annual(daily: pd.DataFrame) -> pd.DataFrame:
    tmp = daily.copy()
    tmp["year"] = tmp["date"].dt.year
    return tmp.groupby("year").agg(
        days=("ret", "count"),
        base_pct=("ret", lambda x: ((1 + x).prod() - 1) * 100),
        gated_pct=("gated_ret", lambda x: ((1 + x).prod() - 1) * 100),
        avg_exposure_pct=("exposure", lambda x: x.mean() * 100),
    ).reset_index()


def main() -> None:
    parser = argparse.ArgumentParser(description="OvernightAH consistency gate test")
    parser.add_argument("--daily-file", type=Path, default=DEFAULT_DAILY)
    parser.add_argument("--strategy", default="stable_order")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--windows", default="3,5,10")
    parser.add_argument("--off-qs", default="0.05,0.10")
    parser.add_argument("--on-qs", default="0.40,0.50,0.60")
    parser.add_argument("--off-exposures", default="0,0.5")
    parser.add_argument("--pvalues", default="0.05,0.10")
    parser.add_argument("--resume-pvalues", default="0.40,0.50,0.60")
    parser.add_argument("--min-train", type=int, default=252)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    daily = pd.read_csv(args.daily_file, parse_dates=["date"])
    base = daily[daily["strategy"] == args.strategy][["date", "n", "ret", "tickers"]].sort_values("date").reset_index(drop=True)
    base["exposure"] = 1.0
    base["gated_ret"] = base["ret"]
    base["switch_off"] = 0

    windows = [int(x) for x in args.windows.split(",") if x]
    off_qs = [float(x) for x in args.off_qs.split(",") if x]
    on_qs = [float(x) for x in args.on_qs.split(",") if x]
    off_exposures = [float(x) for x in args.off_exposures.split(",") if x]
    pvalues = [float(x) for x in args.pvalues.split(",") if x]
    resume_pvalues = [float(x) for x in args.resume_pvalues.split(",") if x]

    q_metrics, q_daily = evaluate_quantile(base, windows, off_qs, on_qs, off_exposures, args.min_train)
    t_metrics, t_daily = evaluate_ttest(base, windows, pvalues, resume_pvalues, off_exposures, args.min_train)
    base_row = metrics(base, "base")
    metrics_df = pd.concat([pd.DataFrame([base_row]), q_metrics, t_metrics], ignore_index=True)
    base_metrics = metrics_df.iloc[0]
    metrics_df["delta_sharpe"] = metrics_df["sharpe"] - base_metrics["sharpe"]
    metrics_df["delta_total_pp"] = metrics_df["total_pct"] - base_metrics["total_pct"]
    metrics_df["delta_maxdd_pp"] = metrics_df["maxdd_pct"] - base_metrics["maxdd_pct"]

    all_daily = pd.concat([base.assign(strategy="base"), q_daily, t_daily], ignore_index=True)
    metrics_path = args.out_dir / "consistency_gate_metrics.csv"
    daily_path = args.out_dir / "consistency_gate_daily.csv"
    metrics_df.to_csv(metrics_path, index=False)
    all_daily.to_csv(daily_path, index=False)

    cols = ["strategy", "gate", "stat", "window", "off_q", "on_q", "off_exposure", "active_pct", "avg_exposure_pct", "total_pct", "sharpe", "maxdd_pct", "switches_off", "delta_total_pp", "delta_sharpe", "delta_maxdd_pp"]
    print("BASE")
    print(metrics_df[metrics_df["strategy"] == "base"].to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("\nTOP BY SHARPE")
    print(metrics_df[metrics_df["strategy"] != "base"].sort_values(["sharpe", "total_pct"], ascending=False)[cols].head(20).to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("\nTOP WITH AVG EXPOSURE >= 85%")
    top_cons = metrics_df[(metrics_df["strategy"] != "base") & (metrics_df["avg_exposure_pct"] >= 85)].sort_values(["sharpe", "total_pct"], ascending=False).head(20)
    print(top_cons[cols].to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    best = top_cons.iloc[0] if not top_cons.empty else metrics_df[metrics_df["strategy"] != "base"].sort_values(["sharpe", "total_pct"], ascending=False).iloc[0]
    best_daily = all_daily[all_daily["strategy"] == best["strategy"]]
    annual_path = args.out_dir / "consistency_gate_best_annual.csv"
    annual(best_daily).to_csv(annual_path, index=False)
    print(f"\nBEST ANNUAL: {best['strategy']}")
    print(annual(best_daily).to_string(index=False, float_format=lambda x: f"{x:.3f}"))
    print(f"\nOutputs:\n  {metrics_path}\n  {daily_path}\n  {annual_path}")


if __name__ == "__main__":
    main()
