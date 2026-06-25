#!/usr/bin/env python3
"""
regime_gate_study.py - Study OvernightAH consistency gates.

Simulates the current OvernightAH daily selection on local cached data, then
tests out-of-sample-style gates using only past portfolio overnight returns:

  - z: recent mean return vs expanding historical distribution
  - slope: linear regression slope/t-stat on recent cumulative returns
  - both: z and slope must both signal inconsistency

The gate can either stop trading or throttle exposure. This script is a study
tool only; it does not change the live strategy.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_TICKERS = ROOT / "config-common" / "tickers" / "stable_ah_top10.json"
DEFAULT_OUT = Path(__file__).resolve().parent / "out"


def load_tickers(path: Path) -> list[str]:
    with open(path) as f:
        return [t for t in json.load(f) if t != "SPY"]


def _find_col(df: pd.DataFrame, names: list[str]) -> str | None:
    lookup = {str(c).lower(): c for c in df.columns}
    for name in names:
        if name.lower() in lookup:
            return lookup[name.lower()]
    return None


def load_symbol(path: Path, start: pd.Timestamp) -> pd.DataFrame:
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
        elif key in {"adj_open", "adjopen"}:
            rename[col] = "adj_open"
        elif key in {"adj_high", "adjhigh"}:
            rename[col] = "adj_high"
        elif key in {"adj_low", "adjlow"}:
            rename[col] = "adj_low"
    raw = raw.rename(columns=rename)

    required = {"date", "open", "high", "low", "close"}
    if not required.issubset(raw.columns):
        return pd.DataFrame()

    # If only adj_close is available, derive adjusted OHLC for indicator use.
    if "adj_close" in raw.columns and "adj_open" not in raw.columns:
        close = pd.to_numeric(raw["close"], errors="coerce").replace(0, np.nan)
        factor = pd.to_numeric(raw["adj_close"], errors="coerce") / close
        for src, dst in [("open", "adj_open"), ("high", "adj_high"), ("low", "adj_low")]:
            raw[dst] = pd.to_numeric(raw[src], errors="coerce") * factor

    for col in ["open", "high", "low", "close", "adj_open", "adj_high", "adj_low", "adj_close"]:
        if col in raw.columns:
            raw[col] = pd.to_numeric(raw[col], errors="coerce")

    return raw.dropna(subset=["open", "high", "low", "close"])


def build_portfolio_returns(
    tickers: list[str],
    data_dir: Path,
    start: str,
    top: int,
    min_vol: float,
    max_vol: float,
    ah_lag1_threshold: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    start_ts = pd.Timestamp(start)
    order = {ticker: i for i, ticker in enumerate(tickers)}
    frames = []

    for ticker in tickers:
        path = data_dir / f"{ticker}.csv"
        if not path.exists():
            print(f"[warn] missing data for {ticker}: {path}")
            continue
        df = load_symbol(path, start_ts)
        if df.empty:
            print(f"[warn] unusable data for {ticker}: {path}")
            continue

        df["ticker"] = ticker
        df["rth_vol"] = (df["high"] - df["low"]) / df["open"]
        df["ah_ret"] = (df["open"].shift(-1) - df["close"]) / df["close"]
        df["ah_lag1"] = (df["open"] - df["close"].shift(1)) / df["close"].shift(1)
        if "adj_close" in df.columns and "adj_open" in df.columns:
            df["ind_ah_ret"] = (df["adj_open"].shift(-1) - df["adj_close"]) / df["adj_close"]
        else:
            df["ind_ah_ret"] = df["ah_ret"]
        frames.append(df[["date", "ticker", "rth_vol", "ah_ret", "ind_ah_ret", "ah_lag1"]].dropna())

    if not frames:
        return pd.DataFrame(), pd.DataFrame()

    all_df = pd.concat(frames).sort_values(["date", "ticker"]).reset_index(drop=True)
    selected_rows = []
    portfolio_rows = []

    for date, day in all_df.groupby("date"):
        candidates = day[
            (day["rth_vol"] >= min_vol)
            & (day["rth_vol"] <= max_vol)
        ].copy()
        if ah_lag1_threshold < 0:
            candidates = candidates[candidates["ah_lag1"] >= ah_lag1_threshold]
        if candidates.empty:
            continue

        candidates["order"] = candidates["ticker"].map(order)
        selected = candidates.sort_values("order").head(top).copy()
        selected_rows.append(selected.assign(signal_date=date))
        portfolio_rows.append(
            {
                "date": date,
                "n": len(selected),
                "ret": selected["ah_ret"].mean(),
                "ind_ret": selected["ind_ah_ret"].mean(),
                "tickers": ",".join(selected["ticker"].tolist()),
            }
        )

    portfolio = pd.DataFrame(portfolio_rows).sort_values("date").reset_index(drop=True)
    selected = pd.concat(selected_rows).reset_index(drop=True) if selected_rows else pd.DataFrame()
    return portfolio, selected


def max_drawdown(returns: pd.Series) -> float:
    eq = (1 + returns.fillna(0)).cumprod()
    return (eq / eq.cummax() - 1).min()


def sharpe(returns: pd.Series) -> float:
    std = returns.std()
    if std == 0 or pd.isna(std):
        return np.nan
    return returns.mean() / std * np.sqrt(252)


def metrics(returns: pd.Series, label: str, exposure: pd.Series | None = None) -> dict:
    exposure = exposure if exposure is not None else pd.Series(1.0, index=returns.index)
    return {
        "strategy": label,
        "days": int(len(returns)),
        "active_pct": float((exposure > 0).mean() * 100),
        "avg_exposure_pct": float(exposure.mean() * 100),
        "total_pct": float(((1 + returns).prod() - 1) * 100),
        "mean_bps": float(returns.mean() * 10000),
        "std_bps": float(returns.std() * 10000),
        "sharpe": float(sharpe(returns)),
        "maxdd_pct": float(max_drawdown(returns) * 100),
        "win_rate_pct": float((returns > 0).mean() * 100),
    }


def rolling_slope_t(values: pd.Series, window: int) -> tuple[pd.Series, pd.Series]:
    arr = values.to_numpy(float)
    slopes = np.full(len(arr), np.nan)
    tstats = np.full(len(arr), np.nan)
    x = np.arange(window, dtype=float)
    x_center = x - x.mean()
    sxx = float((x_center ** 2).sum())

    for i in range(window - 1, len(arr)):
        y = np.cumsum(arr[i - window + 1 : i + 1])
        y_center = y - y.mean()
        slope = float((x_center * y_center).sum() / sxx)
        fitted = y.mean() + slope * x_center
        resid = y - fitted
        if window > 2:
            se2 = float((resid ** 2).sum() / (window - 2))
            stderr = math.sqrt(se2 / sxx) if se2 >= 0 else np.nan
        else:
            stderr = np.nan
        slopes[i] = slope
        tstats[i] = slope / stderr if stderr and stderr > 0 else np.nan

    # Shift one bar: today can only use information known before entering.
    return pd.Series(slopes).shift(1), pd.Series(tstats).shift(1)


def apply_state(signal: pd.Series, resume: pd.Series, off_exposure: float) -> tuple[pd.Series, int]:
    exposure = []
    on = True
    switches_off = 0
    for i in range(len(signal)):
        sig = bool(signal.iloc[i]) if pd.notna(signal.iloc[i]) else False
        ok = bool(resume.iloc[i]) if pd.notna(resume.iloc[i]) else False
        if on and sig:
            on = False
            switches_off += 1
        elif not on and ok:
            on = True
        exposure.append(1.0 if on else off_exposure)
    return pd.Series(exposure), switches_off


def evaluate_gates(
    portfolio: pd.DataFrame,
    windows: list[int],
    z_thresholds: list[float],
    slope_t_thresholds: list[float],
    off_exposures: list[float],
    min_train: int,
) -> pd.DataFrame:
    base_ret = portfolio["ret"].reset_index(drop=True)
    ind_ret = portfolio["ind_ret"].reset_index(drop=True)
    rows = [metrics(base_ret, "base")]

    for window in windows:
        hist_mu = ind_ret.expanding(min_periods=max(min_train, window + 20)).mean().shift(window)
        hist_sd = ind_ret.expanding(min_periods=max(min_train, window + 20)).std().shift(window)
        recent_mean = ind_ret.rolling(window).mean().shift(1)
        z = (recent_mean - hist_mu) / (hist_sd / np.sqrt(window))
        slope, slope_t = rolling_slope_t(ind_ret, window)

        for z_thr in z_thresholds:
            for slope_t_thr in slope_t_thresholds:
                gate_defs = [
                    ("z", z < z_thr, z > -0.2),
                    ("slope", slope_t < slope_t_thr, slope > 0),
                    ("both", (z < z_thr) & (slope_t < slope_t_thr), (z > -0.2) | (slope > 0)),
                ]
                for gate_name, signal, resume in gate_defs:
                    for off_exp in off_exposures:
                        exposure, switches = apply_state(
                            signal.fillna(False),
                            resume.fillna(False),
                            off_exp,
                        )
                        gated_ret = base_ret * exposure
                        row = metrics(
                            gated_ret,
                            f"{gate_name}_w{window}_z{z_thr}_t{slope_t_thr}_off{off_exp}",
                            exposure,
                        )
                        row.update(
                            {
                                "gate": gate_name,
                                "window": window,
                                "z_threshold": z_thr,
                                "slope_t_threshold": slope_t_thr,
                                "off_exposure": off_exp,
                                "switches_off": switches,
                            }
                        )
                        rows.append(row)

    result = pd.DataFrame(rows)
    base = result.iloc[0]
    result["delta_sharpe"] = result["sharpe"] - base["sharpe"]
    result["delta_total_pp"] = result["total_pct"] - base["total_pct"]
    result["delta_maxdd_pp"] = result["maxdd_pct"] - base["maxdd_pct"]
    return result


def annual_breakdown(portfolio: pd.DataFrame, gate_row: pd.Series) -> pd.DataFrame:
    if gate_row["strategy"] == "base":
        tmp = portfolio.copy()
        tmp["gated_ret"] = tmp["ret"]
        tmp["exposure"] = 1.0
    else:
        ind_ret = portfolio["ind_ret"].reset_index(drop=True)
        window = int(gate_row["window"])
        hist_mu = ind_ret.expanding(min_periods=max(80, window + 20)).mean().shift(window)
        hist_sd = ind_ret.expanding(min_periods=max(80, window + 20)).std().shift(window)
        recent_mean = ind_ret.rolling(window).mean().shift(1)
        z = (recent_mean - hist_mu) / (hist_sd / np.sqrt(window))
        slope, slope_t = rolling_slope_t(ind_ret, window)
        if gate_row["gate"] == "z":
            signal = z < float(gate_row["z_threshold"])
            resume = z > -0.2
        elif gate_row["gate"] == "slope":
            signal = slope_t < float(gate_row["slope_t_threshold"])
            resume = slope > 0
        else:
            signal = (z < float(gate_row["z_threshold"])) & (
                slope_t < float(gate_row["slope_t_threshold"])
            )
            resume = (z > -0.2) | (slope > 0)
        exposure, _ = apply_state(signal.fillna(False), resume.fillna(False), float(gate_row["off_exposure"]))
        tmp = portfolio.copy()
        tmp["exposure"] = exposure.to_numpy()
        tmp["gated_ret"] = tmp["ret"] * tmp["exposure"]

    tmp["year"] = pd.to_datetime(tmp["date"]).dt.year
    return tmp.groupby("year").agg(
        days=("ret", "count"),
        base_pct=("ret", lambda x: ((1 + x).prod() - 1) * 100),
        gated_pct=("gated_ret", lambda x: ((1 + x).prod() - 1) * 100),
        avg_exposure_pct=("exposure", lambda x: x.mean() * 100),
    ).reset_index()


def main() -> None:
    parser = argparse.ArgumentParser(description="OvernightAH regime gate study")
    parser.add_argument("--ticker-file", type=Path, default=DEFAULT_TICKERS)
    parser.add_argument("--data-dir", type=Path, default=ROOT / "config-common" / "data" / "d" / "yahoo")
    parser.add_argument("--start", default="2010-01-01")
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--min-vol", type=float, default=0.025)
    parser.add_argument("--max-vol", type=float, default=0.045)
    parser.add_argument("--ah-lag1-threshold", type=float, default=0.0)
    parser.add_argument("--windows", default="10,20,40,60")
    parser.add_argument("--z-thresholds", default="-1.0,-1.5,-2.0")
    parser.add_argument("--slope-t-thresholds", default="-1.0,-1.5,-2.0")
    parser.add_argument("--off-exposures", default="0,0.25,0.5")
    parser.add_argument("--min-train", type=int, default=80)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    windows = [int(x) for x in args.windows.split(",") if x]
    z_thresholds = [float(x) for x in args.z_thresholds.split(",") if x]
    slope_t_thresholds = [float(x) for x in args.slope_t_thresholds.split(",") if x]
    off_exposures = [float(x) for x in args.off_exposures.split(",") if x]

    args.out_dir.mkdir(parents=True, exist_ok=True)
    tickers = load_tickers(args.ticker_file)
    portfolio, selected = build_portfolio_returns(
        tickers,
        args.data_dir,
        args.start,
        args.top,
        args.min_vol,
        args.max_vol,
        args.ah_lag1_threshold,
    )
    if portfolio.empty:
        raise SystemExit("No portfolio returns generated")

    gates = evaluate_gates(
        portfolio,
        windows,
        z_thresholds,
        slope_t_thresholds,
        off_exposures,
        args.min_train,
    )
    gates_path = args.out_dir / "overnight_regime_gate_sweep.csv"
    portfolio_path = args.out_dir / "overnight_regime_gate_daily.csv"
    selected_path = args.out_dir / "overnight_regime_gate_selected.csv"
    gates.to_csv(gates_path, index=False)
    portfolio.to_csv(portfolio_path, index=False)
    selected.to_csv(selected_path, index=False)

    print(f"Period: {portfolio['date'].min().date()} - {portfolio['date'].max().date()}")
    print(f"Sessions: {len(portfolio)}")
    print("\nBASE")
    print(gates[gates["strategy"] == "base"].to_string(index=False, float_format=lambda x: f"{x:.4f}"))

    print("\nTOP BY SHARPE")
    cols = [
        "strategy", "active_pct", "avg_exposure_pct", "total_pct", "sharpe",
        "maxdd_pct", "delta_sharpe", "delta_total_pp", "delta_maxdd_pp",
        "switches_off",
    ]
    top = gates[gates["strategy"] != "base"].sort_values(
        ["sharpe", "total_pct"], ascending=False
    ).head(15)
    print(top[cols].to_string(index=False, float_format=lambda x: f"{x:.4f}"))

    print("\nCONSERVATIVE TOP (avg exposure >= 80%)")
    conservative = gates[
        (gates["strategy"] != "base")
        & (gates["avg_exposure_pct"] >= 80)
    ].sort_values(["sharpe", "total_pct"], ascending=False).head(15)
    print(conservative[cols].to_string(index=False, float_format=lambda x: f"{x:.4f}"))

    best = conservative.iloc[0] if not conservative.empty else top.iloc[0]
    annual = annual_breakdown(portfolio, best)
    annual_path = args.out_dir / "overnight_regime_gate_best_annual.csv"
    annual.to_csv(annual_path, index=False)
    print(f"\nBEST ANNUAL: {best['strategy']}")
    print(annual.to_string(index=False, float_format=lambda x: f"{x:.3f}"))

    print(f"\nOutputs:\n  {gates_path}\n  {portfolio_path}\n  {selected_path}\n  {annual_path}")


if __name__ == "__main__":
    main()
