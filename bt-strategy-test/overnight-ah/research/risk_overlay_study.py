#!/usr/bin/env python3
"""Study dynamic exposure overlays on an OvernightAH run.

The script uses the run's exported trades plus returns and applies only
information available by the entry evening:

  - strategy trailing returns up to the entry date
  - semiconductor basket close-to-close returns up to the entry date

It is a study tool. It does not change the strategy or the source run.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RUN = ROOT / "out/overnight_ah/OvernightAH/LZ"
DEFAULT_OUT = ROOT / "bt-strategy-test/overnight-ah/research/out/risk_overlay_LZ"
DEFAULT_DATA = ROOT / "config-common/data/d/yahoo_adj"
DEFAULT_SEMIS = "NVDA,AMD,AVGO,MU,ASML,MRVL,ARM,AMAT,LRCX,KLAC,MCHP,ADI,TXN,ON,INTC,GFS"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--semis", default=DEFAULT_SEMIS)
    parser.add_argument("--max-exposure", type=float, default=2.0)
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument("--windows", default="3,5,10")
    parser.add_argument("--thresholds", default="-0.04,-0.06,-0.08,-0.10")
    parser.add_argument("--stress-exposures", default="1.0,1.25,1.5")
    return parser.parse_args()


def resolve(path: Path) -> Path:
    return path if path.is_absolute() else ROOT / path


def csv_date_index(path: Path) -> pd.Series:
    df = pd.read_csv(path, index_col=0)
    series = df.iloc[:, 0].astype(float)
    series.index = pd.to_datetime(series.index).normalize()
    return series.groupby(series.index).sum().sort_index()


def load_trades(path: Path, max_exposure: float, max_concurrent: int) -> pd.DataFrame:
    data = json.loads(path.read_text())
    df = pd.DataFrame(data)
    if df.empty:
        raise ValueError(f"No trades found in {path}")

    df["entry_date"] = pd.to_datetime(df["entry_signal_dt"]).dt.normalize()
    df["close_date"] = pd.to_datetime(df["close_datetime"]).dt.normalize()
    df["pnl_pct"] = pd.to_numeric(df["pnl_pct"], errors="coerce").fillna(0.0) / 100.0
    df["slot_weight"] = max_exposure / max_concurrent
    df["base_return_contribution"] = df["pnl_pct"] * df["slot_weight"]
    return df.sort_values(["entry_date", "asset"]).reset_index(drop=True)


def load_close_series(path: Path) -> pd.Series | None:
    if not path.exists():
        return None
    raw = pd.read_csv(path)
    if raw.empty:
        return None
    date_col = next((c for c in ["Date", "timestamp", "datetime", "date"] if c in raw.columns), raw.columns[0])
    raw[date_col] = pd.to_datetime(raw[date_col], utc=True, errors="coerce")
    raw = raw.dropna(subset=[date_col]).sort_values(date_col)
    close_col = next(
        (c for c in ["Adj Close", "AdjClose", "adj_close", "Close", "close"] if c in raw.columns),
        None,
    )
    if close_col is None:
        return None
    series = pd.to_numeric(raw[close_col], errors="coerce")
    index = raw[date_col].dt.tz_localize(None).dt.normalize()
    out = pd.Series(series.values, index=index).dropna()
    return out[~out.index.duplicated(keep="last")].sort_index()


def semis_factor(data_dir: Path, symbols: list[str]) -> pd.Series:
    returns = []
    for symbol in symbols:
        series = load_close_series(data_dir / f"{symbol}.csv")
        if series is None or series.empty:
            continue
        returns.append(series.pct_change().rename(symbol))
    if not returns:
        return pd.Series(dtype=float)
    frame = pd.concat(returns, axis=1)
    return frame.mean(axis=1, skipna=True).dropna().sort_index()


def max_drawdown(returns: pd.Series) -> float:
    equity = (1.0 + returns.fillna(0.0)).cumprod()
    dd = equity / equity.cummax() - 1.0
    return float(dd.min())


def sharpe(returns: pd.Series) -> float:
    std = returns.std()
    if not std or math.isnan(std):
        return float("nan")
    return float(returns.mean() / std * math.sqrt(252))


def daily_from_trades(trades: pd.DataFrame, scales: pd.Series | None = None) -> pd.Series:
    df = trades.copy()
    if scales is None:
        df["scale"] = 1.0
    else:
        df["scale"] = df["entry_date"].map(scales).fillna(1.0)
    df["return_contribution"] = df["base_return_contribution"] * df["scale"]
    daily = df.groupby("close_date")["return_contribution"].sum()
    return daily.sort_index()


def metric_row(name: str, daily: pd.Series, scales: pd.Series, base_daily: pd.Series) -> dict:
    daily = daily.reindex(base_daily.index, fill_value=0.0)
    equity = (1.0 + daily).cumprod()
    active = scales > 0
    worst_day = daily.idxmin()
    years = max((daily.index.max() - daily.index.min()).days / 365.25, 1e-9)
    cagr = equity.iloc[-1] ** (1.0 / years) - 1.0
    return {
        "overlay": name,
        "days": int(len(daily)),
        "trading_entry_days": int(len(scales)),
        "active_entry_days": int(active.sum()),
        "active_entry_pct": float(active.mean() * 100.0),
        "avg_exposure_scale_pct": float(scales.mean() * 100.0),
        "total_return_pct": float((equity.iloc[-1] - 1.0) * 100.0),
        "cagr_pct": float(cagr * 100.0),
        "sharpe": sharpe(daily),
        "maxdd_pct": max_drawdown(daily) * 100.0,
        "worst_day": worst_day.date().isoformat(),
        "worst_day_ret_pct": float(daily.loc[worst_day] * 100.0),
        "monthly_min_ret_pct": float(daily.groupby(daily.index.to_period("M")).apply(lambda s: (1 + s).prod() - 1).min() * 100.0),
        "return_vs_base_pctpt": float(((1 + daily).prod() - (1 + base_daily).prod()) * 100.0),
        "maxdd_improvement_pctpt": float((max_drawdown(daily) - max_drawdown(base_daily)) * 100.0),
    }


def make_report(summary: pd.DataFrame, out_dir: Path) -> None:
    best_dd = summary.sort_values(["maxdd_pct", "total_return_pct"], ascending=[False, False]).head(12)
    best_calmar = summary.assign(calmar=summary["cagr_pct"] / summary["maxdd_pct"].abs()).sort_values(
        ["calmar", "total_return_pct"], ascending=[False, False]
    ).head(12)
    lines = [
        "# OvernightAH Risk Overlay Study",
        "",
        "Study source: exported `LZ` trades and returns. Overlays use only trailing information available by the entry evening.",
        "",
        "## Best Max Drawdown Improvements",
        "",
        best_dd.to_markdown(index=False, floatfmt=".3f"),
        "",
        "## Best Calmar-Like Ranking",
        "",
        best_calmar.to_markdown(index=False, floatfmt=".3f"),
        "",
        "Positive `maxdd_improvement_pctpt` means the overlay made max drawdown less negative.",
    ]
    (out_dir / "report.md").write_text("\n".join(lines) + "\n")


def period_metric_rows(daily_by_overlay: dict[str, pd.Series], scale_by_overlay: dict[str, pd.Series]) -> pd.DataFrame:
    rows = []
    cutoffs = [
        ("full", None),
        ("2016_plus", pd.Timestamp("2016-01-01")),
        ("2021_plus", pd.Timestamp("2021-01-01")),
        ("2024_plus", pd.Timestamp("2024-01-01")),
    ]
    for overlay, daily in daily_by_overlay.items():
        scales = scale_by_overlay[overlay]
        for label, start in cutoffs:
            sub_daily = daily if start is None else daily[daily.index >= start]
            sub_scales = scales if start is None else scales[scales.index >= start]
            if sub_daily.empty:
                continue
            equity = (1.0 + sub_daily).cumprod()
            years = max((sub_daily.index.max() - sub_daily.index.min()).days / 365.25, 1e-9)
            cagr = equity.iloc[-1] ** (1.0 / years) - 1.0
            rows.append(
                {
                    "overlay": overlay,
                    "period": label,
                    "start": sub_daily.index.min().date().isoformat(),
                    "end": sub_daily.index.max().date().isoformat(),
                    "days": int(len(sub_daily)),
                    "active_entry_pct": float((sub_scales > 0).mean() * 100.0) if len(sub_scales) else float("nan"),
                    "avg_exposure_scale_pct": float(sub_scales.mean() * 100.0) if len(sub_scales) else float("nan"),
                    "total_return_pct": float((equity.iloc[-1] - 1.0) * 100.0),
                    "cagr_pct": float(cagr * 100.0),
                    "sharpe": sharpe(sub_daily),
                    "maxdd_pct": max_drawdown(sub_daily) * 100.0,
                    "worst_day": sub_daily.idxmin().date().isoformat(),
                    "worst_day_ret_pct": float(sub_daily.min() * 100.0),
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    run_dir = resolve(args.run_dir)
    out_dir = resolve(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    windows = [int(x) for x in args.windows.split(",") if x.strip()]
    thresholds = [float(x) for x in args.thresholds.split(",") if x.strip()]
    stress_exposures = [float(x) for x in args.stress_exposures.split(",") if x.strip()]

    trades = load_trades(run_dir / "trades.json", args.max_exposure, args.max_concurrent)
    exported_returns = csv_date_index(run_dir / "returns.csv")
    base_daily = daily_from_trades(trades).reindex(exported_returns.index, fill_value=0.0)
    entry_dates = pd.DatetimeIndex(sorted(trades["entry_date"].dropna().unique()))
    base_entry_signal = exported_returns.reindex(entry_dates).fillna(0.0)

    semis_symbols = [s.strip() for s in args.semis.split(",") if s.strip()]
    semis = semis_factor(resolve(args.data_dir), semis_symbols)
    semis_entry_signal = semis.reindex(entry_dates).ffill().fillna(0.0)

    rows = []
    daily_by_overlay = {}
    scale_by_overlay = {}
    scale_frames = []
    base_scales = pd.Series(1.0, index=entry_dates)
    rows.append(metric_row("baseline_rebuilt", base_daily, base_scales, base_daily))
    daily_by_overlay["baseline_rebuilt"] = base_daily
    scale_by_overlay["baseline_rebuilt"] = base_scales

    signals = {
        "strategy": base_entry_signal,
        "semis": semis_entry_signal,
    }

    for signal_name, signal in signals.items():
        for window in windows:
            rolling = (1.0 + signal).rolling(window).apply(lambda values: float(values.prod() - 1.0), raw=True)
            for threshold in thresholds:
                for exposure in stress_exposures:
                    stress_scale = max(0.0, min(1.0, exposure / args.max_exposure))
                    scales = pd.Series(1.0, index=entry_dates)
                    stress_dates = rolling.reindex(entry_dates) <= threshold
                    scales.loc[stress_dates.fillna(False)] = stress_scale
                    name = f"throttle_{signal_name}_w{window}_thr{threshold:.2f}_exp{exposure:.2f}"
                    daily = daily_from_trades(trades, scales).reindex(base_daily.index, fill_value=0.0)
                    rows.append(metric_row(name, daily, scales, base_daily))
                    daily_by_overlay[name] = daily
                    scale_by_overlay[name] = scales
                    scale_frames.append(pd.DataFrame({"entry_date": entry_dates, "overlay": name, "scale": scales.values}))

    summary = pd.DataFrame(rows).sort_values(["maxdd_pct", "total_return_pct"], ascending=[False, False])
    summary.to_csv(out_dir / "overlay_summary.csv", index=False)
    period_metrics = period_metric_rows(daily_by_overlay, scale_by_overlay)
    period_metrics.to_csv(out_dir / "overlay_period_metrics.csv", index=False)
    daily_returns = pd.DataFrame(daily_by_overlay).sort_index()
    daily_returns.index.name = "date"
    daily_returns.to_csv(out_dir / "overlay_daily_returns.csv")
    pd.concat(scale_frames, ignore_index=True).to_csv(out_dir / "overlay_entry_scales.csv", index=False)

    validation = pd.DataFrame(
        {
            "date": exported_returns.index,
            "exported_return": exported_returns.values,
            "rebuilt_return": base_daily.reindex(exported_returns.index, fill_value=0.0).values,
        }
    )
    validation["diff"] = validation["rebuilt_return"] - validation["exported_return"]
    validation.to_csv(out_dir / "baseline_rebuild_validation.csv", index=False)
    make_report(summary, out_dir)

    print(f"wrote={out_dir}")
    print(summary.head(20).to_string(index=False))
    print(
        "baseline max abs rebuild diff",
        float(validation["diff"].abs().max()),
        "mean abs diff",
        float(validation["diff"].abs().mean()),
    )


if __name__ == "__main__":
    main()
