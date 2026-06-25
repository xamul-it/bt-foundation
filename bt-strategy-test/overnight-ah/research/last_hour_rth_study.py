#!/usr/bin/env python3
"""Study last-hour RTH features vs OvernightAH next-open returns.

The study reconstructs the OvernightAH daily candidate set from Alpaca minute
bars, then measures whether 15:00-16:00 ET price/volume behavior separates
good and bad overnight trades. It is intentionally local and research-only.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent.parent
NY_TZ = "America/New_York"
DEFAULT_TICKERS = ROOT / "config-common" / "tickers" / "stable_ah_top10.json"
DEFAULT_DATA = ROOT / "config-common" / "data" / "m" / "alpaca" / "sip"
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "last_hour_rth"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", type=Path, default=DEFAULT_TICKERS)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--fromdate", default="2026-01-01")
    parser.add_argument("--todate", default="2026-06-02")
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument("--min-intraday-vol", type=float, default=0.025)
    parser.add_argument("--max-intraday-vol", type=float, default=0.045)
    parser.add_argument("--ah-lag1-threshold", type=float, default=-0.1)
    parser.add_argument("--min-adv", type=float, default=100_000_000)
    parser.add_argument("--adv-lookback", type=int, default=20)
    parser.add_argument("--bad-quantile", type=float, default=0.25)
    return parser.parse_args()


def load_tickers(path: Path) -> list[str]:
    with path.open() as handle:
        values = json.load(handle)
    return [
        str(value).strip().upper()
        for value in values
        if str(value).strip() and str(value).strip().upper() != "SPY"
    ]


def _first(series: pd.Series) -> float:
    return float(series.iloc[0])


def _last(series: pd.Series) -> float:
    return float(series.iloc[-1])


def load_symbol_minutes(symbol: str, path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.read_csv(path)
    if raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True, errors="coerce")
    raw = raw.dropna(subset=["timestamp"]).sort_values("timestamp")
    raw["ny_timestamp"] = raw["timestamp"].dt.tz_convert(NY_TZ)
    raw["date"] = raw["ny_timestamp"].dt.tz_localize(None).dt.normalize()
    raw["time"] = raw["ny_timestamp"].dt.strftime("%H:%M")

    for col in ["open", "high", "low", "close", "volume"]:
        raw[col] = pd.to_numeric(raw[col], errors="coerce")
    raw = raw.dropna(subset=["open", "high", "low", "close", "volume"])

    rth = raw[(raw["time"] >= "09:30") & (raw["time"] < "16:00")].copy()
    if rth.empty:
        return pd.DataFrame(), pd.DataFrame()

    daily = rth.groupby("date").agg(
        open=("open", _first),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", _last),
        volume=("volume", "sum"),
        n_minutes=("close", "count"),
    ).reset_index()
    daily = daily[daily["n_minutes"] >= 300].copy()
    daily.insert(0, "ticker", symbol)
    daily["rth_vol"] = (daily["high"] - daily["low"]) / daily["open"]
    daily["dollar_volume"] = daily["volume"] * daily["close"]
    daily["adv"] = (
        daily["volume"].rolling(20, min_periods=10).mean().shift(1) * daily["close"]
    )
    daily["ah_lag1"] = daily["open"] / daily["close"].shift(1) - 1.0
    daily["next_open"] = daily["open"].shift(-1)
    daily["ah_ret"] = daily["next_open"] / daily["close"] - 1.0

    last_hour = rth[(rth["time"] >= "15:00") & (rth["time"] < "16:00")].copy()
    first_15 = last_hour.groupby("date").agg(
        lh_open=("open", _first),
        lh_high=("high", "max"),
        lh_low=("low", "min"),
        lh_close=("close", _last),
        lh_volume=("volume", "sum"),
        lh_vwap=("vwap", "last"),
        lh_minutes=("close", "count"),
    ).reset_index()
    first_15 = first_15[first_15["lh_minutes"] >= 45].copy()

    def window_return(start_time: str, name: str) -> pd.DataFrame:
        win = rth[(rth["time"] >= start_time) & (rth["time"] < "16:00")]
        return win.groupby("date").agg(
            **{
                f"{name}_open": ("open", _first),
                f"{name}_close": ("close", _last),
                f"{name}_volume": ("volume", "sum"),
            }
        ).reset_index()

    feats = first_15
    for start_time, name in [("15:30", "m30"), ("15:45", "m15"), ("15:55", "m5")]:
        feats = feats.merge(window_return(start_time, name), on="date", how="left")

    feats.insert(0, "ticker", symbol)
    feats["lh_ret"] = feats["lh_close"] / feats["lh_open"] - 1.0
    feats["lh_range"] = (feats["lh_high"] - feats["lh_low"]) / feats["lh_open"]
    denom = (feats["lh_high"] - feats["lh_low"]).replace(0, np.nan)
    feats["lh_close_pos"] = (feats["lh_close"] - feats["lh_low"]) / denom
    for name in ["m30", "m15", "m5"]:
        feats[f"{name}_ret"] = feats[f"{name}_close"] / feats[f"{name}_open"] - 1.0

    return daily, feats


def add_rolling_context(panel: pd.DataFrame, adv_lookback: int) -> pd.DataFrame:
    panel = panel.sort_values(["ticker", "date"]).copy()
    daily_range = (panel["high"] - panel["low"]).replace(0, np.nan)
    panel["rth_ret"] = panel["close"] / panel["open"] - 1.0
    panel["up_from_open"] = panel["high"] / panel["open"] - 1.0
    panel["down_from_open"] = panel["low"] / panel["open"] - 1.0
    panel["close_pos"] = (panel["close"] - panel["low"]) / daily_range
    panel["body_to_range"] = (panel["close"] - panel["open"]) / daily_range
    panel["abs_body_to_range"] = (panel["close"] - panel["open"]).abs() / daily_range
    panel["upper_wick_to_range"] = (panel["high"] - panel[["open", "close"]].max(axis=1)) / daily_range
    panel["lower_wick_to_range"] = (panel[["open", "close"]].min(axis=1) - panel["low"]) / daily_range
    panel["lh_volume_share"] = panel["lh_volume"] / panel["volume"].replace(0, np.nan)

    grouped = panel.groupby("ticker", group_keys=False)
    for col in [
        "rth_ret",
        "up_from_open",
        "down_from_open",
        "close_pos",
        "body_to_range",
        "abs_body_to_range",
        "upper_wick_to_range",
        "lower_wick_to_range",
        "lh_ret",
        "lh_range",
        "lh_volume_share",
        "m30_ret",
        "m15_ret",
        "m5_ret",
    ]:
        roll_mean = grouped[col].transform(
            lambda s: s.rolling(adv_lookback, min_periods=10).mean().shift(1)
        )
        roll_std = grouped[col].transform(
            lambda s: s.rolling(adv_lookback, min_periods=10).std().shift(1)
        )
        panel[f"{col}_z20"] = (panel[col] - roll_mean) / roll_std.replace(0, np.nan)
    return panel


def build_selected(panel: pd.DataFrame, tickers: list[str], args: argparse.Namespace) -> pd.DataFrame:
    order = {ticker: i for i, ticker in enumerate(tickers)}
    candidates = panel[
        (panel["rth_vol"] >= args.min_intraday_vol)
        & (panel["rth_vol"] <= args.max_intraday_vol)
        & (panel["adv"] >= args.min_adv)
        & (panel["ah_lag1"] >= args.ah_lag1_threshold)
    ].copy()
    candidates = candidates.dropna(subset=["ah_ret", "lh_ret", "lh_volume_share"])
    candidates["order"] = candidates["ticker"].map(order)
    selected = (
        candidates.sort_values(["date", "order"])
        .groupby("date", group_keys=False)
        .head(args.max_concurrent)
        .copy()
    )
    selected["ret_bps"] = selected["ah_ret"] * 10_000
    selected["portfolio_ret"] = selected.groupby("date")["ah_ret"].transform("mean")
    return selected


def simulate_selection(candidates: pd.DataFrame, extra_mask: pd.Series | None = None) -> dict:
    data = candidates if extra_mask is None else candidates[extra_mask].copy()
    selected = (
        data.sort_values(["date", "order"])
        .groupby("date", group_keys=False)
        .head(5)
        .copy()
    )
    daily = selected.groupby("date")["ah_ret"].mean()
    return {
        "trades": int(len(selected)),
        "days": int(daily.size),
        "mean_trade_bps": float(selected["ah_ret"].mean() * 10_000),
        "mean_day_bps": float(daily.mean() * 10_000),
        "median_trade_bps": float(selected["ah_ret"].median() * 10_000),
        "win_trade_pct": float((selected["ah_ret"] > 0).mean() * 100),
        "total_pct": float(((1 + daily).prod() - 1) * 100),
    }


def preselection_filter_sweep(panel: pd.DataFrame, tickers: list[str], args: argparse.Namespace) -> pd.DataFrame:
    order = {ticker: i for i, ticker in enumerate(tickers)}
    candidates = panel[
        (panel["rth_vol"] >= args.min_intraday_vol)
        & (panel["rth_vol"] <= args.max_intraday_vol)
        & (panel["adv"] >= args.min_adv)
        & (panel["ah_lag1"] >= args.ah_lag1_threshold)
    ].copy()
    candidates = candidates.dropna(subset=["ah_ret", "lh_ret", "lh_volume_share"])
    candidates["order"] = candidates["ticker"].map(order)

    rows = [{"rule": "base", **simulate_selection(candidates)}]
    sweep_defs = [
        ("close_pos", [0.20, 0.25, 0.33, 0.50, 0.67, 0.75], ["<=", ">="]),
        ("up_from_open_z20", [0.20, 0.25, 0.33, 0.50, 0.67, 0.75], ["<=", ">="]),
        ("body_to_range", [0.20, 0.25, 0.33, 0.50, 0.67, 0.75], ["<=", ">="]),
        ("upper_wick_to_range", [0.20, 0.25, 0.33, 0.50, 0.67, 0.75], ["<=", ">="]),
        ("m5_ret", [0.20, 0.25, 0.33, 0.50], [">="]),
        ("m5_ret_z20", [0.20, 0.25, 0.33, 0.50], [">="]),
        ("lh_range_z20", [0.25, 0.33, 0.50, 0.67, 0.75], [">="]),
        ("lh_ret_z20", [0.25, 0.33, 0.67, 0.75], ["<=", ">="]),
        ("lh_volume_share_z20", [0.50, 0.67, 0.75], [">="]),
    ]
    for feature, quantiles, ops in sweep_defs:
        for quantile in quantiles:
            threshold = candidates[feature].quantile(quantile)
            for op in ops:
                if op == ">=":
                    mask = candidates[feature] >= threshold
                else:
                    mask = candidates[feature] <= threshold
                rows.append(
                    {
                        "rule": f"{feature} {op} q{quantile:.2f} ({threshold:.6g})",
                        **simulate_selection(candidates, mask),
                    }
                )
    out = pd.DataFrame(rows)
    base_mean = out.loc[out["rule"] == "base", "mean_day_bps"].iloc[0]
    out["delta_day_bps"] = out["mean_day_bps"] - base_mean
    return out.sort_values("delta_day_bps", ascending=False)


def loss_cut_sweep(selected: pd.DataFrame) -> pd.DataFrame:
    """Evaluate simple exclusion rules by profit retained vs loss avoided."""
    base_profit = selected.loc[selected["ah_ret"] > 0, "ah_ret"].sum()
    base_loss = -selected.loc[selected["ah_ret"] < 0, "ah_ret"].sum()
    base_total = selected["ah_ret"].sum()
    base_winners = int((selected["ah_ret"] > 0).sum())
    base_losers = int((selected["ah_ret"] < 0).sum())

    rows = []
    feature_defs = [
        ("lh_range_z20", [0.20, 0.25, 0.33, 0.50, 0.67, 0.75], [">=", "<="]),
        ("lh_ret_z20", [0.20, 0.25, 0.33, 0.50, 0.67, 0.75], [">=", "<="]),
        ("m5_ret_z20", [0.20, 0.25, 0.33, 0.50, 0.67, 0.75], [">=", "<="]),
        ("up_from_open_z20", [0.20, 0.25, 0.33, 0.50, 0.67, 0.75], [">=", "<="]),
        ("body_to_range", [0.20, 0.25, 0.33, 0.50, 0.67, 0.75], [">=", "<="]),
        ("upper_wick_to_range", [0.20, 0.25, 0.33, 0.50, 0.67, 0.75], [">=", "<="]),
        ("close_pos", [0.20, 0.25, 0.33, 0.50, 0.67, 0.75], [">=", "<="]),
    ]
    for feature, quantiles, ops in feature_defs:
        data = selected.dropna(subset=[feature, "ah_ret"])
        if data.empty:
            continue
        for quantile in quantiles:
            threshold = data[feature].quantile(quantile)
            for op in ops:
                if op == ">=":
                    keep = data[data[feature] >= threshold]
                    drop = data[data[feature] < threshold]
                    rule = f"keep {feature} >= q{quantile:.2f} ({threshold:.6g})"
                else:
                    keep = data[data[feature] <= threshold]
                    drop = data[data[feature] > threshold]
                    rule = f"keep {feature} <= q{quantile:.2f} ({threshold:.6g})"
                if len(keep) < 40 or drop.empty:
                    continue

                kept_profit = keep.loc[keep["ah_ret"] > 0, "ah_ret"].sum()
                kept_loss = -keep.loc[keep["ah_ret"] < 0, "ah_ret"].sum()
                lost_profit = drop.loc[drop["ah_ret"] > 0, "ah_ret"].sum()
                avoided_loss = -drop.loc[drop["ah_ret"] < 0, "ah_ret"].sum()
                dropped_winners = int((drop["ah_ret"] > 0).sum())
                dropped_losers = int((drop["ah_ret"] < 0).sum())
                rows.append(
                    {
                        "rule": rule,
                        "keep_trades": int(len(keep)),
                        "drop_trades": int(len(drop)),
                        "keep_pct": float(len(keep) / len(data) * 100),
                        "mean_bps": float(keep["ah_ret"].mean() * 10_000),
                        "median_bps": float(keep["ah_ret"].median() * 10_000),
                        "win_rate_pct": float((keep["ah_ret"] > 0).mean() * 100),
                        "worst_bps": float(keep["ah_ret"].min() * 10_000),
                        "p10_bps": float(keep["ah_ret"].quantile(0.10) * 10_000),
                        "profit_retained_pct": float(kept_profit / base_profit * 100) if base_profit else np.nan,
                        "loss_remaining_pct": float(kept_loss / base_loss * 100) if base_loss else np.nan,
                        "avoided_loss_bps_sum": float(avoided_loss * 10_000),
                        "lost_profit_bps_sum": float(lost_profit * 10_000),
                        "avoided_loss_minus_lost_profit_bps": float((avoided_loss - lost_profit) * 10_000),
                        "dropped_losers_pct": float(dropped_losers / base_losers * 100) if base_losers else np.nan,
                        "dropped_winners_pct": float(dropped_winners / base_winners * 100) if base_winners else np.nan,
                        "total_delta_bps_sum": float((keep["ah_ret"].sum() - base_total) * 10_000),
                    }
                )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(
        ["avoided_loss_minus_lost_profit_bps", "profit_retained_pct"],
        ascending=[False, False],
    )


def summarize_bins(selected: pd.DataFrame, feature: str) -> pd.DataFrame:
    data = selected.dropna(subset=[feature, "ah_ret"]).copy()
    if data[feature].nunique() < 4:
        return pd.DataFrame()
    data["bin"] = pd.qcut(data[feature], 4, labels=False, duplicates="drop") + 1
    return data.groupby("bin").agg(
        feature_min=(feature, "min"),
        feature_max=(feature, "max"),
        trades=("ah_ret", "count"),
        mean_bps=("ret_bps", "mean"),
        median_bps=("ret_bps", "median"),
        win_rate=("ah_ret", lambda s: (s > 0).mean()),
    ).reset_index()


def feature_screen(selected: pd.DataFrame, bad_quantile: float) -> pd.DataFrame:
    threshold = selected["ah_ret"].quantile(bad_quantile)
    data = selected.copy()
    data["bad"] = data["ah_ret"] <= threshold
    features = [
        "rth_ret",
        "rth_ret_z20",
        "up_from_open",
        "up_from_open_z20",
        "down_from_open",
        "down_from_open_z20",
        "close_pos",
        "close_pos_z20",
        "body_to_range",
        "body_to_range_z20",
        "abs_body_to_range",
        "abs_body_to_range_z20",
        "upper_wick_to_range",
        "upper_wick_to_range_z20",
        "lower_wick_to_range",
        "lower_wick_to_range_z20",
        "lh_ret",
        "lh_ret_z20",
        "lh_range",
        "lh_range_z20",
        "lh_close_pos",
        "lh_volume_share",
        "lh_volume_share_z20",
        "m30_ret",
        "m30_ret_z20",
        "m15_ret",
        "m15_ret_z20",
        "m5_ret",
        "m5_ret_z20",
        "rth_vol",
        "ah_lag1",
    ]
    rows = []
    for feature in features:
        tmp = data.dropna(subset=[feature, "ah_ret", "bad"])
        if len(tmp) < 20 or tmp[feature].nunique() < 4:
            continue
        corr = tmp[feature].corr(tmp["ah_ret"], method="spearman")
        good = tmp.loc[~tmp["bad"], feature]
        bad = tmp.loc[tmp["bad"], feature]
        rows.append(
            {
                "feature": feature,
                "n": len(tmp),
                "spearman": corr,
                "bad_mean": bad.mean(),
                "good_mean": good.mean(),
                "bad_median": bad.median(),
                "good_median": good.median(),
                "mean_diff_bad_minus_good": bad.mean() - good.mean(),
            }
        )
    return pd.DataFrame(rows).sort_values("spearman", key=lambda s: s.abs(), ascending=False)


def write_report(
    path: Path,
    tickers: list[str],
    missing: list[str],
    selected: pd.DataFrame,
    screen: pd.DataFrame,
    sweep: pd.DataFrame,
    loss_sweep: pd.DataFrame,
    bin_tables: dict[str, pd.DataFrame],
) -> None:
    lines = ["# OvernightAH last-hour RTH study", ""]
    lines.append(f"Ticker file: {', '.join(tickers)}")
    lines.append(f"Ticker minute mancanti/non usabili: {', '.join(missing) if missing else 'nessuno'}")
    lines.append(
        f"Periodo trade selezionati: {selected['date'].min().date()} - {selected['date'].max().date()}"
    )
    lines.append(
        f"Trade selezionati: {len(selected)}; giorni: {selected['date'].nunique()}; "
        f"ticker: {selected['ticker'].nunique()}"
    )
    lines.append(
        f"Rendimento medio overnight: {selected['ret_bps'].mean():.2f} bps; "
        f"mediana: {selected['ret_bps'].median():.2f} bps; "
        f"win rate: {(selected['ah_ret'] > 0).mean() * 100:.1f}%"
    )
    lines.append("")
    lines.append("## Feature screen")
    lines.append("")
    if screen.empty:
        lines.append("Nessuna feature con campione sufficiente.")
    else:
        show = screen.head(12).copy()
        for col in show.select_dtypes("number").columns:
            show[col] = show[col].round(6)
        lines.append(show.to_markdown(index=False))
    lines.append("")
    lines.append("## Pre-selection filter sweep")
    lines.append("")
    if sweep.empty:
        lines.append("Nessuna simulazione disponibile.")
    else:
        show = sweep.head(12).copy()
        for col in show.select_dtypes("number").columns:
            show[col] = show[col].round(4)
        lines.append(show.to_markdown(index=False))
    lines.append("")
    lines.append("## Loss-cut sweep")
    lines.append("")
    if loss_sweep.empty:
        lines.append("Nessuna simulazione disponibile.")
    else:
        show = loss_sweep.head(12).copy()
        for col in show.select_dtypes("number").columns:
            show[col] = show[col].round(4)
        lines.append(show.to_markdown(index=False))
    for feature, table in bin_tables.items():
        lines.append("")
        lines.append(f"## Quartili {feature}")
        lines.append("")
        if table.empty:
            lines.append("Campione insufficiente.")
        else:
            show = table.copy()
            for col in show.select_dtypes("number").columns:
                show[col] = show[col].round(6)
            show["win_rate"] = (show["win_rate"] * 100).round(2)
            lines.append(show.to_markdown(index=False))
    lines.append("")
    lines.append(
        "Note: il close usato e' l'ultimo minuto RTH prima delle 16:00 ET; "
        "l'open exit e' la barra 09:30 ET del giorno successivo. "
        "I risultati sono su ticker con minute SIP locale disponibile."
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    tickers = load_tickers(args.tickers)
    frames = []
    missing = []
    for ticker in tickers:
        path = args.data_dir / f"{ticker}.csv"
        if not path.exists():
            missing.append(ticker)
            continue
        daily, feats = load_symbol_minutes(ticker, path)
        if daily.empty or feats.empty:
            missing.append(ticker)
            continue
        frames.append(daily.merge(feats, on=["ticker", "date"], how="inner"))

    if not frames:
        raise SystemExit("No usable minute data")

    panel = pd.concat(frames, ignore_index=True)
    panel = add_rolling_context(panel, args.adv_lookback)
    fromdate = pd.Timestamp(args.fromdate)
    todate = pd.Timestamp(args.todate)
    panel = panel[(panel["date"] >= fromdate) & (panel["date"] <= todate)].copy()

    selected = build_selected(panel, tickers, args)
    if selected.empty:
        raise SystemExit("No selected trades")

    screen = feature_screen(selected, args.bad_quantile)
    sweep = preselection_filter_sweep(panel, tickers, args)
    loss_sweep = loss_cut_sweep(selected)
    bin_features = [
        "close_pos",
        "up_from_open_z20",
        "body_to_range",
        "upper_wick_to_range",
        "lh_ret",
        "lh_ret_z20",
        "lh_volume_share_z20",
        "lh_range_z20",
        "m15_ret_z20",
    ]
    bin_tables = {feature: summarize_bins(selected, feature) for feature in bin_features}

    panel.to_csv(args.out_dir / "panel_all_symbol_days.csv", index=False)
    selected.to_csv(args.out_dir / "selected_trades.csv", index=False)
    daily = selected.groupby("date").agg(
        trades=("ah_ret", "count"),
        portfolio_ret=("ah_ret", "mean"),
        mean_lh_ret=("lh_ret", "mean"),
        mean_lh_ret_z20=("lh_ret_z20", "mean"),
        mean_lh_volume_share_z20=("lh_volume_share_z20", "mean"),
        tickers=("ticker", lambda s: ",".join(s)),
    ).reset_index()
    daily["portfolio_bps"] = daily["portfolio_ret"] * 10_000
    daily.to_csv(args.out_dir / "portfolio_daily.csv", index=False)
    screen.to_csv(args.out_dir / "feature_screen.csv", index=False)
    sweep.to_csv(args.out_dir / "preselection_filter_sweep.csv", index=False)
    loss_sweep.to_csv(args.out_dir / "loss_cut_sweep.csv", index=False)
    for feature, table in bin_tables.items():
        table.to_csv(args.out_dir / f"bins_{feature}.csv", index=False)

    write_report(args.out_dir / "report.md", tickers, missing, selected, screen, sweep, loss_sweep, bin_tables)
    print(f"Wrote {args.out_dir}")
    print(f"Selected trades: {len(selected)} across {selected['date'].nunique()} days")
    print(screen.head(10).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
