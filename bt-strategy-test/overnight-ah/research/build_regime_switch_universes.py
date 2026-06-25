#!/usr/bin/env python3
"""Build static/dynamic regime-switch monthly universes for OvernightAH."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
OUT_ROOT = ROOT / "bt-strategy-test" / "overnight-ah" / "research" / "out" / "edge_prediction_study_all_adj"
DEFAULT_DAILY = OUT_ROOT / "daily_panel.csv"
DEFAULT_DYNAMIC = OUT_ROOT / "monthly_universes_theme_weak" / "score_base85_theme_corr1215_top50_gate_spy_dd3m_gt_neg10.csv"
DEFAULT_STATIC = ROOT / "config-common" / "tickers" / "stable_ah_top10.json"
DEFAULT_OUT = OUT_ROOT / "monthly_universes_regime_switch"

SEMIS = [
    "NVDA",
    "AMD",
    "AVGO",
    "MU",
    "ASML",
    "MRVL",
    "ARM",
    "AMAT",
    "LRCX",
    "KLAC",
    "MCHP",
    "ADI",
    "TXN",
    "ON",
    "INTC",
    "GFS",
]


def load_static(path: Path) -> list[str]:
    symbols = json.loads(path.read_text())
    return [str(s).strip().upper() for s in symbols if str(s).strip().upper() != "SPY"]


def semis_monthly_features(daily_path: Path) -> pd.DataFrame:
    daily = pd.read_csv(daily_path, usecols=["date", "ticker", "c2c_ret", "close"], parse_dates=["date"])
    daily = daily[daily["ticker"].isin(SEMIS)].dropna(subset=["date"])
    factor = daily.groupby("date", as_index=False).agg(
        semis_c2c=("c2c_ret", "mean"),
        semis_close=("close", "mean"),
    )
    months = pd.date_range(
        factor["date"].min().to_period("M").to_timestamp(),
        factor["date"].max().to_period("M").to_timestamp(),
        freq="MS",
    )
    rows = []
    for month in months:
        hist = factor[factor["date"] < month].copy()
        row = {"month": month}
        for label, days in [("1m", 21), ("3m", 63), ("6m", 126), ("12m", 252)]:
            window = hist.tail(days)
            if len(window) >= max(10, days // 3):
                row[f"semis_total_{label}"] = float(window["semis_c2c"].sum())
                row[f"semis_mean_{label}"] = float(window["semis_c2c"].mean())
            else:
                row[f"semis_total_{label}"] = float("nan")
                row[f"semis_mean_{label}"] = float("nan")
        closes = hist["semis_close"].dropna()
        if len(closes) >= 63:
            row["semis_ma63_ratio"] = float(closes.iloc[-1] / closes.tail(63).mean() - 1.0)
        else:
            row["semis_ma63_ratio"] = float("nan")
        if len(closes) >= 126:
            row["semis_ma126_ratio"] = float(closes.iloc[-1] / closes.tail(126).mean() - 1.0)
        else:
            row["semis_ma126_ratio"] = float("nan")
        rows.append(row)
    return pd.DataFrame(rows)


def dynamic_map(path: Path) -> dict[tuple[int, int], list[str]]:
    df = pd.read_csv(path, sep=";")
    out = {}
    for row in df.itertuples(index=False):
        symbols = [s for s in str(row.symbols or "").split(",") if s]
        out[(int(row.year), int(row.month))] = symbols
    return out


def rule_pass(row: pd.Series, feature: str, threshold: float) -> bool:
    value = row.get(feature)
    return pd.notna(value) and float(value) > threshold


def write_universe(
    out_dir: Path,
    name: str,
    months: pd.DataFrame,
    dyn: dict[tuple[int, int], list[str]],
    static_symbols: list[str],
    feature: str,
    threshold: float,
    empty_dynamic_fallback: str,
) -> dict[str, object]:
    rows = []
    detail_rows = []
    for row in months.itertuples(index=False):
        month = pd.Timestamp(row.month)
        row_s = pd.Series(row._asdict())
        use_dynamic = rule_pass(row_s, feature, threshold)
        fallback_used = False
        if use_dynamic:
            symbols = dyn.get((month.year, month.month), [])
            if not symbols and empty_dynamic_fallback == "static":
                symbols = static_symbols
                fallback_used = True
        else:
            symbols = static_symbols
        rows.append({"year": month.year, "month": month.month, "symbols": ",".join(symbols)})
        detail_rows.append(
            {
                "month": month,
                "feature": feature,
                "threshold": threshold,
                "value": row_s.get(feature),
                "regime": "dynamic" if use_dynamic else "static",
                "fallback_used": fallback_used,
                "n": len(symbols),
            }
        )
    universe_path = out_dir / f"{name}.csv"
    detail_path = out_dir / f"{name}_detail.csv"
    pd.DataFrame(rows).to_csv(universe_path, sep=";", index=False)
    pd.DataFrame(detail_rows).to_csv(detail_path, index=False)
    return {
        "feature": name,
        "top_n": 50,
        "monthly_universe_file": str(universe_path.resolve().relative_to(ROOT)),
        "detail_file": str(detail_path.resolve().relative_to(ROOT)),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build regime-switch monthly universe files")
    parser.add_argument("--daily", type=Path, default=DEFAULT_DAILY)
    parser.add_argument("--dynamic", type=Path, default=DEFAULT_DYNAMIC)
    parser.add_argument("--static", type=Path, default=DEFAULT_STATIC)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--empty-dynamic-fallback",
        choices=["flat", "static"],
        default="flat",
        help="Policy when switch selects dynamic but the dynamic universe is empty.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    features = semis_monthly_features(args.daily)
    features.to_csv(args.out_dir / "semis_regime_features.csv", index=False)
    dyn = dynamic_map(args.dynamic)
    static_symbols = load_static(args.static)

    specs = []
    for feature, thresholds in {
        "semis_total_1m": [-0.02, 0.0, 0.02, 0.05],
        "semis_total_3m": [0.0, 0.05, 0.10, 0.15, 0.20],
        "semis_total_6m": [0.0, 0.10, 0.20, 0.30, 0.40],
        "semis_ma63_ratio": [-0.02, 0.0, 0.03, 0.06, 0.10],
        "semis_ma126_ratio": [-0.02, 0.0, 0.05, 0.10, 0.15],
    }.items():
        for threshold in thresholds:
            sign = "m" if threshold < 0 else "p"
            pct = int(round(abs(threshold) * 100))
            name = f"switch_{feature}_gt_{sign}{pct}"
            specs.append(
                write_universe(
                    args.out_dir,
                    name,
                    features,
                    dyn,
                    static_symbols,
                    feature,
                    threshold,
                    args.empty_dynamic_fallback,
                )
            )
    index = pd.DataFrame(specs)
    index.to_csv(args.out_dir / "index.csv", index=False)
    print(args.out_dir / "index.csv")
    print(index.to_string(index=False))


if __name__ == "__main__":
    main()
