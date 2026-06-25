#!/usr/bin/env python3
"""Build ex-ante ML/ensemble monthly universes for OvernightAH."""

from __future__ import annotations

import argparse
from pathlib import Path
import warnings

import numpy as np
import pandas as pd
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import HuberRegressor, Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import RobustScaler, StandardScaler


ROOT = Path(__file__).resolve().parents[3]
RESEARCH_OUT = ROOT / "bt-strategy-test" / "overnight-ah" / "research" / "out"
DEFAULT_PANEL = RESEARCH_OUT / "edge_prediction_study_all_adj" / "feature_target_panel.csv"
DEFAULT_OUT = RESEARCH_OUT / "edge_prediction_study_all_adj" / "monthly_universes_ml"


def feature_columns(panel: pd.DataFrame) -> list[str]:
    blocked = {"month", "ticker"}
    blocked.update(c for c in panel.columns if c.startswith("target_"))
    cols = []
    for col in panel.columns:
        if col in blocked or not pd.api.types.is_numeric_dtype(panel[col]):
            continue
        non_null = float(panel[col].notna().mean())
        if non_null < 0.05:
            continue
        cols.append(col)
    return cols


def monthly_rank(series: pd.Series) -> pd.Series:
    return series.rank(pct=True, method="average")


def add_baseline_scores(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    out["rank_c2c6"] = out.groupby("month")["c2c_mean_6m"].transform(monthly_rank)
    out["rank_ah6"] = out.groupby("month")["ah_mean_6m"].transform(monthly_rank)
    out["rank_c2c12"] = out.groupby("month")["c2c_mean_12m"].transform(monthly_rank)
    out["rank_ah12"] = out.groupby("month")["ah_mean_12m"].transform(monthly_rank)
    out["score_c2c60_ah40"] = 0.6 * out["rank_c2c6"] + 0.4 * out["rank_ah6"]
    out["score_c2c50_ah50"] = 0.5 * out["rank_c2c6"] + 0.5 * out["rank_ah6"]
    out["score_c2c6_12_ah6_12"] = 0.35 * out["rank_c2c6"] + 0.25 * out["rank_ah6"] + 0.25 * out["rank_c2c12"] + 0.15 * out["rank_ah12"]
    return out


def make_models(seed: int) -> dict[str, object]:
    return {
        "ridge": make_pipeline(SimpleImputer(strategy="median"), StandardScaler(), Ridge(alpha=20.0)),
        "huber": make_pipeline(SimpleImputer(strategy="median"), RobustScaler(), HuberRegressor(alpha=0.01, epsilon=1.35, max_iter=300)),
        "extratrees": make_pipeline(
            SimpleImputer(strategy="median"),
            ExtraTreesRegressor(
                n_estimators=64,
                max_depth=4,
                min_samples_leaf=24,
                max_features=0.55,
                random_state=seed + 1,
                n_jobs=1,
            ),
        ),
    }


def rolling_predictions(panel: pd.DataFrame, features: list[str], train_start: str, min_train_months: int, seed: int) -> pd.DataFrame:
    months = sorted(pd.Timestamp(m) for m in panel["month"].dropna().unique())
    start = pd.Timestamp(train_start)
    rows = []
    for month in months:
        if month < start:
            continue
        train_months = [m for m in months if m < month]
        if len(train_months) < min_train_months:
            continue
        train = panel[(panel["month"].isin(train_months)) & (panel["target_trades"] >= 2)].dropna(subset=["target_edge_mean_bps"])
        test = panel[panel["month"] == month].copy()
        if len(train) < 200 or test.empty:
            continue
        x_train = train[features]
        y_train = train["target_edge_mean_bps"].astype(float).clip(-250, 250)
        x_test = test[features]
        preds = {}
        for name, model in make_models(seed).items():
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                model.fit(x_train, y_train)
            preds[name] = model.predict(x_test)
        pred_df = test[["month", "ticker", "target_edge_mean_bps", "target_win_ratio", "target_trades"]].copy()
        for name, values in preds.items():
            pred_df[f"pred_{name}"] = values
            pred_df[f"rank_{name}"] = pd.Series(values, index=pred_df.index).rank(pct=True, method="average")
        pred_df["rank_ml_mean"] = pred_df[[f"rank_{name}" for name in preds]].mean(axis=1)
        rows.append(pred_df)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def write_universe(out_dir: Path, feature: str, top_n: int, scored: pd.DataFrame) -> dict[str, object]:
    rows = []
    details = []
    for month, group in scored.groupby("month"):
        g = group.dropna(subset=[feature]).sort_values([feature, "ticker"], ascending=[False, True]).head(top_n)
        symbols = g["ticker"].tolist()
        rows.append({"year": pd.Timestamp(month).year, "month": pd.Timestamp(month).month, "symbols": ",".join(symbols)})
        for rank, row in enumerate(g.itertuples(index=False), start=1):
            details.append({"month": pd.Timestamp(month), "rank": rank, "ticker": row.ticker, "feature": feature, "score": getattr(row, feature)})
    base = f"{feature}_top{top_n}"
    universe_path = out_dir / f"{base}.csv"
    detail_path = out_dir / f"{base}_detail.csv"
    pd.DataFrame(rows).to_csv(universe_path, sep=";", index=False)
    pd.DataFrame(details).to_csv(detail_path, index=False)
    return {
        "feature": feature,
        "top_n": top_n,
        "monthly_universe_file": str(universe_path.resolve().relative_to(ROOT)),
        "detail_file": str(detail_path.resolve().relative_to(ROOT)),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build rolling ML monthly universes for OvernightAH")
    parser.add_argument("--panel", type=Path, default=DEFAULT_PANEL)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--train-start", default="2019-01-01")
    parser.add_argument("--min-train-months", type=int, default=36)
    parser.add_argument("--top-n", type=int, nargs="+", default=[20, 30, 40, 50, 60])
    parser.add_argument("--seed", type=int, default=7)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    panel = pd.read_csv(args.panel, parse_dates=["month"])
    panel = add_baseline_scores(panel)
    features = feature_columns(panel)
    preds = rolling_predictions(panel, features, args.train_start, args.min_train_months, args.seed)
    if preds.empty:
        raise SystemExit("no predictions generated")
    scored = panel.merge(preds, on=["month", "ticker", "target_edge_mean_bps", "target_win_ratio", "target_trades"], how="inner")
    scored["score_ml_mean"] = scored["rank_ml_mean"]
    scored["score_ml60_c2c40"] = 0.6 * scored["rank_ml_mean"] + 0.4 * scored["score_c2c60_ah40"]
    scored["score_ml50_c2c50"] = 0.5 * scored["rank_ml_mean"] + 0.5 * scored["score_c2c60_ah40"]
    scored["score_ml40_c2c60"] = 0.4 * scored["rank_ml_mean"] + 0.6 * scored["score_c2c60_ah40"]
    scored["score_ml50_consensus50"] = 0.5 * scored["rank_ml_mean"] + 0.5 * scored["score_c2c6_12_ah6_12"]
    scored.to_csv(args.out_dir / "rolling_ml_scores.csv", index=False)

    universe_index = []
    score_cols = [
        "score_ml_mean",
        "score_ml60_c2c40",
        "score_ml50_c2c50",
        "score_ml40_c2c60",
        "score_ml50_consensus50",
    ]
    score_cols.extend([f"rank_{m}" for m in ["ridge", "huber", "extratrees"]])
    for score_col in score_cols:
        for top_n in args.top_n:
            universe_index.append(write_universe(args.out_dir, score_col, top_n, scored))
    index = pd.DataFrame(universe_index)
    index.to_csv(args.out_dir / "index.csv", index=False)
    print(args.out_dir / "index.csv")
    print(index.head(30).to_string(index=False))


if __name__ == "__main__":
    main()
