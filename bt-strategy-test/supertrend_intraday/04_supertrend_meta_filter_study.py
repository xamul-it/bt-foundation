#!/usr/bin/env python3
"""
Meta-filter study for SuperTrend intraday entries using RTH daily features.

Builds an event-level dataset:
  SuperTrend trade outcome + latest RTH feature row available for that symbol/date

Then evaluates:
  - univariate quantile rules
  - simple pairwise quantile rules
  - sklearn tabular models used as rankers

Validation protocol is intentionally time-based:
  train:      2025-01-02 .. 2025-09-30
  validation: 2025-10-01 .. 2025-12-31
  OOS:        2026-01-01 .. available end
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    ExtraTreesClassifier,
    ExtraTreesRegressor,
    HistGradientBoostingClassifier,
    HistGradientBoostingRegressor,
    RandomForestClassifier,
    RandomForestRegressor,
)
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


RAW_EXCLUDE = {
    "open",
    "high",
    "low",
    "close",
    "volume",
    "feature_date",
    "trading_date",
    "symbol",
}


def summarize(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "n": 0,
            "edge_bps": None,
            "win_rate": None,
            "avg_win_bps": None,
            "avg_loss_bps": None,
            "sum_bps": 0.0,
        }
    pnl = pd.to_numeric(df["pnl_bps"], errors="coerce").dropna()
    if pnl.empty:
        return {"n": 0, "edge_bps": None, "win_rate": None, "avg_win_bps": None, "avg_loss_bps": None, "sum_bps": 0.0}
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


def add_event_fields(trades: pd.DataFrame) -> pd.DataFrame:
    out = trades.copy()
    out["entry_dt"] = pd.to_datetime(out["open_datetime"])
    out["signal_dt"] = pd.to_datetime(out.get("entry_signal_dt", out["open_datetime"]))
    out["trading_date"] = out["entry_dt"].dt.normalize()
    out["symbol"] = out["asset"].astype(str)
    out["pnl_pct"] = pd.to_numeric(out["pnl_pct"], errors="coerce")
    out["pnl_bps"] = out["pnl_pct"] * 100.0
    out["win"] = (out["pnl_bps"] > 0).astype(int)
    out["good30"] = (out["pnl_bps"] >= 30.0).astype(int)
    out["entry_minute"] = out["entry_dt"].dt.hour * 60 + out["entry_dt"].dt.minute
    out["entry_hour"] = out["entry_dt"].dt.hour
    out["duration_bars"] = pd.to_numeric(out.get("duration_bars"), errors="coerce")
    return out


RANK_COLS = [
    "rth_mom_20",
    "rth_eff_20",
    "rth_signed_eff_20",
    "rth_range_exp_5_20",
    "rth_rvol_5_20",
    "rth_mom_norm_20",
]


def load_feature_map(path: Path) -> pd.DataFrame:
    feat = pd.read_parquet(path)
    feat["feature_date"] = pd.to_datetime(feat["feature_date"]).dt.normalize()
    feat = feat.sort_values(["symbol", "feature_date"]).reset_index(drop=True)

    # Recompute anti-lookahead trading_date from the daily feature stream itself.
    # The existing universe_map can be wrong if the Alpaca calendar cache is
    # incomplete: old feature rows collapse onto the first cached market day.
    feat["trading_date"] = feat.groupby("symbol")["feature_date"].shift(-1)
    feat = feat.dropna(subset=["trading_date"]).copy()
    feat["trading_date"] = pd.to_datetime(feat["trading_date"]).dt.normalize()

    for col in RANK_COLS:
        if col in feat.columns:
            feat[f"{col}_rank_pct"] = feat.groupby("trading_date")[col].rank(
                pct=True,
                na_option="keep",
            )
    return feat


def build_dataset(trades_path: Path, features_path: Path, out_path: Path) -> pd.DataFrame:
    trades = add_event_fields(pd.read_csv(trades_path))
    features = load_feature_map(features_path)
    merged = trades.merge(features, on=["symbol", "trading_date"], how="inner", suffixes=("", "_rth"))
    merged = merged.sort_values(["entry_dt", "symbol"]).reset_index(drop=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(out_path, index=False)
    return merged


def split_frames(df: pd.DataFrame):
    d = df.copy()
    td = pd.to_datetime(d["trading_date"])
    train = d[(td >= "2024-01-02") & (td <= "2025-09-30")].copy()
    valid = d[(td >= "2025-10-01") & (td <= "2025-12-31")].copy()
    oos = d[(td >= "2026-01-01")].copy()
    return train, valid, oos


def feature_columns(df: pd.DataFrame) -> list[str]:
    numeric = [
        c
        for c in df.columns
        if c not in RAW_EXCLUDE
        and c not in {
            "asset", "open_datetime", "close_datetime", "entry_side", "entry_signal_dt",
            "exit_signal_dt", "entry_dt", "signal_dt", "pnl", "pnl_pct", "pnl_bps",
            "pnlcomm", "win", "good30", "size", "price", "value",
            "duration_bars", "entry_delay_bars", "exit_delay_bars",
        }
        and pd.api.types.is_numeric_dtype(df[c])
    ]
    return numeric


def quantile_edges(train: pd.DataFrame, valid: pd.DataFrame, oos: pd.DataFrame, cols: list[str], min_valid: int) -> list[dict]:
    rows = []
    for col in cols:
        s = train[col].replace([np.inf, -np.inf], np.nan).dropna()
        if s.nunique() < 5:
            continue
        try:
            _, bins = pd.qcut(s, q=10, retbins=True, duplicates="drop")
        except Exception:
            continue
        if len(bins) < 4:
            continue
        for side in ("top", "bottom"):
            for q in (0.1, 0.2, 0.3, 0.4, 0.5):
                cut = np.nanquantile(s, 1.0 - q if side == "top" else q)
                if side == "top":
                    masks = {
                        "train": train[col] >= cut,
                        "valid": valid[col] >= cut,
                        "oos": oos[col] >= cut,
                    }
                else:
                    masks = {
                        "train": train[col] <= cut,
                        "valid": valid[col] <= cut,
                        "oos": oos[col] <= cut,
                    }
                st = {name: summarize(frame[masks[name]]) for name, frame in (("train", train), ("valid", valid), ("oos", oos))}
                if st["valid"]["n"] < min_valid:
                    continue
                rows.append({
                    "kind": "univariate",
                    "rule": f"{col} {side} {q:.0%}",
                    "feature": col,
                    "side": side,
                    "share": q,
                    "cut": round(float(cut), 8),
                    **{f"{k}_{m}": v for k, stats in st.items() for m, v in stats.items()},
                })
    return rows


def pairwise_edges(train: pd.DataFrame, valid: pd.DataFrame, oos: pd.DataFrame, cols: list[str], min_valid: int) -> list[dict]:
    # Limit pair search to features with best validation univariate behavior.
    uni = quantile_edges(train, valid, oos, cols, min_valid=max(30, min_valid // 2))
    top_features = []
    for row in sorted(uni, key=lambda r: (r.get("valid_edge_bps") or -999, r.get("valid_n") or 0), reverse=True):
        if row["feature"] not in top_features:
            top_features.append(row["feature"])
        if len(top_features) >= 18:
            break
    rows = []
    for i, a in enumerate(top_features):
        for b in top_features[i + 1:]:
            for qa, qb in ((0.2, 0.2), (0.3, 0.3), (0.2, 0.4), (0.4, 0.2)):
                sa = train[a].replace([np.inf, -np.inf], np.nan).dropna()
                sb = train[b].replace([np.inf, -np.inf], np.nan).dropna()
                if sa.nunique() < 5 or sb.nunique() < 5:
                    continue
                ca = float(np.nanquantile(sa, 1.0 - qa))
                cb = float(np.nanquantile(sb, 1.0 - qb))
                masks = {
                    "train": (train[a] >= ca) & (train[b] >= cb),
                    "valid": (valid[a] >= ca) & (valid[b] >= cb),
                    "oos": (oos[a] >= ca) & (oos[b] >= cb),
                }
                st = {name: summarize(frame[masks[name]]) for name, frame in (("train", train), ("valid", valid), ("oos", oos))}
                if st["valid"]["n"] < min_valid:
                    continue
                rows.append({
                    "kind": "pair_top_top",
                    "rule": f"{a} top {qa:.0%} AND {b} top {qb:.0%}",
                    "feature_a": a,
                    "feature_b": b,
                    "cut_a": round(ca, 8),
                    "cut_b": round(cb, 8),
                    **{f"{k}_{m}": v for k, stats in st.items() for m, v in stats.items()},
                })
    return rows


def make_preprocessor(numeric_cols: list[str]):
    cat_cols = ["symbol", "entry_hour"]
    return ColumnTransformer(
        transformers=[
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median"))]), numeric_cols),
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
        ],
        remainder="drop",
    )


def model_rankers(train: pd.DataFrame, valid: pd.DataFrame, oos: pd.DataFrame, cols: list[str], min_valid: int) -> list[dict]:
    clf_models = {
        "logreg_win": Pipeline([
            ("prep", ColumnTransformer([
                ("num", Pipeline([("imputer", SimpleImputer(strategy="median")), ("scale", StandardScaler())]), cols),
                ("cat", OneHotEncoder(handle_unknown="ignore"), ["symbol", "entry_hour"]),
            ])),
            ("model", LogisticRegression(max_iter=2000, class_weight="balanced", C=0.5)),
        ]),
        "rf_win": Pipeline([
            ("prep", make_preprocessor(cols)),
            ("model", RandomForestClassifier(
                n_estimators=350, max_depth=6, min_samples_leaf=80,
                class_weight="balanced_subsample", n_jobs=-1, random_state=7,
            )),
        ]),
        "extra_win": Pipeline([
            ("prep", make_preprocessor(cols)),
            ("model", ExtraTreesClassifier(
                n_estimators=500, max_depth=7, min_samples_leaf=60,
                class_weight="balanced", n_jobs=-1, random_state=11,
            )),
        ]),
        "hgb_win": Pipeline([
            ("prep", make_preprocessor(cols)),
            ("model", HistGradientBoostingClassifier(
                max_iter=180, max_leaf_nodes=15, learning_rate=0.035,
                l2_regularization=1.0, min_samples_leaf=80, random_state=13,
            )),
        ]),
        "hgb_good30": Pipeline([
            ("prep", make_preprocessor(cols)),
            ("model", HistGradientBoostingClassifier(
                max_iter=200, max_leaf_nodes=15, learning_rate=0.035,
                l2_regularization=1.5, min_samples_leaf=90, random_state=17,
            )),
        ]),
        "extra_good30": Pipeline([
            ("prep", make_preprocessor(cols)),
            ("model", ExtraTreesClassifier(
                n_estimators=500, max_depth=7, min_samples_leaf=70,
                class_weight="balanced", n_jobs=-1, random_state=19,
            )),
        ]),
    }
    reg_models = {
        "hgb_pnl": Pipeline([
            ("prep", make_preprocessor(cols)),
            ("model", HistGradientBoostingRegressor(
                max_iter=180, max_leaf_nodes=15, learning_rate=0.035,
                l2_regularization=2.0, min_samples_leaf=90, random_state=23,
                loss="squared_error",
            )),
        ]),
        "rf_pnl": Pipeline([
            ("prep", make_preprocessor(cols)),
            ("model", RandomForestRegressor(
                n_estimators=350, max_depth=7, min_samples_leaf=80,
                n_jobs=-1, random_state=29,
            )),
        ]),
        "extra_pnl": Pipeline([
            ("prep", make_preprocessor(cols)),
            ("model", ExtraTreesRegressor(
                n_estimators=500, max_depth=8, min_samples_leaf=60,
                n_jobs=-1, random_state=31,
            )),
        ]),
    }
    rows = []
    for name, model in clf_models.items():
        try:
            target = "good30" if name.endswith("good30") else "win"
            y_train = train[target].astype(int)
            model.fit(train, y_train)
            for frame, label in ((train, "train"), (valid, "valid"), (oos, "oos")):
                frame[f"score_{name}"] = model.predict_proba(frame)[:, 1]
            auc = {}
            for frame, label in ((valid, "valid"), (oos, "oos")):
                if frame[target].nunique() > 1:
                    auc[label] = round(float(roc_auc_score(frame[target], frame[f"score_{name}"])), 4)
                else:
                    auc[label] = None
            for share in (0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5):
                cut = float(valid[f"score_{name}"].quantile(1.0 - share))
                masks = {
                    "train": train[f"score_{name}"] >= cut,
                    "valid": valid[f"score_{name}"] >= cut,
                    "oos": oos[f"score_{name}"] >= cut,
                }
                st = {lab: summarize(frame[masks[lab]]) for frame, lab in ((train, "train"), (valid, "valid"), (oos, "oos"))}
                if st["valid"]["n"] < min_valid:
                    continue
                rows.append({
                    "kind": "model_score",
                    "model": name,
                    "target": target,
                    "rule": f"{name} top {share:.0%} by validation score",
                    "share": share,
                    "cut": round(cut, 8),
                    "valid_auc": auc["valid"],
                    "oos_auc": auc["oos"],
                    **{f"{k}_{m}": v for k, stats in st.items() for m, v in stats.items()},
                })
        except Exception as exc:
            rows.append({"kind": "model_error", "model": name, "error": repr(exc)})
    for name, model in reg_models.items():
        try:
            model.fit(train, train["pnl_bps"].astype(float))
            for frame, label in ((train, "train"), (valid, "valid"), (oos, "oos")):
                frame[f"score_{name}"] = model.predict(frame)
            for share in (0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5):
                cut = float(valid[f"score_{name}"].quantile(1.0 - share))
                masks = {
                    "train": train[f"score_{name}"] >= cut,
                    "valid": valid[f"score_{name}"] >= cut,
                    "oos": oos[f"score_{name}"] >= cut,
                }
                st = {lab: summarize(frame[masks[lab]]) for frame, lab in ((train, "train"), (valid, "valid"), (oos, "oos"))}
                if st["valid"]["n"] < min_valid:
                    continue
                rows.append({
                    "kind": "model_score",
                    "model": name,
                    "target": "pnl_bps",
                    "rule": f"{name} top {share:.0%} by validation score",
                    "share": share,
                    "cut": round(cut, 8),
                    "valid_auc": None,
                    "oos_auc": None,
                    **{f"{k}_{m}": v for k, stats in st.items() for m, v in stats.items()},
                })
        except Exception as exc:
            rows.append({"kind": "model_error", "model": name, "error": repr(exc)})
    return rows


def main():
    ap = argparse.ArgumentParser()
    repo_root = Path(__file__).resolve().parents[2]
    ap.add_argument("--trades", default=str(repo_root / "bt-core/out/intraday/SuperTrendIntraday/st_meta_baseline_p5m2_2024_20260527/trades_log.csv"))
    ap.add_argument("--features", default=str(repo_root / "bt-strategy-test/RTH_analysis/out/rth_features.parquet"))
    ap.add_argument("--outdir", default="out/supertrend_meta_filter")
    ap.add_argument("--min-valid", type=int, default=80)
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    trades_path = Path(args.trades).resolve()
    features_path = Path(args.features).resolve()
    outdir = (root / args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    dataset = build_dataset(trades_path, features_path, outdir / "supertrend_rth_event_dataset.parquet")
    train, valid, oos = split_frames(dataset)
    cols = feature_columns(dataset)

    rows = []
    rows.extend(quantile_edges(train, valid, oos, cols, min_valid=args.min_valid))
    rows.extend(pairwise_edges(train, valid, oos, cols, min_valid=args.min_valid))
    rows.extend(model_rankers(train, valid, oos, cols, min_valid=args.min_valid))

    results = pd.DataFrame(rows)
    if not results.empty and "valid_edge_bps" in results.columns:
        results = results.sort_values(["valid_edge_bps", "valid_n"], ascending=[False, False])
    results.to_csv(outdir / "filter_candidates.csv", index=False)

    payload = {
        "dataset": {
            "rows": int(len(dataset)),
            "date_min": str(dataset["trading_date"].min().date()) if len(dataset) else None,
            "date_max": str(dataset["trading_date"].max().date()) if len(dataset) else None,
            "baseline_all": summarize(dataset),
            "train": summarize(train),
            "valid": summarize(valid),
            "oos": summarize(oos),
            "feature_count": len(cols),
        },
        "top_valid_edge": results.head(25).replace({np.nan: None}).to_dict(orient="records") if not results.empty else [],
        "top_oos_edge_min_valid": (
            results[results.get("valid_n", 0).fillna(0) >= args.min_valid]
            .sort_values(["oos_edge_bps", "oos_n"], ascending=[False, False])
            .head(25)
            .replace({np.nan: None})
            .to_dict(orient="records")
            if not results.empty and "oos_edge_bps" in results.columns
            else []
        ),
    }
    (outdir / "summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(json.dumps(payload["dataset"], indent=2))
    print("\nTOP VALID EDGE")
    print(results.head(12).to_string(index=False) if not results.empty else "no results")
    if not results.empty and "oos_edge_bps" in results.columns:
        print("\nTOP OOS EDGE")
        print(results.sort_values(["oos_edge_bps", "oos_n"], ascending=[False, False]).head(12).to_string(index=False))
    print(f"\nOUTDIR {outdir}")


if __name__ == "__main__":
    main()
