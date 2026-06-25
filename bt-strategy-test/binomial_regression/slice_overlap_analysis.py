#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from itertools import combinations
from pathlib import Path
import re
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

LOG = logging.getLogger("slice_overlap_analysis")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Analisi standalone overlap su TUTTE le slice delle feature base. "
            "Per ogni coppia feature genera matrice slice x slice."
        )
    )
    p.add_argument("--input-glob", default="results/*_feature_outcome_full.csv")
    p.add_argument("--thresholds-file", default="results/diagnostics/feature_slice_thresholds.csv")
    p.add_argument("--output-dir", default="results/slice_overlap_analysis")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def resolve(path_like: str) -> Path:
    p = Path(path_like)
    if p.is_absolute():
        return p
    return (Path(__file__).resolve().parent / path_like).resolve()


def load_full_data(input_glob: str) -> pd.DataFrame:
    here = Path(__file__).resolve().parent
    files = sorted(here.glob(input_glob))
    if not files:
        raise FileNotFoundError(f"Nessun file trovato con glob: {input_glob}")

    chunks: List[pd.DataFrame] = []
    for fp in files:
        m = re.match(r"^([^_]+)_feature_outcome_full\.csv$", fp.name)
        asset = m.group(1) if m else "UNK"
        df = pd.read_csv(fp)
        df["asset"] = asset
        chunks.append(df)

    out = pd.concat(chunks, ignore_index=True)
    if "timestamp" in out.columns:
        out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
    return out


def assign_slice(series: pd.Series, lo: float, hi: float) -> pd.Series:
    x = pd.to_numeric(series, errors="coerce")
    mask = x.notna()
    out = pd.Series(False, index=series.index)
    out.loc[mask] = (x.loc[mask] >= lo) & (x.loc[mask] <= hi)
    return out


def save_heatmap(mat: pd.DataFrame, title: str, out_path: Path, cmap: str, cbar: str) -> None:
    if mat.empty:
        return
    fig, ax = plt.subplots(figsize=(max(8, len(mat.columns) * 0.9), max(7, len(mat.index) * 0.8)))
    sns.heatmap(
        mat.astype(float),
        annot=True,
        fmt=".3f",
        cmap=cmap,
        linewidths=0.3,
        linecolor="#eeeeee",
        cbar_kws={"label": cbar},
        ax=ax,
    )
    ax.set_title(title)
    ax.set_xlabel("Slices feature B")
    ax.set_ylabel("Slices feature A")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    out_dir = resolve(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    pair_dir = out_dir / "pair_matrices"
    pair_dir.mkdir(parents=True, exist_ok=True)

    data = load_full_data(args.input_glob)
    thr = pd.read_csv(resolve(args.thresholds_file))
    req = {"feature", "slice_id", "slice_low", "slice_high"}
    miss = req - set(thr.columns)
    if miss:
        raise RuntimeError(f"Soglie incomplete: mancano {sorted(miss)}")

    base_features = sorted(thr["feature"].dropna().unique().tolist())
    if not base_features:
        raise RuntimeError("Nessuna base feature trovata nel file soglie")

    LOG.info("Base features: %s", ", ".join(base_features))

    # Build masks per ogni feature/slice
    mask_map: Dict[str, Dict[int, pd.Series]] = {}
    range_rows = []
    for feat in base_features:
        if feat not in data.columns:
            raise RuntimeError(f"Feature base non presente nel dataset: {feat}")

        sub = thr[thr["feature"] == feat].copy().sort_values("slice_id")
        if sub.empty:
            raise RuntimeError(f"Nessuna soglia per feature {feat}")

        mask_map[feat] = {}
        for _, r in sub.iterrows():
            sid = int(r["slice_id"])
            lo = float(r["slice_low"])
            hi = float(r["slice_high"])
            m = assign_slice(data[feat], lo, hi)
            mask_map[feat][sid] = m
            range_rows.append(
                {
                    "feature": feat,
                    "slice_id": sid,
                    "slice_low": lo,
                    "slice_high": hi,
                    "n_feature_pass": int(m.sum()),
                    "feature_pass_rate": float(m.mean()),
                }
            )

    pd.DataFrame(range_rows).to_csv(out_dir / "slice_overlap_ranges_all_slices.csv", index=False)

    n_total = int(len(data))
    pair_rows = []

    # Matrici slice_a x slice_b per ogni coppia feature
    for fa, fb in combinations(base_features, 2):
        slices_a = sorted(mask_map[fa].keys())
        slices_b = sorted(mask_map[fb].keys())

        jacc = pd.DataFrame(index=slices_a, columns=slices_b, dtype=float)
        inter_rate = pd.DataFrame(index=slices_a, columns=slices_b, dtype=float)
        overlap_coef = pd.DataFrame(index=slices_a, columns=slices_b, dtype=float)
        lift_mat = pd.DataFrame(index=slices_a, columns=slices_b, dtype=float)

        for sa in slices_a:
            ma = mask_map[fa][sa].fillna(False).astype(bool)
            n_a = int(ma.sum())
            for sb in slices_b:
                mb = mask_map[fb][sb].fillna(False).astype(bool)
                n_b = int(mb.sum())

                inter = int((ma & mb).sum())
                union = int((ma | mb).sum())
                j = (inter / union) if union > 0 else np.nan
                ir = (inter / n_total) if n_total > 0 else np.nan
                oc = (inter / min(n_a, n_b)) if min(n_a, n_b) > 0 else np.nan
                lift = (
                    (inter / n_total) / ((n_a / n_total) * (n_b / n_total))
                    if n_total > 0 and n_a > 0 and n_b > 0
                    else np.nan
                )

                pair_rows.append(
                    {
                        "feature_a": fa,
                        "slice_a": sa,
                        "feature_b": fb,
                        "slice_b": sb,
                        "n_total": n_total,
                        "n_a": n_a,
                        "n_b": n_b,
                        "n_intersection": inter,
                        "n_union": union,
                        "intersection_rate_total": ir,
                        "jaccard": j,
                        "overlap_coefficient": oc,
                        "lift_vs_independence": lift,
                    }
                )

                jacc.loc[sa, sb] = j
                inter_rate.loc[sa, sb] = ir
                overlap_coef.loc[sa, sb] = oc
                lift_mat.loc[sa, sb] = lift

        tag = f"{re.sub(r'[^A-Za-z0-9_.-]', '_', fa)}__vs__{re.sub(r'[^A-Za-z0-9_.-]', '_', fb)}"
        jacc.to_csv(pair_dir / f"slice_overlap_jaccard_{tag}.csv")
        inter_rate.to_csv(pair_dir / f"slice_overlap_intersection_rate_{tag}.csv")
        overlap_coef.to_csv(pair_dir / f"slice_overlap_overlapcoef_{tag}.csv")
        lift_mat.to_csv(pair_dir / f"slice_overlap_lift_{tag}.csv")

        save_heatmap(
            jacc,
            f"Jaccard | {fa} (y) vs {fb} (x)",
            pair_dir / f"slice_overlap_jaccard_{tag}.png",
            cmap="Blues",
            cbar="Jaccard",
        )
        save_heatmap(
            inter_rate,
            f"Intersection/Total | {fa} (y) vs {fb} (x)",
            pair_dir / f"slice_overlap_intersection_rate_{tag}.png",
            cmap="Greens",
            cbar="Intersection rate on total",
        )

    pair_df = pd.DataFrame(pair_rows)
    pair_df.to_csv(out_dir / "slice_overlap_pairwise_all_slices.csv", index=False)

    # Sintesi per coppia feature
    if not pair_df.empty:
        summary = (
            pair_df.groupby(["feature_a", "feature_b"], as_index=False)
            .agg(
                max_jaccard=("jaccard", "max"),
                mean_jaccard=("jaccard", "mean"),
                max_intersection_rate=("intersection_rate_total", "max"),
                mean_intersection_rate=("intersection_rate_total", "mean"),
                max_overlap_coefficient=("overlap_coefficient", "max"),
                max_lift=("lift_vs_independence", "max"),
            )
            .sort_values("max_jaccard", ascending=False)
        )
        summary.to_csv(out_dir / "slice_overlap_pair_summary.csv", index=False)

    LOG.info("Output scritto in: %s", out_dir)


if __name__ == "__main__":
    main()
