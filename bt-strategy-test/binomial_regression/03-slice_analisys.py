#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from pathlib import Path
import re
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

LOG = logging.getLogger("slice_analisys")

OUTCOME_ORDER = [
    "close_ret_5m",
    "high_ret_max_5m",
    "low_ret_min_5m",
    "close_ret_10m",
    "high_ret_max_10m",
    "low_ret_min_10m",
    "close_ret_15m",
    "high_ret_max_15m",
    "low_ret_min_15m",
    "close_ret_eod",
    "high_ret_max_eod",
    "low_ret_min_eod",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Seconda analisi separata: filtro AND sulla slice scelta per tutte le feature base "
            "e analisi nuove feature (disp/r2/mse/rmse/mae) con distribuzioni + slice matrix Spearman."
        )
    )
    p.add_argument(
        "--input-glob",
        default="results/*_feature_outcome_full.csv",
        help="Glob dei file full feature/outcome.",
    )
    p.add_argument(
        "--thresholds-file",
        default="results/diagnostics/feature_slice_thresholds.csv",
        help="CSV con soglie slice della prima analisi (non viene modificato).",
    )
    p.add_argument(
        "--slice-id",
        "--slice_id",
        type=int,
        default=1,
        help="Slice da usare sulle feature base (default: 1).",
    )
    p.add_argument(
        "--output-dir",
        default="results/slice_analisys",
        help="Cartella output dedicata a questa seconda analisi.",
    )
    p.add_argument(
        "--min-points",
        type=int,
        default=30,
        help="Minimo punti per calcolare correlazioni in una slice.",
    )
    p.add_argument(
        "--volz-min",
        "--volz_min",
        "--volz-min-quantile",
        "--volz_min_quantile",
        type=float,
        default=0.85,
        help="Filtro addizionale assoluto: usa solo righe con volZ >= soglia (default: 0.85). Metti valore negativo per disattivare.",
    )
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
    here = Path(__file__).resolve().parent
    return (here / path_like).resolve()


def load_full_data(input_glob: str) -> pd.DataFrame:
    here = Path(__file__).resolve().parent
    files = sorted(here.glob(input_glob))
    if not files:
        raise FileNotFoundError(f"Nessun file trovato con glob: {input_glob}")

    parts: List[pd.DataFrame] = []
    for fp in files:
        m = re.match(r"^([^_]+)_feature_outcome_full\.csv$", fp.name)
        asset = m.group(1) if m else "UNK"
        df = pd.read_csv(fp)
        df["asset"] = asset
        parts.append(df)

    out = pd.concat(parts, ignore_index=True)
    if "timestamp" in out.columns:
        out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
    return out


def ordered_outcomes(df: pd.DataFrame) -> List[str]:
    cols = [c for c in OUTCOME_ORDER if c in df.columns]
    extra = sorted([c for c in df.columns if c.startswith(("close_ret_", "high_ret_max_", "low_ret_min_")) and c not in cols])
    return cols + extra


def add_new_features(df: pd.DataFrame, outcomes: List[str]) -> pd.DataFrame:
    # Hook esplicito: nessuna feature costruita da outcome futuri (evita leakage).
    _ = outcomes
    out = df.copy()

    # Feature aggregata di "qualita' fit" per finestra:
    # alta quando r2 e' alto e dispersion/errori sono bassi.
    # Uso rank percentili (0..1) per evitare problemi di scale diverse.
    for win in (5, 20, 60):
        r2_col = f"r2_{win}"
        disp_col = f"disp_{win}"
        mse_col = f"mse_{win}"
        rmse_col = f"rmse_{win}"
        mae_col = f"mae_{win}"
        needed = [r2_col, disp_col, mse_col, rmse_col, mae_col]
        if not all(c in out.columns for c in needed):
            continue

        r2_rank = pd.to_numeric(out[r2_col], errors="coerce").rank(pct=True)
        disp_rank = pd.to_numeric(out[disp_col], errors="coerce").rank(pct=True)
        mse_rank = pd.to_numeric(out[mse_col], errors="coerce").rank(pct=True)
        rmse_rank = pd.to_numeric(out[rmse_col], errors="coerce").rank(pct=True)
        mae_rank = pd.to_numeric(out[mae_col], errors="coerce").rank(pct=True)

        out[f"fit_quality_{win}"] = pd.concat(
            [
                r2_rank.rename("r2_good"),
                (1 - disp_rank).rename("disp_good"),
                (1 - mse_rank).rename("mse_good"),
                (1 - rmse_rank).rename("rmse_good"),
                (1 - mae_rank).rename("mae_good"),
            ],
            axis=1,
        ).mean(axis=1, skipna=False)

    return out


def find_new_features(df: pd.DataFrame) -> List[str]:
    candidates: List[str] = []
    candidates.extend([c for c in df.columns if c.startswith("r2_")])
    candidates.extend([c for c in df.columns if c.startswith("mse_")])
    candidates.extend([c for c in df.columns if c.startswith("rmse_")])
    candidates.extend([c for c in df.columns if c.startswith("mae_")])
    candidates.extend([c for c in df.columns if c.startswith("disp_")])
    candidates.extend([c for c in df.columns if c.startswith("fit_quality_")])
    if "volZ" in df.columns:
        candidates.append("volZ")

    # ordine stabile
    uniq = []
    seen = set()
    for c in candidates:
        if c not in seen:
            uniq.append(c)
            seen.add(c)
    return uniq


def _weight_grid(parts: int = 10) -> List[np.ndarray]:
    # 5 componenti con pesi non-negativi che sommano a 1.
    grid: List[np.ndarray] = []
    for a in range(parts + 1):
        for b in range(parts - a + 1):
            for c in range(parts - a - b + 1):
                for d in range(parts - a - b - c + 1):
                    e = parts - a - b - c - d
                    w = np.array([a, b, c, d, e], dtype=float) / parts
                    grid.append(w)
    return grid


def build_optimized_fit_quality_features(
    df: pd.DataFrame,
    outcomes: List[str],
    min_points: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = df.copy()
    rows = []
    grid = _weight_grid(parts=10)

    for win in (5, 20, 60):
        r2_col = f"r2_{win}"
        disp_col = f"disp_{win}"
        mse_col = f"mse_{win}"
        rmse_col = f"rmse_{win}"
        mae_col = f"mae_{win}"
        needed = [r2_col, disp_col, mse_col, rmse_col, mae_col]
        if not all(c in out.columns for c in needed):
            continue

        comp = pd.DataFrame(
            {
                "r2_good": pd.to_numeric(out[r2_col], errors="coerce").rank(pct=True),
                "disp_good": 1 - pd.to_numeric(out[disp_col], errors="coerce").rank(pct=True),
                "mse_good": 1 - pd.to_numeric(out[mse_col], errors="coerce").rank(pct=True),
                "rmse_good": 1 - pd.to_numeric(out[rmse_col], errors="coerce").rank(pct=True),
                "mae_good": 1 - pd.to_numeric(out[mae_col], errors="coerce").rank(pct=True),
            }
        )

        best_obj = -np.inf
        best_w: np.ndarray | None = None
        best_score: pd.Series | None = None
        best_corrs: dict[str, float] = {}

        # Obiettivo: massimizzare la correlazione assoluta (Spearman) piu' alta
        # sui target outcome disponibili.
        for w in grid:
            s = comp.dot(w)
            vals_abs = []
            corrs = {}
            for out_col in outcomes:
                y = pd.to_numeric(out[out_col], errors="coerce")
                m = s.notna() & y.notna()
                if m.sum() < min_points:
                    continue
                rho = s[m].corr(y[m], method="spearman")
                if pd.isna(rho):
                    continue
                vals_abs.append(abs(float(rho)))
                corrs[out_col] = float(rho)
            if not vals_abs:
                continue
            obj = float(np.max(vals_abs))
            if obj > best_obj:
                best_obj = obj
                best_w = w.copy()
                best_score = s.copy()
                best_corrs = corrs

        if best_w is None or best_score is None:
            continue

        col_name = f"fit_quality_opt_{win}"
        out[col_name] = best_score
        rows.append(
            {
                "window": win,
                "feature": col_name,
                "objective_max_abs_spearman": best_obj,
                "w_r2_good": float(best_w[0]),
                "w_disp_good": float(best_w[1]),
                "w_mse_good": float(best_w[2]),
                "w_rmse_good": float(best_w[3]),
                "w_mae_good": float(best_w[4]),
                "n_outcomes_used": int(len(best_corrs)),
            }
        )

    return out, pd.DataFrame(rows)


def assign_slice(series: pd.Series, bins_low: float, bins_high: float) -> pd.Series:
    mask = series.notna()
    out = pd.Series(False, index=series.index)
    out.loc[mask] = (series.loc[mask] >= bins_low) & (series.loc[mask] <= bins_high)
    return out


def plot_distribution(values: pd.Series, title: str, out_path: Path) -> None:
    if values.dropna().empty:
        return
    fig, ax = plt.subplots(figsize=(10, 4))
    sns.histplot(values.dropna().astype(float), bins=80, kde=True, ax=ax, color="#335c67")
    ax.set_title(title)
    ax.set_xlabel("value")
    ax.set_ylabel("count")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def quantile_slice_table(series: pd.Series, n_slices: int = 10) -> pd.DataFrame:
    x = pd.to_numeric(series, errors="coerce").dropna()
    if x.empty:
        return pd.DataFrame()

    codes, bins = pd.qcut(x, q=n_slices, labels=False, retbins=True, duplicates="drop")
    rows = []
    for sid in sorted(codes.unique()):
        sid_int = int(sid)
        in_slice = codes == sid_int
        rows.append(
            {
                "slice_id": sid_int + 1,
                "slice_low": float(bins[sid_int]),
                "slice_high": float(bins[sid_int + 1]),
                "n_slice": int(in_slice.sum()),
                "slice_rate": float(in_slice.mean()),
            }
        )
    return pd.DataFrame(rows)


def slice_matrix_spearman(
    series: pd.Series,
    outcomes_df: pd.DataFrame,
    min_points: int,
    n_slices: int = 10,
) -> pd.DataFrame:
    x = pd.to_numeric(series, errors="coerce")
    mask = x.notna()
    x_valid = x[mask]
    if x_valid.empty:
        return pd.DataFrame()

    codes, bins = pd.qcut(x_valid, q=n_slices, labels=False, retbins=True, duplicates="drop")
    slice_id = pd.Series(np.nan, index=series.index)
    slice_id.loc[x_valid.index] = codes.astype(float)

    rows = []
    for out_col in outcomes_df.columns:
        y = pd.to_numeric(outcomes_df[out_col], errors="coerce")
        m = slice_id.notna() & y.notna()
        if m.sum() < min_points:
            continue

        yv = y[m]
        sv = slice_id[m].astype(int)

        for s in sorted(sv.unique()):
            in_s = sv == s
            out_s = sv != s
            if in_s.sum() < min_points or out_s.sum() < min_points:
                continue

            pb = in_s.astype(int).corr(yv, method="pearson")
            rows.append(
                {
                    "slice_id": int(s) + 1,
                    "slice_low": float(bins[int(s)]),
                    "slice_high": float(bins[int(s) + 1]),
                    "outcome": out_col,
                    "point_biserial_corr_slice_vs_outcome": float(pb) if pd.notna(pb) else np.nan,
                    "mean_outcome_in_slice": float(yv[in_s].mean()),
                    "n_slice": int(in_s.sum()),
                }
            )

    return pd.DataFrame(rows)


def plot_slice_matrix_heatmap(matrix_df: pd.DataFrame, title: str, out_path: Path, outcomes_order: List[str]) -> None:
    if matrix_df.empty:
        return

    mat = matrix_df.pivot(index="slice_id", columns="outcome", values="point_biserial_corr_slice_vs_outcome")
    ordered_cols = [c for c in outcomes_order if c in mat.columns]
    mat = mat.reindex(columns=ordered_cols)
    mat = mat.sort_index()

    y_labels = []
    bounds = matrix_df[["slice_id", "slice_low", "slice_high"]].drop_duplicates().set_index("slice_id").sort_index()
    for sid in mat.index:
        lo = bounds.loc[sid, "slice_low"]
        hi = bounds.loc[sid, "slice_high"]
        y_labels.append(f"{int(sid)} [{lo:.6g}, {hi:.6g}]")

    fig, ax = plt.subplots(figsize=(max(12, len(mat.columns) * 1.2), max(5, len(mat.index) * 0.6)))
    sns.heatmap(
        mat,
        cmap="RdBu_r",
        vmin=np.nanmin(mat.values),
        vmax=np.nanmax(mat.values),
        center=0.0,
        annot=True,
        fmt=".3f",
        linewidths=0.3,
        linecolor="#eeeeee",
        cbar_kws={"label": "point-biserial corr (slice vs outcome)"},
        ax=ax,
    )
    ax.set_title(title)
    ax.set_xlabel("Outcomes")
    ax.set_ylabel("New-feature slices")
    ax.set_yticklabels(y_labels, rotation=0)
    ax.tick_params(axis="x", labelrotation=35)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def analyze_set_overlap(feature_masks: Dict[str, pd.Series], out_dir: Path) -> None:
    feats = list(feature_masks.keys())
    if not feats:
        return

    rows = []
    n_total = int(len(next(iter(feature_masks.values()))))
    jaccard_mat = pd.DataFrame(index=feats, columns=feats, dtype=float)
    inter_rate_mat = pd.DataFrame(index=feats, columns=feats, dtype=float)

    for fa in feats:
        ma = feature_masks[fa].fillna(False).astype(bool)
        n_a = int(ma.sum())
        for fb in feats:
            mb = feature_masks[fb].fillna(False).astype(bool)
            n_b = int(mb.sum())
            inter = int((ma & mb).sum())
            union = int((ma | mb).sum())
            jacc = (inter / union) if union > 0 else np.nan
            inter_rate = (inter / n_total) if n_total > 0 else np.nan
            overlap_coef = (inter / min(n_a, n_b)) if min(n_a, n_b) > 0 else np.nan
            lift_vs_ind = (
                (inter / n_total) / ((n_a / n_total) * (n_b / n_total))
                if n_total > 0 and n_a > 0 and n_b > 0
                else np.nan
            )
            rows.append(
                {
                    "feature_a": fa,
                    "feature_b": fb,
                    "n_total": n_total,
                    "n_a": n_a,
                    "n_b": n_b,
                    "n_intersection": inter,
                    "n_union": union,
                    "intersection_rate_total": inter_rate,
                    "jaccard": jacc,
                    "overlap_coefficient": overlap_coef,
                    "lift_vs_independence": lift_vs_ind,
                }
            )
            jaccard_mat.loc[fa, fb] = jacc
            inter_rate_mat.loc[fa, fb] = inter_rate

    pair_df = pd.DataFrame(rows)
    pair_df.to_csv(out_dir / "slice_set_overlap_pairwise.csv", index=False)
    jaccard_mat.to_csv(out_dir / "slice_set_overlap_jaccard_matrix.csv")
    inter_rate_mat.to_csv(out_dir / "slice_set_overlap_intersection_rate_matrix.csv")

    fig, ax = plt.subplots(figsize=(max(7, len(feats) * 0.9), max(6, len(feats) * 0.8)))
    sns.heatmap(
        jaccard_mat.astype(float),
        annot=True,
        fmt=".3f",
        cmap="Blues",
        linewidths=0.3,
        linecolor="#eeeeee",
        cbar_kws={"label": "Jaccard"},
        ax=ax,
    )
    ax.set_title("Set overlap (Jaccard) tra slice feature")
    fig.tight_layout()
    fig.savefig(out_dir / "slice_set_overlap_jaccard_matrix.png", dpi=140)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(max(7, len(feats) * 0.9), max(6, len(feats) * 0.8)))
    sns.heatmap(
        inter_rate_mat.astype(float),
        annot=True,
        fmt=".3f",
        cmap="Greens",
        linewidths=0.3,
        linecolor="#eeeeee",
        cbar_kws={"label": "Intersection rate on total"},
        ax=ax,
    )
    ax.set_title("Set overlap (intersezione / totale) tra slice feature")
    fig.tight_layout()
    fig.savefig(out_dir / "slice_set_overlap_intersection_rate_matrix.png", dpi=140)
    plt.close(fig)

    cum_rows = []
    running = pd.Series(True, index=next(iter(feature_masks.values())).index)
    for i, f in enumerate(feats, start=1):
        running &= feature_masks[f].fillna(False).astype(bool)
        n_run = int(running.sum())
        cum_rows.append(
            {
                "step": i,
                "added_feature": f,
                "n_after_and": n_run,
                "rate_after_and": float(n_run / n_total) if n_total > 0 else np.nan,
            }
        )
    pd.DataFrame(cum_rows).to_csv(out_dir / "slice_set_overlap_cumulative_and.csv", index=False)


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    out_dir = resolve(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    data = load_full_data(args.input_glob)
    outcomes = ordered_outcomes(data)
    if not outcomes:
        raise RuntimeError("Nessun outcome trovato.")

    data = add_new_features(data, outcomes)
    new_features = find_new_features(data)
    if not new_features:
        raise RuntimeError("Nessuna nuova feature trovata (r2_*, mse_*, rmse_*, mae_*, disp_*).")

    thresholds_path = resolve(args.thresholds_file)
    thr = pd.read_csv(thresholds_path)
    required = {"feature", "slice_id", "slice_low", "slice_high"}
    missing = required - set(thr.columns)
    if missing:
        raise RuntimeError(f"Soglie incomplete in {thresholds_path}: mancano {sorted(missing)}")

    base_features = sorted(thr["feature"].dropna().unique().tolist())
    LOG.info("Slice target: %s", args.slice_id)
    LOG.info("Base features: %s", ", ".join(base_features))
    LOG.info("New features: %s", ", ".join(new_features))

    # Filtro AND: una riga passa solo se appartiene alla slice scelta per TUTTE le feature base.
    filter_rows = []
    feature_masks: Dict[str, pd.Series] = {}
    mask_all = pd.Series(True, index=data.index)
    for base_feat in base_features:
        if base_feat not in data.columns:
            raise RuntimeError(f"Feature base mancante nel dataset: {base_feat}")
        sel = thr[(thr["feature"] == base_feat) & (thr["slice_id"] == args.slice_id)]
        if sel.empty:
            raise RuntimeError(f"Soglia mancante: feature={base_feat} slice_id={args.slice_id}")
        lo = float(sel.iloc[0]["slice_low"])
        hi = float(sel.iloc[0]["slice_high"])
        mask_feat = assign_slice(pd.to_numeric(data[base_feat], errors="coerce"), lo, hi)
        feature_masks[base_feat] = mask_feat
        mask_all &= mask_feat
        filter_rows.append(
            {
                "base_feature": base_feat,
                "slice_id": args.slice_id,
                "slice_low": lo,
                "slice_high": hi,
                "n_feature_pass": int(mask_feat.sum()),
                "feature_pass_rate": float(mask_feat.mean()),
            }
        )

    sdf = data[mask_all].copy()
    volz_threshold = np.nan
    n_after_volz = len(sdf)
    if args.volz_min >= 0 and "volZ" in sdf.columns:
        volz_series = pd.to_numeric(sdf["volZ"], errors="coerce")
        if volz_series.notna().any():
            volz_threshold = float(args.volz_min)
            sdf = sdf[volz_series >= volz_threshold].copy()
            n_after_volz = len(sdf)
            LOG.info(
                "Applied volZ filter: volZ >= %.6g -> rows=%d",
                volz_threshold,
                n_after_volz,
            )

    meta = {
        "slice_id": args.slice_id,
        "n_input_rows": int(len(data)),
        "n_rows_after_and_filter": int(mask_all.sum()),
        "row_rate_after_and_filter": float(mask_all.mean()) if len(data) > 0 else np.nan,
        "volz_min_value": float(args.volz_min),
        "volz_threshold_value": volz_threshold,
        "n_rows_after_volz_filter": int(n_after_volz),
        "row_rate_after_volz_filter": float(n_after_volz / len(data)) if len(data) > 0 else np.nan,
        "n_base_features": int(len(base_features)),
    }

    pd.DataFrame(filter_rows).to_csv(out_dir / "slice_analisys_and_filter_ranges.csv", index=False)
    pd.DataFrame([meta]).to_csv(out_dir / "slice_analisys_meta.csv", index=False)
    analyze_set_overlap(feature_masks, out_dir)

    if sdf.empty:
        LOG.warning("Filtro AND vuoto: nessuna riga soddisfa tutte le slice richieste.")
        LOG.info("Generati report di overlap insiemi in: %s", out_dir)
        return

    # Costruzione feature aggregata ottimizzata (massimizza correlazione direzionale media).
    sdf, opt_weights = build_optimized_fit_quality_features(sdf, outcomes, args.min_points)
    if not opt_weights.empty:
        opt_weights.to_csv(out_dir / "fit_quality_opt_weights.csv", index=False)
        for c in [c for c in sdf.columns if c.startswith("fit_quality_opt_")]:
            if c not in new_features:
                new_features.append(c)

    # Tabelle correlazione richieste:
    # - tra feature diagnostiche
    # - tra feature diagnostiche e outcome
    diag_cols = [c for c in new_features if c in sdf.columns]
    if diag_cols:
        diag_df = sdf[diag_cols].apply(pd.to_numeric, errors="coerce")
        diag_df.corr(method="spearman").to_csv(out_dir / "correlation_diagnostics_spearman.csv")
        diag_df.corr(method="pearson").to_csv(out_dir / "correlation_diagnostics_pearson.csv")

        corr_rows = []
        for feat in diag_cols:
            x = pd.to_numeric(sdf[feat], errors="coerce")
            for out_col in outcomes:
                y = pd.to_numeric(sdf[out_col], errors="coerce")
                m = x.notna() & y.notna()
                if m.sum() < args.min_points:
                    continue
                xv = x[m]
                yv = y[m]
                corr_rows.append(
                    {
                        "feature": feat,
                        "outcome": out_col,
                        "n": int(m.sum()),
                        "spearman_rho": float(xv.corr(yv, method="spearman")),
                        "pearson_r": float(xv.corr(yv, method="pearson")),
                    }
                )
        if corr_rows:
            corr_df = pd.DataFrame(corr_rows)
            corr_df.to_csv(out_dir / "correlation_diagnostics_vs_outcomes.csv", index=False)

    all_matrix_rows = []
    all_slice_tables = []
    for nf in new_features:
        if nf not in sdf.columns:
            continue
        nf_series = pd.to_numeric(sdf[nf], errors="coerce")
        if nf_series.dropna().empty:
            continue
        safe_nf = re.sub(r"[^A-Za-z0-9_.-]", "_", nf)

        # 1) distribuzione
        plot_distribution(
            nf_series,
            title=f"{nf} distribution | AND-filter slice={args.slice_id} on all base features",
            out_path=out_dir / f"distribution_{safe_nf}.png",
        )

        # 2) soglie 10 slice (10%)
        qtab = quantile_slice_table(nf_series, n_slices=10)
        if not qtab.empty:
            qtab.insert(0, "new_feature", nf)
            qtab.insert(1, "base_slice_id", args.slice_id)
            qtab.to_csv(out_dir / f"slice_thresholds_{safe_nf}.csv", index=False)
            all_slice_tables.append(qtab)

        # 3) matrice correlazione slice vs outcome
        mx = slice_matrix_spearman(nf_series, sdf[outcomes], min_points=args.min_points, n_slices=10)
        if not mx.empty:
            mx.insert(0, "new_feature", nf)
            mx.insert(1, "base_slice_id", args.slice_id)
            mx.to_csv(out_dir / f"slice_matrix_{safe_nf}.csv", index=False)
            all_matrix_rows.append(mx)
            plot_slice_matrix_heatmap(
                mx,
                title=f"{nf}: slice-corr matrix | AND-filter slice={args.slice_id}",
                out_path=out_dir / f"slice_matrix_{safe_nf}.png",
                outcomes_order=outcomes,
            )

    if all_slice_tables:
        pd.concat(all_slice_tables, ignore_index=True).to_csv(out_dir / "slice_analisys_thresholds_all.csv", index=False)
    if all_matrix_rows:
        pd.concat(all_matrix_rows, ignore_index=True).to_csv(out_dir / "slice_analisys_matrix_all.csv", index=False)

    LOG.info("Analisi completata. Output: %s", out_dir)


if __name__ == "__main__":
    main()
