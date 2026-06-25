#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from pathlib import Path
import re
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

LOG = logging.getLogger("entry_exit_analysis")
HORIZONS = list(range(5, 61, 5))
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MINUTE_DATA_DIR = REPO_ROOT / "config-common/data/m/alpaca/sip"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Analisi no-stop: probabilita' di entry/hit target e return medio per 5/10/15/60m."
    )
    p.add_argument("--input-glob", default="results/*_feature_outcome_full.csv")
    p.add_argument("--output-dir", default="results/entry_exit_analysis_no_stop")
    p.add_argument("--minute-data-dir", default=str(DEFAULT_MINUTE_DATA_DIR))
    p.add_argument("--target-pct", type=float, default=0.005, help="Target percentuale (default 0.5%%).")

    # Filtri feature assoluti richiesti
    p.add_argument("--slope5-min", type=float, default=0.0004)
    p.add_argument("--slope20-min", type=float, default=0.0002)
    p.add_argument("--slope60-min", type=float, default=0.00015)
    p.add_argument("--volz-min", type=float, default=0.85)

    p.add_argument("--quality-window", type=int, default=60, choices=[5, 20, 60])
    p.add_argument("--quality-min", type=float, default=0.5)
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

    parts: List[pd.DataFrame] = []
    for fp in files:
        m = re.match(r"^([^_]+)_feature_outcome_full\.csv$", fp.name)
        asset = m.group(1) if m else "UNK"
        df = pd.read_csv(fp)
        df["asset"] = asset
        parts.append(df)

    out = pd.concat(parts, ignore_index=True)
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
    return out


def add_fit_quality(df: pd.DataFrame, window: int) -> pd.DataFrame:
    out = df.copy()
    need = [f"r2_{window}", f"disp_{window}", f"mse_{window}", f"rmse_{window}", f"mae_{window}"]
    if not all(c in out.columns for c in need):
        raise RuntimeError(f"Colonne mancanti per fit_quality_{window}: {need}")

    r2_rank = pd.to_numeric(out[f"r2_{window}"], errors="coerce").rank(pct=True)
    disp_rank = pd.to_numeric(out[f"disp_{window}"], errors="coerce").rank(pct=True)
    mse_rank = pd.to_numeric(out[f"mse_{window}"], errors="coerce").rank(pct=True)
    rmse_rank = pd.to_numeric(out[f"rmse_{window}"], errors="coerce").rank(pct=True)
    mae_rank = pd.to_numeric(out[f"mae_{window}"], errors="coerce").rank(pct=True)

    out[f"fit_quality_{window}"] = pd.concat(
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


def ensure_forward_outcomes(df: pd.DataFrame, horizons: List[int]) -> pd.DataFrame:
    out = df.copy()
    for asset, g in out.groupby("asset", sort=False):
        idx = g.sort_values("timestamp").index
        gg = out.loc[idx]
        close = pd.to_numeric(gg["close"], errors="coerce").to_numpy(dtype=float)
        high = pd.to_numeric(gg["high"], errors="coerce").to_numpy(dtype=float)
        low = pd.to_numeric(gg["low"], errors="coerce").to_numpy(dtype=float)
        n = len(gg)

        for h in horizons:
            ccol = f"close_ret_{h}m"
            hcol = f"high_ret_max_{h}m"
            lcol = f"low_ret_min_{h}m"
            if ccol in out.columns and hcol in out.columns and lcol in out.columns:
                continue

            close_ret = np.full(n, np.nan, dtype=float)
            high_ret = np.full(n, np.nan, dtype=float)
            low_ret = np.full(n, np.nan, dtype=float)
            for i in range(n):
                j = i + h
                if j >= n or np.isnan(close[i]):
                    continue
                win_start = i + 1
                win_end = j + 1
                w_high = high[win_start:win_end]
                w_low = low[win_start:win_end]
                if len(w_high) == 0:
                    continue
                close_ret[i] = close[j] / close[i] - 1 if not np.isnan(close[j]) else np.nan
                high_ret[i] = np.nanmax(w_high) / close[i] - 1 if not np.all(np.isnan(w_high)) else np.nan
                low_ret[i] = np.nanmin(w_low) / close[i] - 1 if not np.all(np.isnan(w_low)) else np.nan

            out.loc[idx, ccol] = close_ret
            out.loc[idx, hcol] = high_ret
            out.loc[idx, lcol] = low_ret

    return out


def add_next_bar_fields_from_minute(df: pd.DataFrame, minute_dir: Path) -> pd.DataFrame:
    out = df.copy()
    out["open_next"] = np.nan
    out["high_next"] = np.nan
    out["low_next"] = np.nan
    out["close_next"] = np.nan

    for asset, g in out.groupby("asset", sort=False):
        fp = minute_dir / f"{asset}.csv"
        if not fp.exists():
            LOG.warning("Minute file mancante per asset=%s: %s", asset, fp)
            continue

        md = pd.read_csv(fp, usecols=["timestamp", "open", "high", "low", "close"])
        md["timestamp"] = pd.to_datetime(md["timestamp"], errors="coerce", utc=True)
        md = md.sort_values("timestamp")
        md["open_next"] = pd.to_numeric(md["open"], errors="coerce").shift(-1)
        md["high_next"] = pd.to_numeric(md["high"], errors="coerce").shift(-1)
        md["low_next"] = pd.to_numeric(md["low"], errors="coerce").shift(-1)
        md["close_next"] = pd.to_numeric(md["close"], errors="coerce").shift(-1)
        md = md[["timestamp", "open_next", "high_next", "low_next", "close_next"]]

        left_idx = g.index
        merged = out.loc[left_idx, ["timestamp"]].merge(md, on="timestamp", how="left")
        out.loc[left_idx, "open_next"] = merged["open_next"].to_numpy()
        out.loc[left_idx, "high_next"] = merged["high_next"].to_numpy()
        out.loc[left_idx, "low_next"] = merged["low_next"].to_numpy()
        out.loc[left_idx, "close_next"] = merged["close_next"].to_numpy()

    return out


def run_target_only(df: pd.DataFrame, horizon: int, entry_mode: str, target_pct: float) -> pd.DataFrame:
    close = pd.to_numeric(df["close"], errors="coerce")
    high_fut = close * (1 + pd.to_numeric(df[f"high_ret_max_{horizon}m"], errors="coerce"))
    low_fut = close * (1 + pd.to_numeric(df[f"low_ret_min_{horizon}m"], errors="coerce"))
    close_fut = close * (1 + pd.to_numeric(df[f"close_ret_{horizon}m"], errors="coerce"))

    if entry_mode == "entry_open_first_bar":
        entry = pd.to_numeric(df["open_next"], errors="coerce")
        entered = entry.notna()
    elif entry_mode == "entry_close_calc_bar":
        entry = close
        # Entry order valid only on the first next bar.
        low_next = pd.to_numeric(df["low_next"], errors="coerce")
        high_next = pd.to_numeric(df["high_next"], errors="coerce")
        entered = (entry.notna()) & (low_next <= entry) & (high_next >= entry)
    elif entry_mode == "entry_low_calc_bar":
        entry = pd.to_numeric(df["low"], errors="coerce")
        # Entry order valid only on the first next bar.
        low_next = pd.to_numeric(df["low_next"], errors="coerce")
        high_next = pd.to_numeric(df["high_next"], errors="coerce")
        entered = (entry.notna()) & (low_next <= entry) & (high_next >= entry)
    else:
        raise ValueError(entry_mode)

    target_px = entry * (1 + target_pct)
    hit_target = entered & (high_fut >= target_px)
    miss_target = entered & (~hit_target)

    close_ret_end = pd.Series(np.nan, index=df.index, dtype=float)
    close_ret_end.loc[entered] = (close_fut.loc[entered] / entry.loc[entered]) - 1

    out = pd.DataFrame(
        {
            "entered": entered,
            "hit_target": hit_target,
            "miss_target": miss_target,
            "close_ret_end": close_ret_end,
        }
    )
    out["entry_mode"] = entry_mode
    out["horizon_m"] = horizon
    return out


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    out_dir = resolve(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_full_data(args.input_glob)
    df = ensure_forward_outcomes(df, HORIZONS)
    df = add_fit_quality(df, window=args.quality_window)
    df = add_next_bar_fields_from_minute(df, resolve(args.minute_data_dir))

    mask = (
        (pd.to_numeric(df["slope_5"], errors="coerce") > args.slope5_min)
        & (pd.to_numeric(df["slope_20"], errors="coerce") > args.slope20_min)
        & (pd.to_numeric(df["slope_60"], errors="coerce") > args.slope60_min)
        & (pd.to_numeric(df[f"fit_quality_{args.quality_window}"], errors="coerce") >= args.quality_min)
        & (pd.to_numeric(df["volZ"], errors="coerce") > args.volz_min)
    )
    sdf = df[mask].copy()

    meta = pd.DataFrame([
        {
            "n_input": int(len(df)),
            "n_selected": int(len(sdf)),
            "selection_rate": float(len(sdf) / len(df)) if len(df) > 0 else np.nan,
            "target_pct": args.target_pct,
            "slope_5_min": args.slope5_min,
            "slope_20_min": args.slope20_min,
            "slope_60_min": args.slope60_min,
            "quality_window": args.quality_window,
            "quality_min": args.quality_min,
            "volz_min": args.volz_min,
        }
    ])
    meta.to_csv(out_dir / "analysis_meta.csv", index=False)

    if sdf.empty:
        LOG.warning("Nessuna riga dopo i filtri.")
        return

    rows = []
    for h in HORIZONS:
        for mode in ["entry_open_first_bar", "entry_close_calc_bar", "entry_low_calc_bar"]:
            rows.append(run_target_only(sdf, h, mode, args.target_pct))

    rr = pd.concat(rows, ignore_index=True)
    rr.to_csv(out_dir / "target_only_row_level.csv", index=False)

    summary = (
        rr.groupby(["entry_mode", "horizon_m"], as_index=False)
        .agg(
            n=("entered", "size"),
            p_enter=("entered", "mean"),
            p_hit_target_uncond=("hit_target", "mean"),
            p_miss_target_uncond=("miss_target", "mean"),
            mean_close_ret_end_uncond=("close_ret_end", "mean"),
        )
        .sort_values(["entry_mode", "horizon_m"])
    )

    # metriche condizionate all'ingresso per close/low (e utili anche per open)
    cond_rows = []
    for (mode, h), g in rr.groupby(["entry_mode", "horizon_m"]):
        entered = g["entered"] == True
        n_ent = int(entered.sum())
        if n_ent == 0:
            ph = np.nan
            pm = np.nan
            mr = np.nan
        else:
            ph = float(g.loc[entered, "hit_target"].mean())
            pm = float(g.loc[entered, "miss_target"].mean())
            mr = float(g.loc[entered, "close_ret_end"].mean())
        cond_rows.append(
            {
                "entry_mode": mode,
                "horizon_m": int(h),
                "n_entered": n_ent,
                "p_hit_target_given_enter": ph,
                "p_miss_target_given_enter": pm,
                "mean_close_ret_end_given_enter": mr,
            }
        )

    cond = pd.DataFrame(cond_rows).sort_values(["entry_mode", "horizon_m"])
    out = summary.merge(cond, on=["entry_mode", "horizon_m"], how="left")
    out.to_csv(out_dir / "target_only_summary.csv", index=False)

    # --- Grafici ---
    # 1) Probabilita di entrare
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(data=out, x="horizon_m", y="p_enter", hue="entry_mode", marker="o", ax=ax)
    n_sel = int(len(sdf))
    ax.set_title(f"Probabilita' di entrare in posizione | n_selected={n_sel}")
    ax.set_xlabel("Orizzonte (min)")
    ax.set_ylabel("Probabilita'")
    ax.set_ylim(0, 1)
    for _, r in out.iterrows():
        ax.text(
            r["horizon_m"],
            r["p_enter"] + 0.015,
            f"n={int(r['n_entered'])}",
            fontsize=7,
            alpha=0.8,
            ha="center",
        )
    fig.tight_layout()
    fig.savefig(out_dir / "p_enter_by_horizon.png", dpi=150)
    plt.close(fig)

    # 2) Probabilita hit target dato ingresso
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(data=out, x="horizon_m", y="p_hit_target_given_enter", hue="entry_mode", marker="o", ax=ax)
    ax.set_title(f"Probabilita' hit target (dato ingresso) | n_selected={n_sel}")
    ax.set_xlabel("Orizzonte (min)")
    ax.set_ylabel("Probabilita'")
    ax.set_ylim(0, 1)
    for _, r in out.iterrows():
        if pd.notna(r["p_hit_target_given_enter"]):
            ax.text(
                r["horizon_m"],
                r["p_hit_target_given_enter"] + 0.015,
                f"n={int(r['n_entered'])}",
                fontsize=7,
                alpha=0.8,
                ha="center",
            )
    fig.tight_layout()
    fig.savefig(out_dir / "p_hit_given_enter_by_horizon.png", dpi=150)
    plt.close(fig)

    # 3) Probabilita miss target dato ingresso
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(data=out, x="horizon_m", y="p_miss_target_given_enter", hue="entry_mode", marker="o", ax=ax)
    ax.set_title(f"Probabilita' mancato target (dato ingresso) | n_selected={n_sel}")
    ax.set_xlabel("Orizzonte (min)")
    ax.set_ylabel("Probabilita'")
    ax.set_ylim(0, 1)
    for _, r in out.iterrows():
        if pd.notna(r["p_miss_target_given_enter"]):
            ax.text(
                r["horizon_m"],
                r["p_miss_target_given_enter"] + 0.015,
                f"n={int(r['n_entered'])}",
                fontsize=7,
                alpha=0.8,
                ha="center",
            )
    fig.tight_layout()
    fig.savefig(out_dir / "p_miss_given_enter_by_horizon.png", dpi=150)
    plt.close(fig)

    # 4) Return medio a chiusura finestra
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(data=out, x="horizon_m", y="mean_close_ret_end_given_enter", hue="entry_mode", marker="o", ax=ax)
    ax.axhline(0.0, color="#444", linewidth=1)
    ax.set_title(f"Return medio a fine finestra (dato ingresso) | n_selected={n_sel}")
    ax.set_xlabel("Orizzonte (min)")
    ax.set_ylabel("Return medio")
    for _, r in out.iterrows():
        if pd.notna(r["mean_close_ret_end_given_enter"]):
            ax.text(
                r["horizon_m"],
                r["mean_close_ret_end_given_enter"],
                f" n={int(r['n_entered'])}",
                fontsize=7,
                alpha=0.75,
            )
    fig.tight_layout()
    fig.savefig(out_dir / "mean_close_ret_given_enter_by_horizon.png", dpi=150)
    plt.close(fig)

    # 5) dimensioni campione esplicite
    fig, ax = plt.subplots(figsize=(10, 5))
    sample_df = out.melt(
        id_vars=["entry_mode", "horizon_m"],
        value_vars=["n", "n_entered"],
        var_name="sample_type",
        value_name="count",
    )
    sns.lineplot(data=sample_df, x="horizon_m", y="count", hue="entry_mode", style="sample_type", marker="o", ax=ax)
    ax.set_title("Dimensioni campione per orizzonte")
    ax.set_xlabel("Orizzonte (min)")
    ax.set_ylabel("Numero osservazioni")
    fig.tight_layout()
    fig.savefig(out_dir / "sample_size_by_horizon.png", dpi=150)
    plt.close(fig)

    LOG.info("Analisi completata. Output: %s", out_dir)


if __name__ == "__main__":
    main()
