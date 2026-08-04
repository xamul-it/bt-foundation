#!/usr/bin/env python3
"""Fase 1 dello studio mesi negativi OvernightAH: identifica i mesi negativi
per static-10 e weak_theme_switch da returns.csv gia' generati da run
precedenti (nessun nuovo backtest).

Vedi docs/context/ah_bad_months_study_spec.md per la specifica completa.

Nota dati (verificata prima di scrivere lo script): la directory
`native_switch_3mpos_oos_warmup2023_trade2024` e' stata sovrascritta
(mtime 2026-07-02) da un run full-history 2000-2026 non isolato, diverso da
quanto suggerisce il nome (trades.json parte dal 2000-02-02, non dal 2024).
Per il segmento OOS di weak_theme_switch si usa quindi uno slice per data
(>= SWITCH_OOS_START) di quel file, non il file come se fosse gia' isolato.

Usage:
  bt-core/.venv/bin/python bt-strategy-test/overnight-ah/research/bad_months_identify.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = Path(__file__).resolve().parent / "out" / "bad_months_study"
BT_OUT = ROOT / "bt-core" / "out" / "overnight_ah" / "OvernightAH"

STATIC_SEGMENTS = {
    "train": BT_OUT / "static_bench_train_stable_ah_top10_train" / "returns.csv",
    "validation": BT_OUT / "static_bench_val_stable_ah_top10_validation" / "returns.csv",
    "oos": BT_OUT / "static_bench_oos_stable_ah_top10_oos" / "returns.csv",
}

SWITCH_SEGMENTS = {
    "train": BT_OUT / "native_switch_3mpos_train_warmup2015_trade2016" / "returns.csv",
    "validation": BT_OUT / "native_switch_3mpos_val_warmup2020_trade2021" / "returns.csv",
    "oos": BT_OUT / "native_switch_3mpos_oos_warmup2023_trade2024" / "returns.csv",
}

# Each native_switch_3mpos_* file embeds a warmup period before the real
# trading window (encoded in the directory name: warmupYYYY_tradeYYYY). The
# warmup rows are all-zero (no trades) but their DATES overlap the *previous*
# segment's real trading dates (e.g. validation's 2020 warmup overlaps
# train's real 2020 trading). Verified via boundary check (overlap_days=253
# before this floor was added) — must trim each segment to its real trading
# floor before concatenating, or the warmup zeros silently overwrite real
# returns. The oos floor also fixes the separate contamination issue (see
# "Nota dati" below).
SWITCH_TRADE_FLOOR = {
    "train": "2016-01-01",
    "validation": "2021-01-01",
    "oos": "2024-01-01",
}

# Threshold chosen after inspecting the real monthly-return distribution
# (see summary.md "Soglia scelta" for the reasoning) — not picked a priori.
NEGATIVE_MONTH_THRESHOLD = 0.0


def load_daily_returns(path: Path) -> pd.Series:
    df = pd.read_csv(path, parse_dates=["index"])
    df = df.rename(columns={"index": "date"}).sort_values("date")
    df = df.drop_duplicates("date", keep="last")
    return df.set_index("date")["return"]


def monthly_compounded_returns(daily: pd.Series) -> pd.Series:
    """Compound daily simple returns within each calendar month."""
    return daily.groupby(daily.index.to_period("M")).apply(lambda s: (1.0 + s).prod() - 1.0)


def build_policy_daily_series(
    segments: dict[str, Path], trade_floor: dict[str, str] | None = None
) -> tuple[pd.Series, pd.DataFrame, pd.DataFrame]:
    """Concatenate train->validation->oos daily returns, checking for gaps/overlaps."""
    frames: dict[str, pd.Series] = {}
    for name, path in segments.items():
        s = load_daily_returns(path)
        floor = (trade_floor or {}).get(name)
        if floor is not None:
            s = s[s.index >= pd.Timestamp(floor)]
        frames[name] = s

    order = ["train", "validation", "oos"]
    boundary_rows = []
    for a, b in zip(order, order[1:]):
        sa, sb = frames[a], frames[b]
        overlap = sa.index.intersection(sb.index)
        gap_days = (sb.index.min() - sa.index.max()).days if len(sa) and len(sb) else np.nan
        boundary_rows.append(
            {
                "segment_a": a,
                "segment_b": b,
                "a_end": sa.index.max(),
                "b_start": sb.index.min(),
                "overlap_days": len(overlap),
                "gap_calendar_days": gap_days,
            }
        )

    labeled = pd.concat(
        [frames[k].rename("return").to_frame().assign(segment=k) for k in order]
    ).sort_index()
    labeled = labeled[~labeled.index.duplicated(keep="last")]

    combined = labeled["return"]

    return combined, pd.DataFrame(boundary_rows), labeled


def month_segment_map(labeled_daily: pd.DataFrame) -> pd.Series:
    """For each calendar month, the majority segment label of its daily rows."""
    labeled_daily = labeled_daily.copy()
    labeled_daily["month"] = labeled_daily.index.to_period("M")
    return labeled_daily.groupby("month")["segment"].agg(lambda s: s.value_counts().idxmax())


def negative_months_table(monthly: pd.Series, segment_by_month: pd.Series, threshold: float) -> pd.DataFrame:
    out = monthly.rename("return_frac").to_frame()
    out["return_pct"] = out["return_frac"] * 100
    out["segment"] = segment_by_month
    out["is_negative"] = monthly < threshold
    out.index = out.index.to_timestamp()
    out.index.name = "month"
    return out.drop(columns=["return_frac"]).reset_index()


def describe_distribution(monthly: pd.Series) -> dict:
    return {
        "n_months": int(monthly.shape[0]),
        "mean_pct": float(monthly.mean() * 100),
        "median_pct": float(monthly.median() * 100),
        "std_pct": float(monthly.std() * 100),
        "min_pct": float(monthly.min() * 100),
        "max_pct": float(monthly.max() * 100),
        "p05_pct": float(monthly.quantile(0.05) * 100),
        "p10_pct": float(monthly.quantile(0.10) * 100),
        "p25_pct": float(monthly.quantile(0.25) * 100),
        "pct_below_0": float((monthly < 0).mean() * 100),
        "pct_below_neg2": float((monthly < -0.02).mean() * 100),
        "pct_below_neg5": float((monthly < -0.05).mean() * 100),
    }


def run(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    static_daily, static_boundaries, static_labeled = build_policy_daily_series(STATIC_SEGMENTS)
    switch_daily, switch_boundaries, switch_labeled = build_policy_daily_series(
        SWITCH_SEGMENTS, trade_floor=SWITCH_TRADE_FLOOR
    )

    static_monthly = monthly_compounded_returns(static_daily)
    switch_monthly = monthly_compounded_returns(switch_daily)

    static_seg_by_month = month_segment_map(static_labeled)
    switch_seg_by_month = month_segment_map(switch_labeled)

    static_dist = describe_distribution(static_monthly)
    switch_dist = describe_distribution(switch_monthly)

    static_neg = negative_months_table(static_monthly, static_seg_by_month, NEGATIVE_MONTH_THRESHOLD)
    switch_neg = negative_months_table(switch_monthly, switch_seg_by_month, NEGATIVE_MONTH_THRESHOLD)

    static_daily.rename("return").to_csv(out_dir / "daily_returns_static.csv", header=True)
    switch_daily.rename("return").to_csv(out_dir / "daily_returns_switch.csv", header=True)
    static_neg.to_csv(out_dir / "monthly_returns_static.csv", index=False)
    switch_neg.to_csv(out_dir / "monthly_returns_switch.csv", index=False)
    static_neg[static_neg["is_negative"]].to_csv(out_dir / "negative_months_static.csv", index=False)
    switch_neg[switch_neg["is_negative"]].to_csv(out_dir / "negative_months_switch.csv", index=False)
    static_boundaries.to_csv(out_dir / "static_segment_boundaries.csv", index=False)
    switch_boundaries.to_csv(out_dir / "switch_segment_boundaries.csv", index=False)

    report = []
    report.append("# Fase 1 — Identificazione mesi negativi OvernightAH")
    report.append("")
    report.append("## Nota dati")
    report.append(
        "`native_switch_3mpos_oos_warmup2023_trade2024/returns.csv` e' stato "
        "sovrascritto (mtime 2026-07-02) da un run full-history 2000-2026 non "
        "isolato, diverso da quanto suggerisce il nome della directory "
        "(trades.json parte dal 2000-02-02, non dal 2024). Per il segmento OOS "
        f"di weak_theme_switch si usa quindi uno slice per data (>= "
        f"{SWITCH_TRADE_FLOOR['oos']}) di quel file, non il file come se fosse "
        "gia' isolato con capitale/base proprio."
    )
    report.append(
        "Inoltre ogni file `native_switch_3mpos_*` include un warmup che "
        "precede la finestra di trading reale (nome directory: "
        "`warmupYYYY_tradeYYYY`), con date che si sovrappongono al periodo di "
        "trading reale del segmento precedente (es. warmup 2020 di validation "
        "sovrapposto al trading reale 2020 di train) — se non filtrato, lo "
        "zero del warmup sovrascriverebbe silenziosamente il rendimento reale. "
        f"Trimmata ogni segmento al proprio floor di trading reale: "
        f"{SWITCH_TRADE_FLOOR}."
    )
    report.append("")
    report.append("## Continuita' segmenti (boundary check)")
    report.append("### static-10")
    report.append(static_boundaries.to_markdown(index=False))
    report.append("")
    report.append("### weak_theme_switch")
    report.append(switch_boundaries.to_markdown(index=False))
    report.append("")
    report.append("## Distribuzione rendimenti mensili")
    report.append("### static-10")
    for k, v in static_dist.items():
        report.append(f"- {k}: {v:.3f}" if isinstance(v, float) else f"- {k}: {v}")
    report.append("")
    report.append("### weak_theme_switch")
    for k, v in switch_dist.items():
        report.append(f"- {k}: {v:.3f}" if isinstance(v, float) else f"- {k}: {v}")
    report.append("")
    report.append(f"## Soglia scelta: {NEGATIVE_MONTH_THRESHOLD:.1%}")
    report.append(
        "Confrontando le 3 soglie candidate (0%, -2%, -5%) sulla distribuzione "
        "reale: 0% cattura 24.6% dei mesi su static-10 e 23.8% su "
        "weak_theme_switch, praticamente coincidente col bottom quartile "
        "(p25 static = 0.0%, p25 switch = 0.85%) — coerenza tra la definizione "
        "letterale ('mese negativo' = rendimento sotto zero) e il cross-check "
        "a percentile richiesto dalla spec. -2% cattura circa 16-17% (tra "
        "decile e quartile), -5% circa 8-11% (vicino al bottom decile, p10 "
        "static=-3.65%, p10 switch=-5.29%). Scelto **0%** come soglia primaria "
        "per Fasi 2-5: definizione letterale, coincide col bottom quartile "
        "(campione via via piu' robusto di quello che darebbe -5%, circa "
        "31 mesi su static-10 e 30 su weak_theme_switch invece di ~10), e "
        "lascia -2%/-5% disponibili come soglie piu' severe per stress-test "
        "successivi sugli stessi CSV."
    )
    (out_dir / "summary_fase1.md").write_text("\n".join(report))

    print(f"wrote {out_dir}")
    print("static-10 distribution:")
    for k, v in static_dist.items():
        print(f"  {k}: {v}")
    print("weak_theme_switch distribution:")
    for k, v in switch_dist.items():
        print(f"  {k}: {v}")
    print()
    print("static boundaries:")
    print(static_boundaries.to_string(index=False))
    print("switch boundaries:")
    print(switch_boundaries.to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fase 1: identificazione mesi negativi OvernightAH")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args().out_dir)
