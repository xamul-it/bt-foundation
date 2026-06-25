#!/usr/bin/env python3
"""Stress OvernightAH Backtrader runs with explicit per-trade round-trip costs."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
RUN_BASE = ROOT / "out" / "overnight_ah" / "OvernightAH"
DEFAULT_OUT = ROOT / "bt-strategy-test" / "overnight-ah" / "research" / "out" / "edge_prediction_study_all_adj" / "cost_stress_trade_edge_all_segments.csv"

DEFAULT_RUNS = {
    "train": {
        "static_top10": "edge_main_train_static_top10_train",
        "combo_c2c6_ah6_top60": "edge_expanded_train_combo_c2c6_ah6_top60_train",
        "combo_top60_spy_dd3m_gt_m15": "edge_gated_grid_train_combo_c2c6_ah6_top60_gate_spy_dd3m_gt_m15_top60_train",
        "combo_top60_spy_dd3m_gt_m10": "edge_gated_grid_train_combo_c2c6_ah6_top60_gate_spy_dd3m_gt_m10_top60_train",
        "ah_total_6m_top20": "edge_main_train_ah_total_6m_top20_train",
    },
    "validation": {
        "static_top10": "edge_main_val_static_top10_validation",
        "combo_c2c6_ah6_top60": "edge_expanded_val_combo_c2c6_ah6_top60_validation",
        "combo_top60_spy_dd3m_gt_m15": "edge_gated_grid_val_combo_c2c6_ah6_top60_gate_spy_dd3m_gt_m15_top60_validation",
        "combo_top60_spy_dd3m_gt_m10": "edge_gated_grid_val_combo_c2c6_ah6_top60_gate_spy_dd3m_gt_m10_top60_validation",
        "ah_total_6m_top20": "edge_main_val_ah_total_6m_top20_validation",
    },
    "oos": {
        "static_top10": "edge_batch_static_top10_oos",
        "combo_c2c6_ah6_top60": "edge_expanded_combo_c2c6_ah6_top60_oos",
        "combo_top60_spy_dd3m_gt_m15": "edge_gated_grid_combo_c2c6_ah6_top60_gate_spy_dd3m_gt_m15_top60_oos",
        "combo_top60_spy_dd3m_gt_m10": "edge_gated_grid_combo_c2c6_ah6_top60_gate_spy_dd3m_gt_m10_top60_oos",
        "ah_total_6m_top20": "edge_batch_ah_total_6m_top20_oos",
    },
}


def read_single_result(path: Path) -> dict:
    data = json.loads(path.read_text())
    if not data:
        raise ValueError(f"empty results file: {path}")
    return next(iter(data.values()))


def summarize_run(segment: str, policy: str, run_id: str, costs_bps: list[int]) -> dict:
    run_dir = RUN_BASE / run_id
    trades_path = run_dir / "trades.json"
    results_path = run_dir / "results.json"
    row = {"segment": segment, "policy": policy, "run_id": run_id}
    if not trades_path.exists() or not results_path.exists():
        return {**row, "status": "missing"}

    trades = pd.read_json(trades_path)
    result = read_single_result(results_path)
    start_value = float(result.get("ptf inizio", 200000.0))
    final_value = start_value + float(result.get("PNL_money", math.nan))
    value = pd.to_numeric(trades["value"], errors="coerce").fillna(0.0)
    pnl = pd.to_numeric(trades["pnlcomm"], errors="coerce").fillna(0.0)
    notional_sum = float(value.sum())
    pnl_sum = float(pnl.sum())

    row.update(
        {
            "status": "ok",
            "final_value": final_value,
            "trades": int(len(trades)),
            "win_ratio_pct": float((pnl > 0).mean() * 100.0) if len(pnl) else math.nan,
            "gross_edge_bps": float(pnl_sum / notional_sum * 10000.0) if notional_sum else math.nan,
            "gross_pnl_sum": pnl_sum,
            "notional_sum": notional_sum,
        }
    )
    for cost_bps in costs_bps:
        net = pnl - value * cost_bps / 10000.0
        row[f"net_edge_{cost_bps}bps_rt"] = float(net.sum() / notional_sum * 10000.0) if notional_sum else math.nan
        row[f"net_win_{cost_bps}bps_rt_pct"] = float((net > 0).mean() * 100.0) if len(net) else math.nan
        row[f"pnl_haircut_{cost_bps}bps_pct"] = float((1.0 - net.sum() / pnl_sum) * 100.0) if pnl_sum else math.nan
    return row


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply explicit round-trip cost stress to saved OvernightAH trades")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--costs-bps", type=int, nargs="+", default=[5, 10, 20, 30])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = []
    for segment, runs in DEFAULT_RUNS.items():
        for policy, run_id in runs.items():
            rows.append(summarize_run(segment, policy, run_id, args.costs_bps))

    summary = pd.DataFrame(rows)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(args.out, index=False)
    ok = summary[summary["status"] == "ok"].copy()
    if not ok.empty:
        cols = [
            "segment",
            "policy",
            "final_value",
            "trades",
            "win_ratio_pct",
            "gross_edge_bps",
        ]
        for cost_bps in args.costs_bps[:2]:
            cols.extend([f"net_edge_{cost_bps}bps_rt", f"net_win_{cost_bps}bps_rt_pct"])
        print(ok[cols].sort_values(["segment", "gross_edge_bps"], ascending=[True, False]).to_string(index=False))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
