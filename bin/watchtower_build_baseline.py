#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path

from watchtower_runtime import WatchtowerRepository, canonicalize_params, strategy_fingerprint, strategy_params_hash


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_run_dirs(root: Path):
    for results_path in root.rglob("results.json"):
        yield results_path.parent


def _extract_context(run_dir: Path):
    results_path = run_dir / "results.json"
    trades_path = run_dir / "trades.json"
    if not results_path.exists() or not trades_path.exists():
        return None
    results = _load_json(results_path)
    first = next(iter(results.values()), None)
    if not first:
        return None
    strategy = first.get("strategy")
    params_data = {}
    for key, value in first.items():
        if key.startswith("input_"):
            params_data[key[6:]] = value
        elif key.startswith("param_"):
            params_data[key[6:]] = value
    fingerprint = strategy_fingerprint(strategy, first.get("input_statVersion") or first.get("param_stat_version"))
    trades = _load_json(trades_path)
    pnl_values = [float(item["pnl_pct"]) for item in trades if item.get("pnl_pct") is not None]
    return {
        "strategy": strategy,
        "fingerprint": fingerprint,
        "params": canonicalize_params(params_data),
        "pnl_values": pnl_values,
        "run_dir": str(run_dir),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Build statistical baselines from backtest output directories")
    parser.add_argument("--db-dsn", default=None, help="Postgres DSN")
    parser.add_argument("--root", required=True, help="Root folder containing run directories with results.json/trades.json")
    return parser.parse_args()


def main():
    args = parse_args()
    repo = WatchtowerRepository(args.db_dsn)
    if not repo.available():
        raise SystemExit("Postgres DSN missing")
    root = Path(args.root).resolve()
    grouped = {}
    for run_dir in _iter_run_dirs(root):
        ctx = _extract_context(run_dir)
        if not ctx or not ctx["pnl_values"] or not ctx["strategy"]:
            continue
        key = (ctx["strategy"], ctx["fingerprint"], strategy_params_hash(ctx["params"]))
        bucket = grouped.setdefault(
            key,
            {
                "strategy": ctx["strategy"],
                "fingerprint": ctx["fingerprint"],
                "params": ctx["params"],
                "pnl_values": [],
                "sources": [],
            },
        )
        bucket["pnl_values"].extend(ctx["pnl_values"])
        bucket["sources"].append(ctx["run_dir"])

    created = []
    for item in grouped.values():
        baseline = repo.upsert_baseline(
            item["strategy"],
            item["fingerprint"],
            item["params"],
            item["pnl_values"],
            source_meta={
                "source_count": len(item["sources"]),
                "sources": item["sources"][:50],
                "builder": "watchtower_build_baseline",
            },
        )
        created.append(baseline)

    print(json.dumps({"root": str(root), "baselines": created}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
