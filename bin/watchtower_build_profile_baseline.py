#!/usr/bin/env python3
"""Manual driver for the Baseline Manager (on-demand, not scheduled).

Runs a long backtest for a scheduled cron profile over an operator-chosen
window and persists it to profile_baselines. Same core as the bt-api
POST /watchtower/cron/<profile>/baselines endpoint.

    bin/watchtower_build_profile_baseline.py \
        --profile development --label hist-2026 \
        --from 2015-01-01 --to 2026-01-01
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

BT_CORE = Path(__file__).resolve().parent.parent / "bt-core"
if str(BT_CORE) not in sys.path:
    sys.path.insert(0, str(BT_CORE))

import profile_baseline as pbl  # noqa: E402
from watchtower_runtime import WatchtowerRepository  # noqa: E402


def _d(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--from", dest="frm", required=True, type=_d)
    ap.add_argument("--to", dest="to", required=True, type=_d)
    ap.add_argument("--as-of", dest="as_of", default=None, type=_d)
    ap.add_argument("--db-dsn", default=None)
    ap.add_argument("--print-cmd", action="store_true", help="resolve and print the backtest command, do not run")
    args = ap.parse_args(argv)

    repo = WatchtowerRepository(args.db_dsn)
    if not repo.available():
        print("Postgres DSN missing", file=sys.stderr)
        return 2

    env = pbl.load_profile_env(args.profile)
    if args.print_cmd:
        ctx = pbl.resolve_baseline_context(repo, args.profile, env, args.as_of or args.to)
        code_root = Path(env.get("CODE_ROOT") or BT_CORE.parent)
        cmd = pbl.build_baseline_backtest_cmd(
            bt_core_python=str(code_root / "bt-core" / ".venv" / "bin" / "python"),
            strategy=ctx["strategy"], ticker=ctx["ticker"], provider=ctx["provider"],
            alpaca_feed=ctx["alpaca_feed"], margin_leverage=ctx["margin_leverage"],
            fromdate=args.frm, todate=args.to, stratargs_str=ctx["stratargs_str"],
            run_id=f"baseline_{args.profile}_PREVIEW",
        )
        print(" ".join(cmd))
        return 0

    row = pbl.compute_profile_baseline(
        repo, profile=args.profile, label=args.label,
        window_start=args.frm, window_end=args.to, as_of_date=args.as_of, profile_env=env,
    )
    print(json.dumps({"id": row["id"], "label": row["label"], "sample_size": row["sample_size"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
