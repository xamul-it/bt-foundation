#!/usr/bin/env python3
"""Generic hook: record one observation of a scheduled profile's
STRATARGS/commit into the profile_param_versions append-only timeline
(bt_live_events Postgres DB, tables added in
bt-core/config/sql/live_events_postgres.sql).

Implements the write side of the algorithm described in
docs/context/watchtower_cron_monitoring_brief.md, section D-bis: on each
call it compares a canonical hash of (stratargs, code_commit, core_commit,
extra) against the current open row for `profile`, and either bumps
`last_confirmed_date`, opens the first row, or closes the current row and
opens a new one -- never mutating history in place.

Designed to be called once per profile per trading day, from the `entry`
phase of a scheduled job (today: scripts/scheduled-job.sh, which lives in
the shared backtrader-prod checkout and is NOT modified by this script --
see the finalization report for the exact line to add there instead).

`--profile` / `--strategy` are always free text: this script has zero
hardcoded knowledge of which profiles/strategies exist today. A brand new
profile scheduled tomorrow in a brand new checkout/branch works with this
script unmodified -- see "Vincolo cardine" in the analysis doc.

Usage (typical, called from a shell wrapper that already resolved
STRATARGS/commit for the run about to fire):

    python bin/watchtower_record_profile_param.py \\
        --profile development \\
        --strategy overnight_ah.OvernightAH \\
        --trading-date 2026-08-20 \\
        --stratargs "max_concurrent=5 min_intraday_vol=0.025 auction=True" \\
        --code-commit "$(git -C /path/to/code_root rev-parse HEAD)" \\
        --core-commit "$(git -C /path/to/bt_core rev-parse HEAD)" \\
        --extra '{"ticker_file": "stable_ah_top10.json", "data_provider": "yahoo"}'
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

BT_CORE = Path(__file__).resolve().parent.parent / "bt-core"
if str(BT_CORE) not in sys.path:
    sys.path.insert(0, str(BT_CORE))

from btmain import parse_strategy_args, StrategyArgumentValidationError  # noqa: E402
import watchtower_runtime as wr  # noqa: E402


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--profile", required=True,
        help="Scheduled profile name (free text, e.g. development/mirror/challenger/live/...)",
    )
    parser.add_argument(
        "--strategy", required=True,
        help="Strategy dotted path as passed to --strat (e.g. overnight_ah.OvernightAH)",
    )
    parser.add_argument(
        "--trading-date", required=True, type=_parse_date,
        help="Trading date this observation belongs to (YYYY-MM-DD). Must be "
             ">= the last confirmed date already recorded for this profile "
             "(append-only timeline, not a general upsert).",
    )
    parser.add_argument(
        "--stratargs", default="",
        help="STRATARGS string, same 'key=value key2=value2 ...' format btmain.py's "
             "--stratargs accepts (values parsed with ast.literal_eval).",
    )
    parser.add_argument("--code-commit", default=None, help="git commit of CODE_ROOT for this profile's checkout")
    parser.add_argument("--core-commit", default=None, help="git commit of bt-core for this profile's checkout")
    parser.add_argument(
        "--extra", default="{}",
        help="JSON object with any other identity-relevant knob (ticker file, "
             "data provider, auction flag, margin leverage, ...) that should "
             "count towards 'is this the same version'.",
    )
    parser.add_argument("--source", default="observed_run", choices=("observed_run", "reconstructed"))
    parser.add_argument("--db-dsn", default=None, help="Override DSN (defaults to LIVE_EVENTS_DB_DSN / env file)")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        stratargs = parse_strategy_args(args.stratargs)
    except StrategyArgumentValidationError as exc:
        parser.error(str(exc))

    try:
        extra = json.loads(args.extra) if args.extra else {}
    except json.JSONDecodeError as exc:
        parser.error(f"--extra is not valid JSON: {exc}")
        return 2
    if not isinstance(extra, dict):
        parser.error("--extra must decode to a JSON object")
        return 2

    repo = wr.WatchtowerRepository(dsn=args.db_dsn)
    result = repo.record_profile_param_observation(
        profile=args.profile,
        strategy=args.strategy,
        stratargs=stratargs,
        trading_date=args.trading_date,
        code_commit=args.code_commit,
        core_commit=args.core_commit,
        extra=extra,
        source=args.source,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
