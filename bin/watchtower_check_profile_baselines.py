#!/usr/bin/env python3
"""Run and persist the daily drift check for each compatible profile baseline."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BT_CORE = Path(__file__).resolve().parent.parent / "bt-core"
if str(BT_CORE) not in sys.path:
    sys.path.insert(0, str(BT_CORE))

import profile_baseline as pbl  # noqa: E402
import watchtower_runtime as wr  # noqa: E402


def check_profile(repo: wr.WatchtowerRepository, profile: str, recent_window_days: int) -> dict:
    current = pbl.current_baseline_identity(repo, profile)
    baselines = pbl.annotate_baselines_with_compatibility(repo.list_profile_baselines(profile), current)
    baseline = next((row for row in baselines if row.get("compatibility", {}).get("is_default")), None)
    if not baseline:
        return {"profile": profile, "status": "skipped", "reason": "no_compatible_baseline"}
    verdict = pbl.compute_baseline_drift(repo, baseline, recent_window_days=recent_window_days)
    verdict["compatibility"] = baseline["compatibility"]
    stored = repo.record_profile_baseline_drift_check(
        profile, baseline["id"], verdict["status"], verdict.get("score"), verdict,
    )
    return {"profile": profile, "baseline_id": baseline["id"], "status": verdict["status"], "check_id": stored["id"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", action="append", dest="profiles", help="Profile to check; repeatable")
    parser.add_argument("--recent-window-days", type=int, default=pbl.RECENT_WINDOW_DEFAULT)
    parser.add_argument("--db-dsn", default=None)
    args = parser.parse_args(argv)
    repo = wr.WatchtowerRepository(args.db_dsn)
    if not repo.available():
        parser.error("Postgres DSN missing")
    profiles = args.profiles or repo.list_known_profiles()
    results = []
    failed = False
    for profile in profiles:
        try:
            results.append(check_profile(repo, profile, args.recent_window_days))
        except Exception as exc:  # noqa: BLE001 -- continue with independent profiles
            failed = True
            results.append({"profile": profile, "status": "error", "error": str(exc)})
    print(json.dumps(results, indent=2, default=str))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
