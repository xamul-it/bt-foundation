#!/usr/bin/env python3
"""One-off backfill: reconstruct the profile_param_versions timeline (D/D-bis)
for the period before the append-only hook (watchtower_record_profile_param.py)
existed, from git history of the STRATEGY_CONFIG files referenced by the
scheduled profiles.

Source of truth for the backfill: for each scheduled profile (discovered
from ~/.config/backtrader/scheduled/*.env -- never a hardcoded list), the
STRATEGY_CONFIG file it points at (e.g.
config-common/scheduled/strategies/overnight-ah-development.env) is git
-tracked in the /home/htpc/backtrader repo. Every commit that touched that
file is a candidate "version boundary": this script checks out the file
content at each commit (oldest first), resolves the ACTIVE (uncommented)
STRAT/STRATARGS assignment at that point in time (including simple shell
`$VAR`/`${VAR}` substitution against other KEY=VALUE lines in the same
snapshot, matching how these files are `source`d by the scheduled-job.sh
wrapper), and feeds it through the same append-only algorithm the daily
hook uses (WatchtowerRepository.record_profile_param_observation), with
source='reconstructed'.

Two consecutive commits that resolve to the identical STRATARGS/extras
(e.g. a docs-only commit) collapse into a single version, exactly like two
consecutive daily "observed_run" calls with the same hash would.

Known, accepted limitations of this reconstruction (do not "fix" these by
guessing -- surface them instead):
- `code_commit`/`core_commit` are left NULL for every reconstructed row:
  which commit of the *strategy* checkout (backtrader-prod or backtrader)
  was actually deployed on a given historical day is not recoverable from
  the config file's own git history. Only observed_run rows (written going
  forward by the daily hook, which resolves the checkout's own commit at
  run time) will have this filled in.
- Granularity is one calendar day: if a profile's STRATEGY_CONFIG changed
  twice on the exact same date with two genuinely different STRATARGS
  values, only the git history can resolve which one is "final" for that
  date -- this script takes the last commit of that date's snapshot, same
  as the daily append-only algorithm would if called twice same-day.
- History only goes back to the file's first git commit. Trading days
  before that are simply not covered -- resolve_params_as_of() will raise
  ProfileParamsUnresolvedError for them, which is correct: don't backfill
  a version we can't evidence.

`--profile` is optional and filters to one profile; with no filter, ALL
profiles discovered in ~/.config/backtrader/scheduled/*.env are processed
-- this script has zero hardcoded knowledge of which profiles exist.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

BT_CORE = Path(__file__).resolve().parent.parent / "bt-core"
if str(BT_CORE) not in sys.path:
    sys.path.insert(0, str(BT_CORE))

from btmain import parse_strategy_args, StrategyArgumentValidationError  # noqa: E402
import watchtower_runtime as wr  # noqa: E402

WORKSPACE_ROOT = Path(
    __import__("os").environ.get("BT_WORKSPACE_ROOT", str(Path.home() / "backtrader"))
).resolve()
SCHEDULED_PROFILES_DIR = Path(
    __import__("os").environ.get(
        "BT_SCHEDULED_PROFILES_DIR", str(Path.home() / ".config" / "backtrader" / "scheduled")
    )
)

_ASSIGN_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$")


def _strip_outer_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def _substitute_vars(value: str, known: dict[str, str]) -> str:
    def repl(match: "re.Match[str]") -> str:
        name = match.group(1) or match.group(2)
        return known.get(name, match.group(0))

    return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)", repl, value)


def resolve_env_snapshot(text: str) -> dict[str, str]:
    """Resolve a simple `KEY=VALUE` / `KEY="...$OTHER..."` env-style file
    (as produced by `source file.env` in bash) top-to-bottom, without any
    shell features beyond variable substitution -- sufficient for the
    overnight-ah-*.env config files used here."""
    resolved: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _ASSIGN_RE.match(line)
        if not match:
            continue
        key, raw_value = match.group(1), match.group(2)
        value = _strip_outer_quotes(raw_value)
        value = _substitute_vars(value, resolved)
        resolved[key] = value
    return resolved


def discover_profiles() -> dict[str, dict[str, str]]:
    """profile -> resolved KEY=VALUE dict of its ~/.config/backtrader/scheduled/{profile}.env.
    No hardcoded profile names: whatever *.env files exist there is the full set."""
    profiles: dict[str, dict[str, str]] = {}
    if not SCHEDULED_PROFILES_DIR.is_dir():
        return profiles
    for env_file in sorted(SCHEDULED_PROFILES_DIR.glob("*.env")):
        resolved = resolve_env_snapshot(env_file.read_text(encoding="utf-8"))
        profile = resolved.get("ROLE") or env_file.stem
        profiles[profile] = resolved
    return profiles


def git_file_commits(repo_root: Path, relpath: str) -> list[tuple[str, date]]:
    """[(commit_hash, commit_date)] for `relpath`, oldest first, following renames."""
    out = subprocess.run(
        ["git", "-C", str(repo_root), "log", "--follow", "--reverse",
         "--format=%H|%cd", "--date=format:%Y-%m-%d", "--", relpath],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    commits: list[tuple[str, date]] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        commit_hash, date_str = line.split("|", 1)
        commits.append((commit_hash, datetime.strptime(date_str, "%Y-%m-%d").date()))
    return commits


def git_show_file(repo_root: Path, commit_hash: str, relpath: str) -> str | None:
    proc = subprocess.run(
        ["git", "-C", str(repo_root), "show", f"{commit_hash}:{relpath}"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def _resolve_commit_snapshots(profile: str, strategy_config: str,
                               commits: list[tuple[str, date]]) -> list[dict[str, Any]]:
    """One entry per commit that has a parseable STRAT+STRATARGS, oldest first."""
    snapshots: list[dict[str, Any]] = []
    for commit_hash, commit_date in commits:
        content = git_show_file(WORKSPACE_ROOT, commit_hash, strategy_config)
        if content is None:
            continue
        snapshot = resolve_env_snapshot(content)
        strategy = snapshot.get("STRAT")
        stratargs_raw = snapshot.get("STRATARGS")
        if not strategy or stratargs_raw is None:
            # Snapshot predates STRATARGS/STRAT existing in this file -- nothing to record yet.
            continue
        try:
            stratargs = parse_strategy_args(stratargs_raw)
        except StrategyArgumentValidationError as exc:
            print(f"[{profile}] {commit_hash[:10]} ({commit_date}): unparsable STRATARGS, skipped: {exc}",
                  file=sys.stderr)
            continue
        extra = {k: v for k, v in snapshot.items() if k not in ("STRAT", "STRATARGS", "STRATARGS_PRECEDENTE")}
        params_hash = wr.profile_param_version_hash(stratargs, None, None, extra)
        snapshots.append({
            "commit": commit_hash[:10], "trading_date": commit_date, "strategy": strategy,
            "stratargs": stratargs, "extra": extra, "params_hash": params_hash,
        })
    return snapshots


def _collapse_into_segments(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse consecutive commits with an identical params_hash into a
    single segment (e.g. a docs-only commit that didn't change the active
    STRATARGS), then stretch each segment's effective_to_date to the day
    before the NEXT segment starts -- not just the day it was first
    observed -- since a backfill has full hindsight of the whole ordered
    sequence and should not under-fill coverage the way the one-day-at-a-
    time daily algorithm would. The last segment stays open
    (effective_to_date=None): it is still the current version as far as
    this profile's config-file history shows."""
    segments: list[dict[str, Any]] = []
    for snap in snapshots:
        if segments and segments[-1]["params_hash"] == snap["params_hash"]:
            continue  # identical version, no new boundary
        segments.append({**snap, "effective_from_date": snap["trading_date"]})
    for i, seg in enumerate(segments):
        if i + 1 < len(segments):
            seg["effective_to_date"] = segments[i + 1]["effective_from_date"] - timedelta(days=1)
        else:
            seg["effective_to_date"] = None
    return segments


def backfill_profile(repo: "wr.WatchtowerRepository", profile: str, profile_env: dict[str, str],
                      dry_run: bool) -> list[dict[str, Any]]:
    strategy_config = profile_env.get("STRATEGY_CONFIG")
    if not strategy_config:
        print(f"[{profile}] no STRATEGY_CONFIG in the profile env -- skipped", file=sys.stderr)
        return []

    commits = git_file_commits(WORKSPACE_ROOT, strategy_config)
    if not commits:
        print(f"[{profile}] {strategy_config} has no git history in {WORKSPACE_ROOT} -- skipped", file=sys.stderr)
        return []

    snapshots = _resolve_commit_snapshots(profile, strategy_config, commits)
    segments = _collapse_into_segments(snapshots)

    results: list[dict[str, Any]] = []
    for seg in segments:
        if dry_run:
            results.append({
                "profile": profile, "commit": seg["commit"], "strategy": seg["strategy"],
                "params_hash": seg["params_hash"],
                "effective_from_date": seg["effective_from_date"].isoformat(),
                "effective_to_date": seg["effective_to_date"].isoformat() if seg["effective_to_date"] else None,
            })
            continue
        outcome = repo.insert_reconstructed_version(
            profile=profile,
            strategy=seg["strategy"],
            stratargs=seg["stratargs"],
            effective_from_date=seg["effective_from_date"],
            effective_to_date=seg["effective_to_date"],
            code_commit=None,
            core_commit=None,
            extra=seg["extra"],
        )
        outcome["commit"] = seg["commit"]
        results.append(outcome)
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", default=None, help="Restrict to one profile (default: all discovered)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be recorded, write nothing")
    parser.add_argument("--db-dsn", default=None)
    args = parser.parse_args(argv)

    profiles = discover_profiles()
    if not profiles:
        print(f"no profile *.env files found under {SCHEDULED_PROFILES_DIR}", file=sys.stderr)
        return 1
    if args.profile and args.profile not in profiles:
        print(f"unknown profile {args.profile!r}, discovered: {sorted(profiles)}", file=sys.stderr)
        return 1

    repo = wr.WatchtowerRepository(dsn=args.db_dsn)
    all_results: dict[str, list[dict[str, Any]]] = {}
    for profile, profile_env in profiles.items():
        if args.profile and profile != args.profile:
            continue
        all_results[profile] = backfill_profile(repo, profile, profile_env, args.dry_run)

    print(json.dumps(all_results, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
