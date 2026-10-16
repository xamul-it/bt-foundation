#!/usr/bin/env python3
"""Execute one already-claimed persistent Watchtower calculation."""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import traceback
import subprocess
from datetime import date
from pathlib import Path

WORKSPACE_ROOT = Path(os.environ.get("BT_WORKSPACE_ROOT", Path(__file__).resolve().parents[1])).resolve()
BT_CORE = WORKSPACE_ROOT / "bt-core"
BIN = WORKSPACE_ROOT / "bin"
for path in (BT_CORE, BIN):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import profile_baseline as pbl  # noqa: E402
import watchtower_backfill_current_params as backfill  # noqa: E402
from watchtower_runtime import WatchtowerRepository  # noqa: E402


ALLOWED_KINDS = {"baseline_build", "backtest_rebuild", "simulation", "market_data_refresh"}


class Heartbeat:
    def __init__(self, repo, job_id: str, worker_token: str):
        self.repo = repo
        self.job_id = job_id
        self.worker_token = worker_token
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name=f"calc-heartbeat-{job_id[:8]}", daemon=True)

    def _run(self):
        while not self.stop_event.wait(15):
            try:
                if not self.repo.heartbeat_calculation_job(
                    self.job_id, self.worker_token, worker_pid=os.getpid(),
                ):
                    return
            except Exception as exc:  # the main calculation owns final reporting
                print(f"heartbeat_error:{exc}", flush=True)

    def __enter__(self):
        self.repo.heartbeat_calculation_job(self.job_id, self.worker_token, worker_pid=os.getpid())
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.stop_event.set()
        self.thread.join(timeout=2)


def run_baseline(repo, row, snapshot):
    repo.update_calculation_progress(
        row["id"], row["worker_token"], progress_done=0, phase="baseline_backtest",
    )
    created = pbl.compute_profile_baseline(
        repo, profile=snapshot["profile"], label=snapshot["label"],
        window_start=date.fromisoformat(snapshot["window_start"]),
        window_end=date.fromisoformat(snapshot["window_end"]),
        profile_env=dict(snapshot["profile_env"]),
    )
    baseline_id = created.get("id") if created else None
    if not baseline_id or not repo.get_profile_baseline(baseline_id):
        raise RuntimeError("baseline_insert_verification_failed")
    return {"baseline_id": baseline_id, "baseline": created}


def run_rebuild(repo, row, snapshot):
    def progress(done, processing_date, outcomes):
        repo.update_calculation_progress(
            row["id"], row["worker_token"], progress_done=done,
            phase="rebuilding_backtest", processing_date=processing_date,
            result={"dates": outcomes, "failed": sum(item["status"] == "failed" for item in outcomes)},
        )
    return backfill.run_rebuild_from_snapshot(
        repo, snapshot, row["id"], progress_callback=progress,
    )


def run_simulation(repo, row, snapshot):
    from simulation_studio import execute_simulation
    repo.update_calculation_progress(row['id'], row['worker_token'], progress_done=0, phase='backtest')
    return execute_simulation(snapshot['configuration'], row['id'])


def run_market_refresh(repo, row, snapshot):
    from simulation_studio import data_inventory
    config = snapshot['configuration']
    ticker = ','.join(config['symbols']) if snapshot.get('benchmark_definition') else config['ticker']
    repo.update_calculation_progress(row['id'], row['worker_token'], progress_done=0, phase='download_market_data')
    command = [str(BT_CORE / '.venv/bin/python'), 'load_tickers.py', '--ticker', ticker,
               '--provider', config['provider'], '--timeframe', 'm' if config['timeframe'] == 'minutes' else 'd',
               '--alpaca-feed', config['alpaca_feed'], '--fromdate', config['fromdate'], '--todate', config['todate']]
    subprocess.run(command, cwd=BT_CORE, check=True, timeout=10800)
    coverage = data_inventory(ticker, config['provider'], config['timeframe'], config['alpaca_feed'])
    if not any(item['available'] for item in coverage):
        raise RuntimeError('Il download non ha prodotto dati locali: consultare il log')
    if snapshot.get('benchmark_definition'):
        from benchmark_studio import publish_buyhold
        repo.update_calculation_progress(row['id'], row['worker_token'], progress_done=0, phase='benchmark_buyhold')
        info = publish_buyhold(config, snapshot['benchmark_definition'], job_id=row['id'])
        return dict(configuration=config, coverage=coverage, benchmark=info,
                    benchmark_definition=snapshot['benchmark_definition'])
    return dict(configuration=config, coverage=coverage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--worker-token", required=True)
    parser.add_argument("--db-dsn")
    args = parser.parse_args()
    repo = WatchtowerRepository(dsn=args.db_dsn)
    row = repo.get_calculation_job(args.job_id)
    if not row or row.get("status") != "running" or row.get("worker_token") != args.worker_token:
        raise SystemExit("job is not claimed by this worker")
    if row.get("kind") not in ALLOWED_KINDS:
        raise SystemExit("job kind is not allowed")
    snapshot = row.get("request_snapshot") or {}
    if snapshot.get("kind") != row["kind"] or snapshot.get("profile") != row["profile"]:
        raise SystemExit("job snapshot identity mismatch")

    print(json.dumps({"job_id": row["id"], "kind": row["kind"], "profile": row["profile"]}), flush=True)
    with Heartbeat(repo, row["id"], args.worker_token):
        try:
            runner = {'baseline_build': run_baseline, 'backtest_rebuild': run_rebuild,
                      'simulation': run_simulation, 'market_data_refresh': run_market_refresh}[row['kind']]
            result = runner(repo, row, snapshot)
            failures = int(result.get("failed") or 0)
            status = "failed" if failures else "completed"
            error = f"{failures}_dates_failed" if failures else None
            repo.finish_calculation_job(
                row["id"], args.worker_token, status=status, result=result, error=error,
            )
            print(json.dumps({"status": status, "result": result}, default=str), flush=True)
            return 1 if failures else 0
        except Exception as exc:
            traceback.print_exc()
            repo.finish_calculation_job(
                row["id"], args.worker_token, status="failed", error=str(exc),
                result={"exception_type": type(exc).__name__},
            )
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
