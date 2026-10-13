#!/usr/bin/env python3
"""Bounded parallel sweep for fixed-sizing, idle-cash capital levels."""

from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
STUDY = ROOT / "out/overnight_ah_capital_study_20260909"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--capitals",
        default="10000000,20000000,40000000,80000000,160000000,320000000,640000000",
    )
    parser.add_argument("--strategy", choices=["legacy", "composite"], default="composite")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    queue = []
    for capital_text in args.capitals.split(","):
        capital = int(capital_text)
        name = f"idle_cash_{args.strategy}_{capital}"
        if (STUDY / "idle_cash_cases" / name / "complete.json").exists():
            continue
        command = [
            str(ROOT / "bt-core/.venv/bin/python"),
            str(Path(__file__).with_name("run_idle_cash_case.py")),
            "--name", name,
            "--cash", str(capital),
            "--strategy", args.strategy,
        ]
        queue.append((name, command))

    active: list[tuple[str, subprocess.Popen, object]] = []
    while queue or active:
        for item in active[:]:
            name, process, log = item
            if process.poll() is not None:
                log.close()
                active.remove(item)
                if process.returncode:
                    raise SystemExit(f"{name} failed with code {process.returncode}")
                print(f"completed {name}", flush=True)
        while queue and len(active) < args.workers:
            name, command = queue.pop(0)
            log = (STUDY / f"{name}.log").open("w", encoding="utf-8")
            process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
            active.append((name, process, log))
            print(f"started {name}", flush=True)
        time.sleep(2)


if __name__ == "__main__":
    main()
