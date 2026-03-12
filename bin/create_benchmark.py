#!/usr/bin/env python3
"""Crea benchmark CSV avviando BuyAndHold e copiando il returns risultante."""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import csv


def parse_args():
    parser = argparse.ArgumentParser(
        description="Create a benchmark by running BuyAndHold and exporting returns.csv"
    )
    parser.add_argument(
        "ticker_list",
        help="Ticker list JSON file name in config/tickers (e.g., NASDAQ_100_US.json)",
    )
    parser.add_argument(
        "--provider",
        default="yahoo",
        help="Data provider (default: yahoo)",
    )
    parser.add_argument(
        "--data",
        default="data",
        help="Data folder under config/ (default: data)",
    )
    parser.add_argument(
        "--timeframe",
        default="daily",
        help="Timeframe (default: daily)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output benchmark CSV path (default: config/benchmark/<listname>.csv)",
    )
    parser.add_argument(
        "--no-run",
        action="store_true",
        help="Skip running btmain.py (assume returns.csv already exists)",
    )
    return parser.parse_args()


def parse_first_date(csv_path):
    try:
        with csv_path.open("r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                return None
            for row in reader:
                if not row:
                    continue
                raw = row[0].strip()
                if not raw:
                    continue
                try:
                    return datetime.fromisoformat(raw)
                except ValueError:
                    try:
                        return datetime.strptime(raw, "%Y-%m-%d")
                    except ValueError:
                        return None
    except Exception:
        return None
    return None


def find_earliest_date(tickers, data_dir):
    earliest = None
    for ticker in tickers:
        csv_path = data_dir / f"{ticker}.csv"
        if not csv_path.exists():
            continue

        first_date = parse_first_date(csv_path)
        if first_date is None:
            continue

        if earliest is None or first_date < earliest:
            earliest = first_date

    return earliest


def find_data_dir(tickers, candidates):
    for candidate in candidates:
        if not candidate.exists():
            continue
        for ticker in tickers:
            if (candidate / f"{ticker}.csv").exists():
                return candidate
    return None


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    tickers_file = repo_root / "config" / "tickers" / args.ticker_list
    if not tickers_file.exists():
        print(f"Ticker list not found: {tickers_file}", file=sys.stderr)
        return 2

    with tickers_file.open("r", encoding="utf-8") as f:
        tickers = json.load(f)

    if not tickers:
        print("Ticker list is empty", file=sys.stderr)
        return 2

    candidate_dirs = [
        repo_root / "config" / args.data / args.timeframe[0:1] / args.provider,
        repo_root / "config" / args.data / args.timeframe / args.provider,
        repo_root / "config" / args.data / args.provider,
        repo_root / "config" / args.data,
    ]
    data_dir = find_data_dir(tickers, candidate_dirs)
    if data_dir is None:
        print("No data directory found for the selected tickers.", file=sys.stderr)
        return 2

    earliest = find_earliest_date(tickers, data_dir)
    if earliest is None:
        print(f"No data found for tickers in {data_dir}", file=sys.stderr)
        return 2

    fromdate = earliest.date().isoformat()

    list_name = Path(args.ticker_list).stem
    output_path = Path(args.output) if args.output else (repo_root / "config" / "benchmark" / f"{list_name}.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not args.no_run:
        cmd = [
            sys.executable,
            str(repo_root / "btmain.py"),
            "--ticker",
            args.ticker_list,
            "--strat",
            "generic.BuyAndHold",
            "--fromdate",
            fromdate,
            "--timeframe",
            args.timeframe,
            "--provider",
            args.provider,
            "--data",
            args.data,
            "--mode",
            "backtest",
        ]

        print("Running:", " ".join(cmd))
        result = subprocess.run(cmd, cwd=str(repo_root))
        if result.returncode != 0:
            return result.returncode

    returns_path = repo_root / "out" / "BuyAndHold" / "returns.csv"
    if not returns_path.exists():
        alt_path = repo_root / "out" / "BuyAndHold" / "result.csv"
        if alt_path.exists():
            returns_path = alt_path
        else:
            print(f"Returns file not found: {returns_path}", file=sys.stderr)
            return 2

    output_path.write_bytes(returns_path.read_bytes())
    print(f"Benchmark written to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
