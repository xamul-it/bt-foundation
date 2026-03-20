#!/usr/bin/env python3
"""Crea benchmark CSV avviando BuyAndHold e copiando il returns risultante."""

import argparse
import json
import tempfile
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import csv
import pandas as pd


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
        help="Data folder under config-common/ (default: data)",
    )
    parser.add_argument(
        "--timeframe",
        default="daily",
        help="Timeframe (default: daily)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output benchmark CSV path (default: config-common/benchmark/<listname>.csv)",
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


def parse_first_date_parquet(parquet_path):
    try:
        df = pd.read_parquet(parquet_path)
    except Exception:
        return None

    if df is None or df.empty:
        return None

    for col in ("datetime", "timestamp", "Date", "date"):
        if col in df.columns:
            series = pd.to_datetime(df[col], errors="coerce")
            series = series.dropna()
            if not series.empty:
                return series.min().to_pydatetime()

    if isinstance(df.index, pd.DatetimeIndex) and not df.index.empty:
        return df.index.min().to_pydatetime()
    return None


def first_date_for_ticker(data_dir, ticker):
    csv_path = data_dir / f"{ticker}.csv"
    if csv_path.exists():
        first_date = parse_first_date(csv_path)
        if first_date is not None:
            return first_date

    parquet_path = data_dir / f"{ticker}.parquet"
    if parquet_path.exists():
        return parse_first_date_parquet(parquet_path)

    return None


def find_earliest_date(tickers, data_dir):
    earliest = None
    for ticker in tickers:
        first_date = first_date_for_ticker(data_dir, ticker)
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
            if (candidate / f"{ticker}.csv").exists() or (candidate / f"{ticker}.parquet").exists():
                return candidate
    return None


def prepare_btmain_data_root(source_dir, timeframe, provider):
    """
    Create a temporary data root with btmain-expected layout:
      <tmp_root>/<m|d>/<provider> -> symlink to source_dir
    Returns tmp_root path.
    """
    tf_key = "m" if timeframe.startswith("min") or timeframe == "m" else "d"
    tmp_root = Path(tempfile.mkdtemp(prefix="bt_bench_data_"))
    target = tmp_root / tf_key / provider
    target.parent.mkdir(parents=True, exist_ok=True)
    target.symlink_to(source_dir)
    return tmp_root


def find_latest_returns_file(out_root):
    patterns = [
        "out/generic/BuyAndHold/**/returns.csv",
        "out/generic/BuyAndHold/returns.csv",
        "out/BuyAndHold/**/returns.csv",
        "out/BuyAndHold/returns.csv",
        "out/generic/BuyAndHold/**/result.csv",
        "out/BuyAndHold/**/result.csv",
    ]
    matches = []
    for pattern in patterns:
        matches.extend((out_root / pattern.split("/")[0]).glob("/".join(pattern.split("/")[1:])))
    files = [p for p in matches if p.exists() and p.is_file()]
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    btmain_path = repo_root / "bt-core" / "btmain.py"
    python_path = repo_root / "bt-core" / ".venv" / "bin" / "python"
    if not btmain_path.exists():
        print(f"btmain.py not found: {btmain_path}", file=sys.stderr)
        return 2
    if not python_path.exists():
        print(f"python not found: {python_path}", file=sys.stderr)
        return 2

    tickers_file = repo_root / "config-common" / "tickers" / args.ticker_list
    if not tickers_file.exists():
        tickers_file = repo_root / "config" / "tickers" / args.ticker_list
    if not tickers_file.exists():
        print(f"Ticker list not found: {tickers_file}", file=sys.stderr)
        return 2

    with tickers_file.open("r", encoding="utf-8") as f:
        tickers = json.load(f)

    if not tickers:
        print("Ticker list is empty", file=sys.stderr)
        return 2

    tf = args.timeframe.strip().lower()
    timeframe_variants = [tf, tf[0:1]]
    if tf in ("daily", "d"):
        timeframe_variants += ["minutes", "minute", "m"]
    elif tf in ("minutes", "minute", "m"):
        timeframe_variants += ["daily", "d"]
    timeframe_variants = list(dict.fromkeys([x for x in timeframe_variants if x]))

    cfg_roots = [repo_root / "config-common", repo_root / "config"]
    candidate_dirs = [
        cfg / args.data / t / args.provider
        for cfg in cfg_roots
        for t in timeframe_variants
    ] + [
        cfg / args.data / args.provider
        for cfg in cfg_roots
    ] + [
        cfg / args.data
        for cfg in cfg_roots
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
    default_bench_root = (repo_root / "config-common") if (repo_root / "config-common").exists() else (repo_root / "config")
    output_path = Path(args.output) if args.output else (default_bench_root / "benchmark" / f"{list_name}.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not args.no_run:
        run_timeframe = "minutes" if "minutes" in data_dir.parts else ("daily" if tf in ("daily", "d") else args.timeframe)
        staged_data_root = prepare_btmain_data_root(data_dir, run_timeframe, args.provider)
        cmd = [
            str(python_path),
            str(btmain_path),
            "--ticker",
            args.ticker_list,
            "--strat",
            "generic.BuyAndHold",
            "--fromdate",
            fromdate,
            "--timeframe",
            run_timeframe,
            "--provider",
            args.provider,
            "--benchmark",
            "buyandhold",
            "--data",
            str(staged_data_root),
            "--mode",
            "backtest",
        ]

        print(f"Data directory: {data_dir}")
        print(f"Staging data root: {staged_data_root}")
        print(f"From date: {fromdate}")
        print("Running:", " ".join(cmd))
        result = subprocess.run(cmd, cwd=str(repo_root))
        if result.returncode != 0:
            return result.returncode

    returns_path = find_latest_returns_file(repo_root)
    if returns_path is None:
        print("Returns file not found under out/", file=sys.stderr)
        return 2

    output_path.write_bytes(returns_path.read_bytes())
    print(f"Benchmark written to {output_path} (source: {returns_path})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
