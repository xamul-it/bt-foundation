#!/usr/bin/env python3
"""
Run intraday.HMA on a fixed quarterly month sample and build per-symbol factsheets.

Design goals:
- Same sampled months for every symbol
- Resume safely after interruption/restart
- Optional cache prefetch with load_tickers.py
- Per-symbol QuantStats factsheet on stitched return series (no long date gaps)
"""

from __future__ import annotations

import argparse
import calendar
import json
import subprocess
import sys
import traceback
from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Quarterly sampled HMA batch runner.\n\n"
            "Runs intraday.HMA on a fixed set of months each year across a multi-year\n"
            "window, then stitches results into per-symbol QuantStats factsheets.\n\n"
            "Phases:\n"
            "  prefetch  Download missing minute-bar CSV data via load_tickers.py\n"
            "  run       Execute btmain.py for each (symbol, month) pair\n"
            "  analyze   Stitch returns, compute stats, generate QuantStats HTML\n"
            "  all       Run all three phases in sequence\n\n"
            "Output tree:\n"
            "  <output-root>/\n"
            "    state.json                 Resume checkpoint\n"
            "    monthly_results.csv        Per (symbol, month) metrics\n"
            "    symbol_summary.csv         Aggregated per-symbol ranking\n"
            "    analysis_summary.json      Top-level counts and paths\n"
            "    factsheets/<SYMBOL>/\n"
            "      returns_stitched.csv     Chronologically stitched daily returns\n"
            "      factsheet_quantstats.html\n"
            "    logs/\n"
            "      prefetch/<YYYY-MM>.log\n"
            "      runs/<SYMBOL>_<YYYY-MM>.log"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "\n"
            "  # Only rebuild analysis from existing backtest outputs (fastest):\n"
            "  python3 bt-strategy-test/HMA/run_hma_quarterly_sample.py \\\n"
            "      --phase analyze \\\n"
            "      --years 5 --end-year 2025 \\\n"
            "      --sample-months 1,4,7,10\n"
            "\n"
            "  # Full run (prefetch + backtests + analysis):\n"
            "  python3 bt-strategy-test/HMA/run_hma_quarterly_sample.py \\\n"
            "      --phase all --prefetch \\\n"
            "      --years 5 --end-year 2025 \\\n"
            "      --sample-months 1,4,7,10\n"
            "\n"
            "  # Run backtests only (data already downloaded):\n"
            "  python3 bt-strategy-test/HMA/run_hma_quarterly_sample.py \\\n"
            "      --phase run \\\n"
            "      --years 3 --end-year 2024 \\\n"
            "      --max-runs 10 --sleep-sec 1\n"
            "\n"
            "  # Retry failed tasks and force-rerun existing results:\n"
            "  python3 bt-strategy-test/HMA/run_hma_quarterly_sample.py \\\n"
            "      --phase run --retry-failed --no-skip-existing\n"
            "\n"
            "  # Custom ticker file and strategy args:\n"
            "  python3 bt-strategy-test/HMA/run_hma_quarterly_sample.py \\\n"
            "      --ticker-file config/tickers/SP500.json \\\n"
            "      --stratargs 'period=20 inverted=True exitbar=4' \\\n"
            "      --phase all\n"
        ),
    )

    # --- Ticker / Strategy ---
    grp_strat = p.add_argument_group("Strategy")
    grp_strat.add_argument(
        "--ticker-file",
        default="config/tickers/NASDAQ_100_US.json",
        metavar="PATH",
        help="JSON file with list of ticker symbols (default: NASDAQ_100_US.json)",
    )
    grp_strat.add_argument(
        "--strategy",
        default="intraday.HMA",
        metavar="MODULE.CLASS",
        help="Strategy to backtest (default: intraday.HMA)",
    )
    grp_strat.add_argument(
        "--stratargs",
        default="period=16 inverted=True exitbar=6",
        metavar="'KEY=VAL ...'",
        help="Space-separated strategy parameters (default: 'period=16 inverted=True exitbar=6')",
    )
    grp_strat.add_argument(
        "--provider",
        default="alpaca",
        choices=["alpaca", "yahoo"],
        help="Data provider (default: alpaca)",
    )
    grp_strat.add_argument(
        "--timeframe",
        default="minutes",
        choices=["minutes", "daily"],
        help="Bar timeframe (default: minutes)",
    )
    grp_strat.add_argument(
        "--mode",
        default="backtest",
        choices=["backtest", "paper", "shadow"],
        help="Execution mode (default: backtest)",
    )
    grp_strat.add_argument(
        "--commission",
        default="none",
        help="Commission schema: none | fineco | fineco_low (default: none)",
    )
    grp_strat.add_argument(
        "--cash",
        type=float,
        default=10000,
        metavar="AMOUNT",
        help="Starting cash for each backtest (default: 10000)",
    )
    grp_strat.add_argument(
        "--amount",
        type=float,
        default=10000,
        metavar="AMOUNT",
        help="Per-trade order size (default: 10000)",
    )

    # --- Sampling window ---
    grp_sample = p.add_argument_group("Sampling window")
    grp_sample.add_argument(
        "--years",
        type=int,
        default=5,
        metavar="N",
        help="Number of years in the sample window (default: 5)",
    )
    grp_sample.add_argument(
        "--end-year",
        type=int,
        default=date.today().year - 1,
        metavar="YYYY",
        help="Last year included in sample (default: previous calendar year)",
    )
    grp_sample.add_argument(
        "--sample-months",
        default="1,4,7,10",
        metavar="M,M,...",
        help=(
            "Comma-separated month numbers to backtest each year "
            "(default: '1,4,7,10' = Jan/Apr/Jul/Oct, one per quarter)"
        ),
    )

    # --- Phase control ---
    grp_phase = p.add_argument_group("Phase control")
    grp_phase.add_argument(
        "--phase",
        choices=["all", "prefetch", "run", "analyze"],
        default="all",
        help=(
            "Which phase(s) to execute: "
            "prefetch=download data, run=backtests, analyze=stats+HTML, all=all three "
            "(default: all)"
        ),
    )
    grp_phase.add_argument(
        "--prefetch",
        action="store_true",
        default=False,
        help="Enable data prefetch when --phase all (no-op if --phase prefetch)",
    )

    # --- Execution options ---
    grp_exec = p.add_argument_group("Execution options")
    grp_exec.add_argument(
        "--python-bin",
        default="python3",
        metavar="BIN",
        help="Python interpreter to use for sub-processes (default: python3)",
    )
    grp_exec.add_argument(
        "--load-script",
        default="./load_tickers.py",
        metavar="PATH",
        help="Path to load_tickers.py used for prefetch (default: ./load_tickers.py)",
    )
    grp_exec.add_argument(
        "--output-root",
        default="out/intraday/HMA_quarterly_sample",
        metavar="PATH",
        help="Root directory for all outputs (default: out/intraday/HMA_quarterly_sample)",
    )
    grp_exec.add_argument(
        "--run-id-prefix",
        default="quarterly_sample",
        metavar="PREFIX",
        help="Prefix for btmain --id argument (default: quarterly_sample)",
    )
    grp_exec.add_argument(
        "--state-file",
        default=None,
        metavar="PATH",
        help="Override path for the JSON resume checkpoint (default: <output-root>/state.json)",
    )
    grp_exec.add_argument(
        "--max-runs",
        type=int,
        default=0,
        metavar="N",
        help="Stop after N backtest runs per session; 0 = no limit (default: 0)",
    )
    grp_exec.add_argument(
        "--sleep-sec",
        type=float,
        default=0.0,
        metavar="SECS",
        help="Pause between backtest sub-processes in seconds (default: 0)",
    )
    grp_exec.add_argument(
        "--retry-failed",
        action="store_true",
        default=False,
        help="Re-run tasks that previously failed (default: skip them)",
    )
    grp_exec.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip (symbol, period) pairs whose returns.csv already exists (default: on)",
    )
    grp_exec.add_argument(
        "--no-skip-existing",
        action="store_false",
        dest="skip_existing",
        help="Force re-run even if returns.csv already exists",
    )
    grp_exec.add_argument(
        "--debug",
        action="store_true",
        default=True,
        help="Pass --debug to btmain.py sub-processes (default: on)",
    )
    grp_exec.add_argument(
        "--no-debug",
        action="store_false",
        dest="debug",
        help="Suppress --debug flag in btmain.py sub-processes",
    )
    return p.parse_args()


@dataclass
class Period:
    key: str
    fromdate: str
    todate: str


def month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def build_periods(end_year: int, years: int, sample_months: List[int]) -> List[Period]:
    start_year = end_year - years + 1
    periods: List[Period] = []
    for year in range(start_year, end_year + 1):
        for month in sample_months:
            start = date(year, month, 1)
            end = month_end(year, month)
            periods.append(
                Period(
                    key=f"{year}-{month:02d}",
                    fromdate=start.isoformat(),
                    todate=end.isoformat(),
                )
            )
    periods.sort(key=lambda x: x.key)
    return periods


def load_symbols(repo_root: Path, ticker_file: str) -> Tuple[List[str], Path]:
    candidate = Path(ticker_file)
    if not candidate.is_absolute():
        candidate = (repo_root / candidate).resolve()
    if not candidate.exists():
        candidate = (repo_root / "config" / "tickers" / ticker_file).resolve()
    if not candidate.exists():
        raise FileNotFoundError(f"Ticker file not found: {ticker_file}")

    with candidate.open("r", encoding="utf-8") as f:
        symbols = json.load(f)
    symbols = [str(s).strip() for s in symbols if str(s).strip()]
    if not symbols:
        raise ValueError(f"No symbols in {candidate}")
    return symbols, candidate


def default_state_path(output_root: Path) -> Path:
    return output_root / "state.json"


def load_state(path: Path) -> Dict:
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            state = json.load(f)
    else:
        state = {"prefetch": {}, "tasks": {}}
    # Interrupted tasks go back to pending
    for task in state.get("tasks", {}).values():
        if task.get("status") == "running":
            task["status"] = "pending"
    for task in state.get("prefetch", {}).values():
        if task.get("status") == "running":
            task["status"] = "pending"
    return state


def save_state(path: Path, state: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    tmp.replace(path)


def run_command(cmd: List[str], cwd: Path, log_file: Path) -> int:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("w", encoding="utf-8") as log:
        log.write("COMMAND:\n")
        log.write(" ".join(cmd) + "\n\n")
        log.flush()
        proc = subprocess.run(cmd, cwd=str(cwd), stdout=log, stderr=subprocess.STDOUT, text=True)
    return proc.returncode


def run_prefetch(
    args: argparse.Namespace,
    repo_root: Path,
    ticker_file_path: Path,
    symbols: List[str],
    periods: List[Period],
    state: Dict,
    state_path: Path,
) -> None:
    ticker_rel = None
    cfg_dir = (repo_root / "config" / "tickers").resolve()
    if cfg_dir in ticker_file_path.parents:
        ticker_rel = ticker_file_path.name

    for period in periods:
        task = state["prefetch"].setdefault(
            period.key,
            {
                "status": "pending",
                "attempts": 0,
                "fromdate": period.fromdate,
                "todate": period.todate,
            },
        )
        if task["status"] == "done":
            continue
        task["status"] = "running"
        task["attempts"] += 1
        save_state(state_path, state)

        ticker_arg = ticker_rel if ticker_rel else ",".join(symbols)
        cmd = [
            args.python_bin,
            args.load_script,
            "--provider=alpaca",
            "--timeframe=m",
            f"--ticker={ticker_arg}",
            f"--fromdate={period.fromdate}",
            f"--todate={period.todate}",
            "--incremental",
        ]
        log_file = repo_root / args.output_root / "logs" / "prefetch" / f"{period.key}.log"
        rc = run_command(cmd, repo_root, log_file)
        task["status"] = "done" if rc == 0 else "failed"
        task["return_code"] = rc
        task["log"] = str(log_file)
        save_state(state_path, state)


def make_run_id(prefix: str, symbol: str, period_key: str) -> str:
    return f"{prefix}/{symbol}/{period_key.replace('-', '')}"


def run_backtests(
    args: argparse.Namespace,
    repo_root: Path,
    symbols: List[str],
    periods: List[Period],
    state: Dict,
    state_path: Path,
) -> None:
    executed = 0
    out_root = repo_root / "out" / "intraday" / "HMA"
    for symbol in symbols:
        for period in periods:
            task_id = f"{symbol}|{period.key}"
            run_id = make_run_id(args.run_id_prefix, symbol, period.key)
            run_dir = out_root / run_id
            returns_file = run_dir / "returns.csv"

            task = state["tasks"].setdefault(
                task_id,
                {
                    "symbol": symbol,
                    "period": asdict(period),
                    "status": "pending",
                    "attempts": 0,
                    "run_id": run_id,
                    "run_dir": str(run_dir),
                },
            )
            if task["status"] == "done":
                if args.skip_existing:
                    continue
                # Force rerun when --no-skip-existing is used
                task["status"] = "pending"
            if task["status"] == "failed" and not args.retry_failed:
                continue
            if args.skip_existing and returns_file.exists():
                task["status"] = "done"
                task["returns_file"] = str(returns_file)
                save_state(state_path, state)
                continue
            if args.max_runs and executed >= args.max_runs:
                return

            task["status"] = "running"
            task["attempts"] += 1
            save_state(state_path, state)

            cmd = [
                args.python_bin,
                "./btmain.py",
                f"--strat={args.strategy}",
                f"--ticker={symbol}",
                f"--timeframe={args.timeframe}",
                f"--provider={args.provider}",
                f"--mode={args.mode}",
                f"--stratargs={args.stratargs}",
                f"--fromdate={period.fromdate}",
                f"--todate={period.todate}",
                f"--commission={args.commission}",
                f"--cash={args.cash}",
                f"--amount={args.amount}",
                f"--id={run_id}",
            ]
            if args.debug:
                cmd.append("--debug")

            log_file = repo_root / args.output_root / "logs" / "runs" / f"{symbol}_{period.key}.log"
            rc = run_command(cmd, repo_root, log_file)
            task["return_code"] = rc
            task["log"] = str(log_file)
            if rc == 0 and returns_file.exists():
                task["status"] = "done"
                task["returns_file"] = str(returns_file)
            else:
                task["status"] = "failed"
            save_state(state_path, state)
            executed += 1
            if args.sleep_sec > 0:
                import time
                time.sleep(args.sleep_sec)


def build_matrix_html(monthly_df, title: str) -> str:
    """Return an HTML page with a period × symbol heatmap of monthly returns.

    Columns: Period | sym... | + | - | Eq.W%
    Footer rows: AVG (per-symbol), % pos months, Compound (geometric across periods)
    Filter bar: avg range, % pos months, symbol search — all stats update live.
    """
    import pandas as pd

    pivot = monthly_df.pivot(index="period", columns="symbol", values="total_return")
    pivot = pivot.sort_index()
    symbols = list(pivot.columns)
    periods = list(pivot.index)
    n_periods = len(periods)
    n_symbols = len(symbols)

    # --- per-symbol stats -----------------------------------------------
    col_avg = pivot.mean(skipna=True)
    col_pos = (pivot > 0).sum(skipna=True) / pivot.count()

    # --- Python-side color helpers (used for static cells only) ---------
    def _bg(val: float, clamp: float = 0.10) -> str:
        if pd.isna(val):
            return "#2a2a2a"
        intensity = min(abs(val) / clamp, 1.0)
        if val >= 0:
            r = int(20 + (1 - intensity) * 40)
            g = int(90 + intensity * 120)
            b = int(20 + (1 - intensity) * 40)
        else:
            r = int(90 + intensity * 120)
            g = int(20 + (1 - intensity) * 40)
            b = int(20 + (1 - intensity) * 40)
        return f"#{r:02x}{g:02x}{b:02x}"

    def _fg(val: float) -> str:
        if pd.isna(val):
            return "#666"
        return "#fff" if abs(val) > 0.03 else "#ccc"

    # --- header ---------------------------------------------------------
    # Symbol columns start at data-col=1; stats cols have no data-col.
    header_cells = "<th>Period</th>\n"
    for i, sym in enumerate(symbols):
        avg_v = col_avg[sym]
        pos_v = col_pos[sym]
        avg_pct = f"{avg_v*100:+.2f}%" if not pd.isna(avg_v) else "—"
        pos_pct = f"{pos_v*100:.0f}%" if not pd.isna(pos_v) else "—"
        avg_raw = round(float(avg_v) * 100, 4) if not pd.isna(avg_v) else "null"
        pos_raw = round(float(pos_v) * 100, 1) if not pd.isna(pos_v) else "null"
        header_cells += (
            f'<th data-col="{i+1}" data-avg="{avg_raw}" data-pos="{pos_raw}"'
            f' title="AVG {avg_pct} | +months {pos_pct}">{sym}</th>\n'
        )
    header_cells += '<th class="stats-th" title="Symbols with positive return this period">+</th>\n'
    header_cells += '<th class="stats-th" title="Symbols with negative return this period">−</th>\n'
    header_cells += '<th class="stats-th" title="Equal-weight mean return (visible symbols)">Eq.W%</th>\n'

    # --- data rows ------------------------------------------------------
    rows_html = []
    for period in periods:
        row = pivot.loc[period]
        cells = f'<td class="period-label">{period}</td>\n'
        for i, sym in enumerate(symbols):
            val = row[sym]
            if pd.isna(val):
                cells += f'<td data-col="{i+1}" class="na">—</td>\n'
            else:
                bg = _bg(val)
                fg = _fg(val)
                pct = f"{val*100:+.1f}%"
                val_raw = round(float(val), 6)
                cells += (
                    f'<td data-col="{i+1}" data-val="{val_raw}"'
                    f' style="background:{bg};color:{fg}"'
                    f' title="{sym} {period}: {pct}">{pct}</td>\n'
                )
        # stats cells — populated by JS
        cells += '<td class="stats-cell s-pos">—</td>\n'
        cells += '<td class="stats-cell s-neg">—</td>\n'
        cells += '<td class="stats-cell s-eqw">—</td>\n'
        rows_html.append(f'<tr class="data-row">{cells}</tr>')

    # --- AVG footer row (per-symbol averages) ---------------------------
    avg_cells = '<td class="period-label avg-label">▶ AVG</td>\n'
    for i, sym in enumerate(symbols):
        avg_v = col_avg[sym]
        if pd.isna(avg_v):
            avg_cells += f'<td data-col="{i+1}" class="na">—</td>\n'
        else:
            bg = _bg(avg_v)
            fg = _fg(avg_v)
            val_raw = round(float(avg_v), 6)
            avg_cells += (
                f'<td data-col="{i+1}" data-val="{val_raw}"'
                f' style="background:{bg};color:{fg}">'
                f'{avg_v*100:+.2f}%</td>\n'
            )
    avg_cells += '<td class="stats-cell s-pos avg-stats">—</td>\n'
    avg_cells += '<td class="stats-cell s-neg avg-stats">—</td>\n'
    avg_cells += '<td class="stats-cell s-eqw avg-stats">—</td>\n'
    rows_html.append(f'<tr class="footer-row avg-row">{avg_cells}</tr>')

    # --- % pos months footer row ----------------------------------------
    pos_cells = '<td class="period-label pct-label">% pos</td>\n'
    for i, sym in enumerate(symbols):
        pos_v = col_pos[sym]
        if pd.isna(pos_v):
            pos_cells += f'<td data-col="{i+1}" class="na">—</td>\n'
        else:
            bg = _bg(pos_v - 0.5, clamp=0.5)   # centred on 50 %
            pos_cells += (
                f'<td data-col="{i+1}" style="background:{bg};color:#ddd">'
                f'{pos_v*100:.0f}%</td>\n'
            )
    pos_cells += '<td class="stats-cell" colspan="3"></td>\n'
    rows_html.append(f'<tr class="footer-row pct-row">{pos_cells}</tr>')

    # --- Compound footer row (computed entirely by JS) ------------------
    compound_cells = '<td class="period-label compound-label">⬡ Compound</td>\n'
    compound_cells += f'<td class="stats-cell" colspan="{n_symbols}"></td>\n'
    compound_cells += '<td class="stats-cell" colspan="2"></td>\n'
    compound_cells += '<td class="stats-cell s-compound">—</td>\n'
    rows_html.append(f'<tr class="footer-row compound-row">{compound_cells}</tr>')

    # ====================================================================
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>{title}</title>
<style>
*, *::before, *::after {{ box-sizing:border-box; }}
body {{
  background:#161616; color:#ddd; font-family:monospace;
  padding:14px; margin:0;
}}
h1 {{ font-size:.95em; margin:0 0 10px; color:#bbb; font-weight:normal; }}

/* ── filter bar ─────────────────────────────────────────────── */
#filters {{
  display:flex; flex-wrap:wrap; gap:8px 14px; align-items:center;
  background:#1e1e1e; border:1px solid #333; border-radius:6px;
  padding:9px 14px; margin-bottom:10px;
}}
#filters label {{ font-size:.73em; color:#777; white-space:nowrap; }}
#filters input {{
  background:#272727; border:1px solid #3e3e3e; color:#ccc;
  border-radius:3px; padding:3px 6px; font-family:monospace;
  font-size:.78em; width:76px;
}}
#filters input.wide {{ width:130px; }}
#filters input:focus {{ outline:1px solid #555; }}
.sep {{ color:#383838; font-size:.8em; }}
#reset {{
  background:#2c2c2c; border:1px solid #484848; color:#999;
  border-radius:3px; padding:3px 10px; font-size:.75em; cursor:pointer;
}}
#reset:hover {{ background:#3a3a3a; color:#eee; }}
#meta {{ font-size:.7em; color:#484848; margin:0 0 8px; }}
#meta span {{ color:#666; }}

/* ── table ───────────────────────────────────────────────────── */
.wrapper {{ overflow:auto; max-height:84vh; }}
table {{ border-collapse:collapse; white-space:nowrap; }}

/* header */
th {{
  background:#222; color:#888; padding:4px 8px; font-size:.7em;
  border:1px solid #333; position:sticky; top:0; z-index:2;
}}
th[data-col] {{ border-bottom:3px solid #444; }}
.stats-th {{
  background:#1c2028; color:#7aaccc; border-left:2px solid #2a3540;
  font-size:.7em;
}}

/* data cells */
td {{
  padding:3px 7px; font-size:.73em; border:1px solid #232323;
  text-align:right;
}}
.period-label {{
  color:#888; text-align:left; background:#1a1a1a;
  position:sticky; left:0; z-index:1; min-width:72px;
  border-right:2px solid #333;
}}
.na {{ color:#383838; text-align:center; background:#181818; }}

/* stats columns */
.stats-cell {{
  background:#1c2028; color:#9dbbd0; border-left:2px solid #2a3540;
  font-weight:bold;
}}
.s-pos {{ color:#6abf69; }}
.s-neg {{ color:#e06060; }}
.s-eqw {{ color:#d0d0d0; }}

/* ── AVG row ─────────────────────────────────────────────────── */
.avg-row td {{
  border-top:3px solid #505050 !important;
  background:#1c1c1c !important;
  font-size:.8em !important;
  font-weight:bold;
}}
.avg-label {{
  color:#e8e8e8 !important;
  background:#202020 !important;
  letter-spacing:.05em;
}}
.avg-stats {{ font-size:.8em !important; }}

/* ── % pos row ───────────────────────────────────────────────── */
.pct-row td {{ background:#181818; border-top:1px solid #2e2e2e; }}
.pct-label {{ color:#777 !important; font-style:italic; }}

/* ── Compound row ────────────────────────────────────────────── */
.compound-row td {{
  border-top:3px solid #5a5a3a !important;
  background:#1e1e16 !important;
}}
.compound-label {{
  color:#cccc88 !important;
  background:#1c1c14 !important;
  letter-spacing:.04em;
}}
.s-compound {{
  color:#ffff99 !important;
  background:#252510 !important;
  font-size:.85em !important;
  font-weight:bold;
  border-left:2px solid #6a6a30 !important;
}}

.col-hidden {{ display:none !important; }}
</style>
</head>
<body>
<h1>{title}</h1>

<div id="filters">
  <label>AVG %
    <input id="fMin" type="number" step="0.1" placeholder="min"
           title="Mostra solo simboli con avg mensile ≥ questo valore (%)">
  </label>
  <span class="sep">—</span>
  <label>
    <input id="fMax" type="number" step="0.1" placeholder="max"
           title="Mostra solo simboli con avg mensile ≤ questo valore (%)">
  </label>
  <span class="sep">|</span>
  <label>% pos mesi ≥
    <input id="fPos" type="number" step="1" min="0" max="100" placeholder="0"
           title="Mostra solo simboli con almeno X% di mesi positivi">
  </label>
  <span class="sep">|</span>
  <label>Simbolo
    <input id="fSym" type="text" class="wide" placeholder="AAPL, TSLA …"
           title="Filtra per nome simbolo (virgola-separati)">
  </label>
  <button id="reset">Reset</button>
</div>

<p id="meta">
  {n_periods} periodi &nbsp;×&nbsp;
  <span id="visCount">{n_symbols}</span>/{n_symbols} simboli visibili
  &nbsp;|&nbsp; scala colori: rosso = −10% &nbsp; verde = +10%
  &nbsp;|&nbsp; riga % pos centrata su 50%
</p>

<div class="wrapper">
<table id="matrix">
<thead><tr id="headerRow">{header_cells}</tr></thead>
<tbody>
{"".join(rows_html)}
</tbody>
</table>
</div>

<script>
(function () {{
  var fMin     = document.getElementById('fMin');
  var fMax     = document.getElementById('fMax');
  var fPos     = document.getElementById('fPos');
  var fSym     = document.getElementById('fSym');
  var visCount = document.getElementById('visCount');
  var total    = {n_symbols};

  /* ── colour helper (mirrors Python _bg) ─────────────────────── */
  function bgColor(val, clamp) {{
    clamp = clamp || 0.10;
    if (isNaN(val)) return '#2a2a2a';
    var intensity = Math.min(Math.abs(val) / clamp, 1.0);
    var r, g, b;
    if (val >= 0) {{
      r = Math.round(20 + (1 - intensity) * 40);
      g = Math.round(90 + intensity * 120);
      b = Math.round(20 + (1 - intensity) * 40);
    }} else {{
      r = Math.round(90 + intensity * 120);
      g = Math.round(20 + (1 - intensity) * 40);
      b = Math.round(20 + (1 - intensity) * 40);
    }}
    function h(n) {{ return n.toString(16).padStart(2,'0'); }}
    return '#' + h(r) + h(g) + h(b);
  }}
  function fgColor(val) {{
    return (isNaN(val) || Math.abs(val) > 0.03) ? '#fff' : '#bbb';
  }}
  function fmtPct(val, decimals) {{
    decimals = decimals === undefined ? 1 : decimals;
    var s = (val * 100).toFixed(decimals);
    return (val >= 0 ? '+' : '') + s + '%';
  }}

  /* ── update a stats cell ─────────────────────────────────────── */
  function setEqw(td, val) {{
    if (isNaN(val)) {{ td.textContent = '—'; td.style.background = ''; td.style.color = '#9dbbd0'; return; }}
    td.textContent = fmtPct(val, 2);
    td.style.background = bgColor(val);
    td.style.color = fgColor(val);
  }}

  /* ── main filter + stats recompute ──────────────────────────── */
  function applyFilters() {{
    var minV = fMin.value !== '' ? parseFloat(fMin.value) : -Infinity;
    var maxV = fMax.value !== '' ? parseFloat(fMax.value) : +Infinity;
    var posV = fPos.value !== '' ? parseFloat(fPos.value) : 0;
    var syms = fSym.value.trim()
      ? fSym.value.split(',').map(function(s){{ return s.trim().toUpperCase(); }}).filter(Boolean)
      : [];

    /* 1. show/hide symbol columns */
    var ths = document.querySelectorAll('#headerRow th[data-col]');
    var visible = 0;
    var visibleCols = [];
    ths.forEach(function(th) {{
      var col = th.getAttribute('data-col');
      var avg = parseFloat(th.getAttribute('data-avg'));
      var pos = parseFloat(th.getAttribute('data-pos'));
      var sym = th.textContent.trim().toUpperCase();

      var show = true;
      if (!isNaN(avg) && (avg < minV || avg > maxV)) show = false;
      if (!isNaN(pos) && pos < posV)                  show = false;
      if (syms.length && syms.indexOf(sym) === -1)    show = false;

      th.classList.toggle('col-hidden', !show);
      document.querySelectorAll('td[data-col="' + col + '"]').forEach(function(td) {{
        td.classList.toggle('col-hidden', !show);
      }});
      if (show) {{ visible++; visibleCols.push(col); }}
    }});
    visCount.textContent = visible;

    /* 2. recompute per-row stats for data rows */
    var eqwPerRow = [];   // equal-weight return per period (for compound)

    document.querySelectorAll('tr.data-row').forEach(function(tr) {{
      var vals = [];
      visibleCols.forEach(function(col) {{
        var td = tr.querySelector('td[data-col="' + col + '"]');
        if (td && td.hasAttribute('data-val')) {{
          var v = parseFloat(td.getAttribute('data-val'));
          if (!isNaN(v)) vals.push(v);
        }}
      }});

      var pos = vals.filter(function(v) {{ return v > 0; }}).length;
      var neg = vals.filter(function(v) {{ return v < 0; }}).length;
      var eqw = vals.length ? vals.reduce(function(a,b){{return a+b;}},0) / vals.length : NaN;
      eqwPerRow.push(eqw);

      var cells = tr.querySelectorAll('.stats-cell');
      cells[0].textContent = vals.length ? '+' + pos : '—';
      cells[1].textContent = vals.length ? '−' + neg : '—';
      setEqw(cells[2], isNaN(eqw) ? NaN : eqw);
    }});

    /* 3. recompute AVG row stats */
    var avgRow = document.querySelector('tr.avg-row');
    if (avgRow) {{
      var avgVals = [];
      visibleCols.forEach(function(col) {{
        var td = avgRow.querySelector('td[data-col="' + col + '"]');
        if (td && td.hasAttribute('data-val')) {{
          var v = parseFloat(td.getAttribute('data-val'));
          if (!isNaN(v)) avgVals.push(v);
        }}
      }});
      var aPos = avgVals.filter(function(v){{ return v > 0; }}).length;
      var aNeg = avgVals.filter(function(v){{ return v < 0; }}).length;
      var aEqw = avgVals.length
        ? avgVals.reduce(function(a,b){{return a+b;}},0) / avgVals.length : NaN;
      var ac = avgRow.querySelectorAll('.stats-cell');
      ac[0].textContent = avgVals.length ? '+' + aPos : '—';
      ac[1].textContent = avgVals.length ? '−' + aNeg : '—';
      setEqw(ac[2], isNaN(aEqw) ? NaN : aEqw);
    }}

    /* 4. compound across periods: prod(1 + eqw_j) - 1 */
    var compound = NaN;
    var valid = eqwPerRow.filter(function(v){{ return !isNaN(v); }});
    if (valid.length) {{
      compound = valid.reduce(function(acc, v) {{ return acc * (1 + v); }}, 1.0) - 1.0;
    }}
    var cCell = document.querySelector('.s-compound');
    if (cCell) {{
      if (isNaN(compound)) {{
        cCell.textContent = '—';
        cCell.style.background = '';
      }} else {{
        cCell.textContent = fmtPct(compound, 2);
        cCell.style.color = compound >= 0 ? '#ffff88' : '#ff9999';
      }}
    }}
  }}

  [fMin, fMax, fPos, fSym].forEach(function(el) {{
    el.addEventListener('input', applyFilters);
  }});
  document.getElementById('reset').addEventListener('click', function() {{
    fMin.value=''; fMax.value=''; fPos.value=''; fSym.value='';
    applyFilters();
  }});

  applyFilters();
}})();
</script>
</body>
</html>"""
    return html


def calc_max_drawdown(returns) -> float:
    equity = (1 + returns).cumprod()
    dd = equity / equity.cummax() - 1.0
    return float(dd.min()) if len(dd) else 0.0


def analyze_results(args: argparse.Namespace, repo_root: Path, state: Dict, periods: List[Period]) -> None:
    import pandas as pd

    qs_available = True
    try:
        import quantstats as qs
    except Exception:
        qs_available = False
        qs = None

    output_root = (repo_root / args.output_root).resolve()
    factsheets_dir = output_root / "factsheets"
    factsheets_dir.mkdir(parents=True, exist_ok=True)

    period_order = {p.key: i for i, p in enumerate(periods)}
    by_symbol: Dict[str, List[Dict]] = {}

    for task in state.get("tasks", {}).values():
        if task.get("status") != "done":
            continue
        symbol = task["symbol"]
        period_key = task["period"]["key"]
        returns_path = Path(task.get("returns_file", ""))
        if not returns_path.exists():
            continue
        by_symbol.setdefault(symbol, []).append(
            {"period_key": period_key, "returns_path": returns_path}
        )

    monthly_rows = []
    symbol_rows = []

    for symbol, segments in by_symbol.items():
        segments.sort(key=lambda x: period_order.get(x["period_key"], 9999))
        stitched_parts = []
        stitched_cursor = pd.Timestamp("2000-01-03")
        positive_months = 0
        month_returns = []
        used_periods = 0

        for seg in segments:
            df = pd.read_csv(seg["returns_path"])
            if df.empty:
                continue
            dt_col = df.columns[0]
            ret_col = "return" if "return" in df.columns else df.columns[-1]
            idx = pd.to_datetime(df[dt_col], errors="coerce", utc=True)
            if hasattr(idx, "dt"):
                idx = idx.dt.tz_convert(None)
            series = pd.Series(df[ret_col].astype(float).values, index=idx).dropna()
            series = series[~series.index.duplicated(keep="last")].sort_index()
            if series.empty:
                continue

            mret = float((1.0 + series).prod() - 1.0)
            month_returns.append(mret)
            if mret > 0:
                positive_months += 1
            used_periods += 1
            monthly_rows.append(
                {
                    "symbol": symbol,
                    "period": seg["period_key"],
                    "n_days": int(series.shape[0]),
                    "total_return": mret,
                    "avg_daily_return": float(series.mean()),
                    "std_daily_return": float(series.std(ddof=0)),
                    "max_drawdown": calc_max_drawdown(series),
                }
            )

            stitched_idx = pd.bdate_range(stitched_cursor, periods=len(series))
            stitched_parts.append(pd.Series(series.values, index=stitched_idx))
            stitched_cursor = stitched_idx[-1] + pd.offsets.BDay(1)

        if not stitched_parts:
            continue

        stitched = pd.concat(stitched_parts).sort_index()
        stitched = stitched[~stitched.index.duplicated(keep="last")]
        stitched.name = "return"

        symbol_dir = factsheets_dir / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)
        stitched_path = symbol_dir / "returns_stitched.csv"
        stitched.to_csv(stitched_path, header=True)

        if qs_available and qs is not None:
            html_path = symbol_dir / "factsheet_quantstats.html"
            try:
                qs.reports.html(
                    stitched,
                    output=str(html_path),
                    title=f"{args.strategy} - {symbol} (quarterly sample stitched)",
                )
            except Exception:
                with (symbol_dir / "quantstats_error.txt").open("w", encoding="utf-8") as f:
                    f.write(traceback.format_exc())

        total_return = float((1.0 + stitched).prod() - 1.0)
        std_month = float(pd.Series(month_returns).std(ddof=0)) if month_returns else 0.0
        symbol_rows.append(
            {
                "symbol": symbol,
                "periods_tested": used_periods,
                "pct_positive_months": (positive_months / used_periods) if used_periods else 0.0,
                "avg_monthly_return": float(pd.Series(month_returns).mean()) if month_returns else 0.0,
                "std_monthly_return": std_month,
                "sample_total_return": total_return,
                "stitched_max_drawdown": calc_max_drawdown(stitched),
            }
        )

    monthly_df = pd.DataFrame(monthly_rows)
    symbol_df = pd.DataFrame(symbol_rows)

    if not monthly_df.empty:
        monthly_df = monthly_df.sort_values(["symbol", "period"])
    if not symbol_df.empty:
        symbol_df = symbol_df.sort_values(
            ["pct_positive_months", "sample_total_return"],
            ascending=[False, False],
        )

    if not monthly_df.empty:
        monthly_df.to_csv(output_root / "monthly_results.csv", index=False)
    if not symbol_df.empty:
        symbol_df.to_csv(output_root / "symbol_summary.csv", index=False)

    # --- period × symbol return matrix --------------------------------
    if not monthly_df.empty:
        matrix_title = f"{args.strategy} — monthly return matrix"
        matrix_html = build_matrix_html(monthly_df, title=matrix_title)
        matrix_path = output_root / "return_matrix.html"
        matrix_path.write_text(matrix_html, encoding="utf-8")
        print(f"Return matrix → {matrix_path}")

    summary = {
        "symbols_total": len(by_symbol),
        "symbols_with_results": int(symbol_df.shape[0]) if not symbol_df.empty else 0,
        "quantstats_available": qs_available,
        "output_root": str(output_root),
    }
    with (output_root / "analysis_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    output_root = (repo_root / args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    state_path = Path(args.state_file).resolve() if args.state_file else default_state_path(output_root)

    sample_months = [int(x.strip()) for x in args.sample_months.split(",") if x.strip()]
    bad_months = [m for m in sample_months if m < 1 or m > 12]
    if bad_months:
        raise ValueError(f"Invalid months: {bad_months}")

    symbols, ticker_file_path = load_symbols(repo_root, args.ticker_file)
    periods = build_periods(args.end_year, args.years, sample_months)

    state = load_state(state_path)
    state["config"] = {
        "ticker_file": str(ticker_file_path),
        "n_symbols": len(symbols),
        "strategy": args.strategy,
        "stratargs": args.stratargs,
        "years": args.years,
        "end_year": args.end_year,
        "sample_months": sample_months,
        "periods": [asdict(p) for p in periods],
        "run_id_prefix": args.run_id_prefix,
    }
    save_state(state_path, state)

    if args.phase == "prefetch" or (args.phase == "all" and args.prefetch):
        run_prefetch(args, repo_root, ticker_file_path, symbols, periods, state, state_path)
    if args.phase in ("all", "run"):
        run_backtests(args, repo_root, symbols, periods, state, state_path)
    if args.phase in ("all", "analyze"):
        analyze_results(args, repo_root, state, periods)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted. State file preserved for resume.", file=sys.stderr)
        raise
