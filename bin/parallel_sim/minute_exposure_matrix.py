#!/usr/bin/env python3
"""
Build minute-by-minute exposure matrices from sim_paper_trade_match_matrix CSV.

For each side (sim, paper):
- count matrix: active trade count per minute x symbol
- notional matrix: gross notional per minute x symbol (qty * open_avg_price)
- totals CSV: per-minute total active count and total gross notional

Input is trade-level matrix (not order-level).
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


DT_FMT = "%Y-%m-%d %H:%M"


@dataclass
class TradeLeg:
    symbol: str
    open_dt: datetime
    close_dt: datetime
    qty: float
    open_price: float

    @property
    def notional(self) -> float:
        return abs(self.qty * self.open_price)


def parse_dt(value: str) -> Optional[datetime]:
    txt = (value or "").strip()
    if not txt:
        return None
    try:
        return datetime.strptime(txt, DT_FMT)
    except ValueError:
        return None


def parse_float(value: str) -> Optional[float]:
    txt = (value or "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def minute_range(start: datetime, end: datetime, include_close: bool) -> Iterable[datetime]:
    if include_close:
        cur = start
        while cur <= end:
            yield cur
            cur += timedelta(minutes=1)
        return

    # [start, end) for exposure, but keep single-minute trades visible
    if start >= end:
        yield start
        return
    cur = start
    while cur < end:
        yield cur
        cur += timedelta(minutes=1)


def load_legs(matrix_path: Path, prefix: str) -> List[TradeLeg]:
    open_col = f"{prefix}_open_bar"
    qty_col = f"{prefix}_open_qty"
    px_col = f"{prefix}_open_avg_price"
    close_col = f"{prefix}_close_bar"

    legs: List[TradeLeg] = []
    with matrix_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = (row.get("asset") or "").strip()
            if not symbol:
                continue
            odt = parse_dt(row.get(open_col, ""))
            cdt = parse_dt(row.get(close_col, ""))
            qty = parse_float(row.get(qty_col, ""))
            px = parse_float(row.get(px_col, ""))
            if odt is None or cdt is None or qty is None or px is None:
                continue
            if qty == 0:
                continue
            if cdt < odt:
                # Defensive: keep coherent interval
                cdt = odt
            legs.append(TradeLeg(symbol=symbol, open_dt=odt, close_dt=cdt, qty=qty, open_price=px))
    return legs


def build_matrices(
    legs: List[TradeLeg],
    include_close: bool,
) -> Tuple[List[datetime], List[str], Dict[Tuple[datetime, str], int], Dict[Tuple[datetime, str], float]]:
    if not legs:
        return [], [], {}, {}

    symbols = sorted({l.symbol for l in legs})
    min_dt = min(l.open_dt for l in legs)
    max_dt = max(l.close_dt for l in legs)

    timeline: List[datetime] = []
    cur = min_dt
    while cur <= max_dt:
        timeline.append(cur)
        cur += timedelta(minutes=1)

    count_map: Dict[Tuple[datetime, str], int] = {}
    notional_map: Dict[Tuple[datetime, str], float] = {}
    for leg in legs:
        for minute in minute_range(leg.open_dt, leg.close_dt, include_close=include_close):
            key = (minute, leg.symbol)
            count_map[key] = count_map.get(key, 0) + 1
            notional_map[key] = notional_map.get(key, 0.0) + leg.notional

    return timeline, symbols, count_map, notional_map


def write_wide_matrix(
    out_path: Path,
    timeline: List[datetime],
    symbols: List[str],
    values: Dict[Tuple[datetime, str], float],
    decimals: int,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["minute", *symbols])
        for minute in timeline:
            row = [minute.strftime(DT_FMT)]
            for sym in symbols:
                val = values.get((minute, sym), 0.0)
                if decimals == 0:
                    row.append(str(int(val)))
                else:
                    row.append(f"{val:.{decimals}f}")
            w.writerow(row)


def write_totals(
    out_path: Path,
    timeline: List[datetime],
    symbols: List[str],
    count_map: Dict[Tuple[datetime, str], int],
    notional_map: Dict[Tuple[datetime, str], float],
) -> Tuple[Tuple[datetime, int], Tuple[datetime, float]]:
    max_count = (-1, None)  # type: ignore[assignment]
    max_notional = (-1.0, None)  # type: ignore[assignment]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["minute", "active_trades_total", "gross_notional_total"])
        for minute in timeline:
            total_count = 0
            total_notional = 0.0
            for sym in symbols:
                total_count += int(count_map.get((minute, sym), 0))
                total_notional += float(notional_map.get((minute, sym), 0.0))
            w.writerow([minute.strftime(DT_FMT), total_count, f"{total_notional:.2f}"])
            if total_count > max_count[0]:
                max_count = (total_count, minute)
            if total_notional > max_notional[0]:
                max_notional = (total_notional, minute)

    # Cast back to expected tuple types
    return (max_count[1], max_count[0]), (max_notional[1], max_notional[0])  # type: ignore[index]


def process_side(matrix_path: Path, out_dir: Path, side: str, include_close: bool) -> None:
    legs = load_legs(matrix_path, prefix=side)
    timeline, symbols, count_map, notional_map = build_matrices(legs, include_close=include_close)

    count_csv = out_dir / f"{side}_minute_count_matrix.csv"
    notional_csv = out_dir / f"{side}_minute_notional_matrix.csv"
    totals_csv = out_dir / f"{side}_minute_totals.csv"

    write_wide_matrix(count_csv, timeline, symbols, {k: float(v) for k, v in count_map.items()}, decimals=0)
    write_wide_matrix(notional_csv, timeline, symbols, notional_map, decimals=2)
    max_count, max_notional = write_totals(totals_csv, timeline, symbols, count_map, notional_map)

    print(f"[{side.upper()}] trades_used={len(legs)} symbols={len(symbols)} minutes={len(timeline)}")
    if max_count[0] is not None:
        print(f"[{side.upper()}] max_active_trades={max_count[1]} at {max_count[0].strftime(DT_FMT)}")
    if max_notional[0] is not None:
        print(f"[{side.upper()}] max_gross_notional={max_notional[1]:.2f} at {max_notional[0].strftime(DT_FMT)}")
    print(f"[{side.upper()}] wrote: {count_csv}")
    print(f"[{side.upper()}] wrote: {notional_csv}")
    print(f"[{side.upper()}] wrote: {totals_csv}")


def main() -> None:
    p = argparse.ArgumentParser(description="Minute exposure matrices from sim/paper trade match matrix")
    p.add_argument("--matrix", required=True, help="Path to sim_paper_trade_match_matrix_*.csv")
    p.add_argument(
        "--outdir",
        default="out/intraday/HMA",
        help="Output directory for generated CSV matrices",
    )
    p.add_argument(
        "--exclude-close-minute",
        action="store_true",
        help="Use [open, close) interval instead of [open, close]",
    )
    args = p.parse_args()

    matrix_path = Path(args.matrix).resolve()
    out_dir = Path(args.outdir).resolve()
    include_close = not args.exclude_close_minute

    process_side(matrix_path, out_dir, side="sim", include_close=include_close)
    process_side(matrix_path, out_dir, side="paper", include_close=include_close)


if __name__ == "__main__":
    main()

