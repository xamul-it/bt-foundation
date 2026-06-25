#!/usr/bin/env python3
"""
HMA 3-source comparison — pandas vs BT backtest vs paper trading.

Confronta i trade da tre fonti:
  1. pandas (sim.py)      — sempre la base di riferimento
  2. BT backtest          — deve coincidere con pandas (delta accettabile < 0.1%)
  3. paper trading        — diverge per fill lag, slippage; l'agente diagnostica perché

Uso:
    # Solo pandas vs BT (backtest):
    python bt-strategy-test/HMA/compare.py --bt-dir out/intraday/HMA/sim

    # Tre fonti (pandas + BT + paper):
    python bt-strategy-test/HMA/compare.py --bt-dir out/intraday/HMA/sim --paper-dir out/intraday/HMA/paper

    # Filtra su un simbolo:
    python bt-strategy-test/HMA/compare.py --bt-dir out/intraday/HMA/sim --symbol NFLX

Output:
    Tabella divergenze con colonne:
        symbol, entry_time, pd_entry, bt_entry, pd_exit, bt_exit, delta_pnl, divergence_type
"""
import sys
from pathlib import Path

AGENT_ROOT = Path(__file__).resolve().parents[2] / "bt-agent"
sys.path.insert(0, str(AGENT_ROOT))

import argparse
import json
import pandas as pd

from bt_bridge.comparator import TradeComparator
from sim import run_sim, HMA_TOP9


# ── BT output reader ──────────────────────────────────────────────────────────

def _load_bt_trades(bt_dir: Path, symbol: str | None = None) -> pd.DataFrame:
    """
    Legge trades.json da out/{module}/{Strategy}/{id}/.
    Formato atteso: lista di dict con entry_time, exit_time, entry_price, exit_price, side, pnl_pct.
    Prova anche transactions.csv (QuantStats format).
    """
    # prova trades.json
    trades_file = bt_dir / "trades.json"
    if trades_file.exists():
        with open(trades_file) as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        _normalize_bt_df(df)
        if symbol:
            df = df[df["symbol"] == symbol]
        return df

    # prova transactions.csv
    csv_file = bt_dir / "transactions.csv"
    if csv_file.exists():
        df = pd.read_csv(csv_file, parse_dates=["Date"])
        _normalize_bt_csv(df)
        if symbol:
            df = df[df["symbol"] == symbol]
        return df

    raise FileNotFoundError(
        f"Nessun file trades trovato in {bt_dir}. "
        "Cerca trades.json o transactions.csv"
    )


def _normalize_bt_df(df: pd.DataFrame) -> None:
    """Normalizza colonne trades.json → schema comparator."""
    rename = {
        "entry_time": "entry_time", "exit_time": "exit_time",
        "entry_price": "entry_price", "exit_price": "exit_price",
        "side": "side", "symbol": "symbol",
        "pnl_pct": "exit_pct", "pnl": "exit_pct",
    }
    df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)
    for col in ("entry_time", "exit_time"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    if "symbol" not in df.columns:
        df["symbol"] = "UNKNOWN"


def _normalize_bt_csv(df: pd.DataFrame) -> None:
    """Normalizza transactions.csv (QuantStats) → schema comparator."""
    rename = {"Date": "entry_time", "Symbol": "symbol", "Side": "side"}
    df.rename(columns=rename, inplace=True)
    df["entry_time"] = pd.to_datetime(df["entry_time"], utc=True, errors="coerce")
    if "exit_time" not in df.columns:
        df["exit_time"] = df["entry_time"]
    if "entry_price" not in df.columns:
        df["entry_price"] = df.get("Price", pd.NA)
    if "exit_price" not in df.columns:
        df["exit_price"] = pd.NA
    if "exit_pct" not in df.columns:
        df["exit_pct"] = pd.NA


# ── main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="HMA 3-source comparison")
    p.add_argument("--bt-dir",    required=True, help="Dir output BT (contiene trades.json o transactions.csv)")
    p.add_argument("--paper-dir", default=None,  help="Dir output paper trading (opzionale)")
    p.add_argument("--symbols",   nargs="+", default=HMA_TOP9)
    p.add_argument("--from",  dest="fromdate", default=None)
    p.add_argument("--to",    dest="todate",   default=None)
    p.add_argument("--symbol", default=None, help="Filtra su un solo simbolo")
    p.add_argument("--out",   default=None, help="Salva report CSV")
    return p.parse_args()


def main():
    args = parse_args()
    symbols = [args.symbol] if args.symbol else args.symbols

    print("\nRun pandas simulation...")
    pd_trades, _ = run_sim(symbols=symbols, fromdate=args.fromdate,
                           todate=args.todate, verbose=False)

    if pd_trades.empty:
        print("Nessun trade pandas. Controlla i dati.")
        return

    bt_dir = Path(args.bt_dir)
    print(f"\nCarico BT trades da: {bt_dir}")
    try:
        bt_trades = _load_bt_trades(bt_dir, symbol=args.symbol)
    except FileNotFoundError as e:
        print(f"ERRORE: {e}")
        sys.exit(1)

    # pandas vs BT
    print(f"\n── pandas ({len(pd_trades)} trades) vs BT ({len(bt_trades)} trades) ──")
    cmp = TradeComparator()
    diff = cmp.compare(pd_trades, bt_trades)
    _print_divergences(diff, "pandas vs BT")

    # pandas vs paper
    if args.paper_dir:
        paper_dir = Path(args.paper_dir)
        print(f"\nCarico paper trades da: {paper_dir}")
        try:
            paper_trades = _load_bt_trades(paper_dir, symbol=args.symbol)
        except FileNotFoundError as e:
            print(f"ATTENZIONE paper: {e}")
            paper_trades = pd.DataFrame()

        if not paper_trades.empty:
            print(f"\n── pandas ({len(pd_trades)} trades) vs paper ({len(paper_trades)} trades) ──")
            diff_paper = cmp.compare(pd_trades, paper_trades)
            _print_divergences(diff_paper, "pandas vs paper")

    if args.out:
        diff.to_csv(args.out, index=False)
        print(f"\nReport salvato: {args.out}")


def _print_divergences(diff: pd.DataFrame, label: str) -> None:
    if diff.empty:
        print(f"  {label}: nessun dato")
        return

    types = diff["divergence_type"].value_counts()
    print(f"  Totale righe: {len(diff)}")
    for t, n in types.items():
        print(f"    {t}: {n}")

    mismatches = diff[diff["divergence_type"] != "match"]
    if mismatches.empty:
        print("  ✓ Nessuna divergenza!")
    else:
        print(f"\n  Prime 10 divergenze:")
        cols = [c for c in ["symbol", "entry_time", "pd_entry", "bt_entry",
                            "pd_exit", "bt_exit", "delta_pnl", "divergence_type"]
                if c in mismatches.columns]
        print(mismatches[cols].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
