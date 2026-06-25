#!/usr/bin/env python3
"""
HMA SL Study — calibrazione Stop Loss come paracadute

Per ogni trade calcola MAE (Max Adverse Excursion) sulla finestra fissa
[entry_bar : entry_bar + exitbar], che è la massima esposizione possibile
indipendentemente da quando il trade esce realmente.

Mostra distribuzione percentile separata per long e short:
- p90, p95, p97, p99 del MAE
- Per ciascun livello: % trade stoppati e % di quelli che erano già perdenti

Lo SL va impostato come paracadute operativo (non per ottimizzare il backtest),
tipicamente a p95–p99 a seconda della tolleranza al rischio live.

Uso:
    python bt-strategy-test/HMA/sl_study.py
    python bt-strategy-test/HMA/sl_study.py --period 8 --exitbar 6 --symbols KHC NFLX
    python bt-strategy-test/HMA/sl_study.py --out /tmp/sl_study.csv
"""
import sys
from pathlib import Path

WORKSPACE  = Path(__file__).resolve().parents[2]
AGENT_ROOT = WORKSPACE / "bt-agent"
sys.path.insert(0, str(AGENT_ROOT))
sys.path.insert(0, str(WORKSPACE))

import argparse
import numpy as np
import pandas as pd

from engine.backtest import PandasBacktest
from strategies.generic import SignalStrategy
from sim import load_symbol, compute_rth_signals, hma_signal, HMA_TOP9

DEFAULT_PERIOD   = 8
DEFAULT_EXITBAR  = 6
INVERTED         = True
EXITBAR_MODE     = "bt_style"
MINUTES_BF_CLOSE = 15
MINUTES_AF_OPEN  = 10


def compute_mae_per_trade(
    df_rth: pd.DataFrame,
    trades: pd.DataFrame,
    exitbar: int,
) -> pd.DataFrame:
    """
    Per ogni trade calcola MAE sulla finestra [entry_bar : entry_bar + exitbar].

    Returns DataFrame con colonne aggiuntive:
        mae_pct   — max adverse excursion %
        is_winner — True se exit_pct > 0
    """
    if trades.empty:
        return trades.copy()

    idx   = df_rth.index
    highs = df_rth["high"].values
    lows  = df_rth["low"].values

    mae_list = []

    for _, row in trades.iterrows():
        entry_ts = row["entry_time"]
        entry_px = row["entry_price"]
        side     = row["side"]

        try:
            i0 = idx.get_loc(entry_ts)
        except KeyError:
            i0 = idx.searchsorted(entry_ts)
            if i0 >= len(idx):
                i0 = len(idx) - 1

        # finestra fissa: [entry : entry + exitbar] inclusi
        i1 = min(i0 + exitbar + 1, len(idx))

        if side == "long":
            worst = lows[i0:i1].min()
            mae   = (entry_px - worst) / entry_px * 100
        else:
            worst = highs[i0:i1].max()
            mae   = (worst - entry_px) / entry_px * 100

        mae_list.append(max(mae, 0.0))  # MAE >= 0 per costruzione

    result = trades.copy()
    result["mae_pct"]   = mae_list
    result["is_winner"] = result["exit_pct"] > 0
    return result


def print_mae_table(mae_arr: np.ndarray, exit_arr: np.ndarray, label: str) -> dict:
    """
    Stampa tabella percentili MAE e ritorna dizionario con i livelli.
    """
    n = len(mae_arr)
    if n == 0:
        return {}

    percentiles = [50, 75, 90, 95, 97, 99]
    levels = {p: np.percentile(mae_arr, p) for p in percentiles}

    print(f"\n  {label}  ({n} trade)  MAE medio={mae_arr.mean():.4f}%")
    print(f"  {'pct':>4}  {'SL_dist':>8}  {'stopped%':>9}  {'of_which_losers%':>17}  {'impact_mean_exit':>17}")
    print(f"  {'-'*60}")

    mean_natural = exit_arr.mean()
    rows = {}

    for p in percentiles:
        sl = levels[p]
        stopped_mask = mae_arr >= sl
        n_stopped = stopped_mask.sum()
        pct_stopped = n_stopped / n * 100

        losers_among_stopped = (exit_arr[stopped_mask] <= 0).sum()
        pct_losers = losers_among_stopped / n_stopped * 100 if n_stopped > 0 else 0.0

        # impatto: rimpiazza exit_pct dei trade stoppati con -sl
        exit_with_sl = np.where(stopped_mask, -sl, exit_arr)
        delta = exit_with_sl.mean() - mean_natural

        print(f"  p{p:02d}  {sl:8.4f}%  {pct_stopped:8.1f}%  {pct_losers:16.1f}%  {delta:+16.5f}%")
        rows[p] = {
            "sl_pct": round(sl, 4),
            "pct_stopped": round(pct_stopped, 2),
            "pct_losers_stopped": round(pct_losers, 1),
            "delta_mean_exit": round(delta, 5),
        }

    return rows


def run_sl_study(
    symbols:  list[str],
    fromdate: str | None = None,
    todate:   str | None = None,
    period:   int  = DEFAULT_PERIOD,
    exitbar:  int  = DEFAULT_EXITBAR,
    verbose:  bool = True,
) -> pd.DataFrame:

    strategy = SignalStrategy(signal_fn=hma_signal, period=period, inverted=INVERTED)
    engine   = PandasBacktest(
        sl_pct=0.0,
        exitbar=exitbar,
        exitbar_mode=EXITBAR_MODE,
        minutes_before_close=MINUTES_BF_CLOSE,
        minutes_after_open=MINUTES_AF_OPEN,
    )

    all_rows = []

    for sym in symbols:
        try:
            df_full = load_symbol(sym, fromdate, todate, rth_only=False)
        except FileNotFoundError as e:
            print(f"  [SKIP] {e}", file=sys.stderr)
            continue

        df_rth, signals_rth = compute_rth_signals(
            df_full, hma_signal, period=period, inverted=INVERTED
        )
        if df_rth.empty:
            continue

        trades = engine.run(df_rth, strategy, symbol=sym, signals=signals_rth)
        if trades.empty:
            continue

        trades_mae = compute_mae_per_trade(df_rth, trades, exitbar)

        if verbose:
            print(f"\n{'='*65}")
            print(f"  {sym}  ({len(trades)} trade)  "
                  f"mean_exit={trades['exit_pct'].mean():+.4f}%  "
                  f"win_rate={trades['exit_pct'].gt(0).mean():.1%}")

        for side in ("long", "short"):
            mask = trades_mae["side"] == side
            sub  = trades_mae[mask]
            if sub.empty:
                continue
            mae_arr  = sub["mae_pct"].values
            exit_arr = sub["exit_pct"].values
            rows = print_mae_table(mae_arr, exit_arr, side.upper())
            for p, r in rows.items():
                all_rows.append({
                    "symbol":              sym,
                    "side":                side,
                    "n_trades":            int(mask.sum()),
                    "mean_exit":           round(sub["exit_pct"].mean(), 5),
                    "win_rate":            round(sub["exit_pct"].gt(0).mean(), 4),
                    "percentile":          p,
                    "sl_pct":              r["sl_pct"],
                    "pct_stopped":         r["pct_stopped"],
                    "pct_losers_stopped":  r["pct_losers_stopped"],
                    "delta_mean_exit":     r["delta_mean_exit"],
                })

    summary = pd.DataFrame(all_rows)

    if verbose and not summary.empty:
        print(f"\n{'='*65}")
        print("RIEPILOGO p95 per simbolo (long + short aggregato)")
        p95 = summary[summary["percentile"] == 95].copy()
        # media pesata per n_trades
        for sym in p95["symbol"].unique():
            s = p95[p95["symbol"] == sym]
            w = s["n_trades"].values
            sl_w = (s["sl_pct"].values * w).sum() / w.sum()
            stop_w = (s["pct_stopped"].values * w).sum() / w.sum()
            los_w  = (s["pct_losers_stopped"].values * w).sum() / w.sum()
            dlt_w  = (s["delta_mean_exit"].values * w).sum() / w.sum()
            print(f"  {sym:<6}  SL={sl_w:.4f}%  stopped={stop_w:.1f}%  "
                  f"losers={los_w:.1f}%  Δmean={dlt_w:+.5f}%")

    return summary


def parse_args():
    p = argparse.ArgumentParser(description="HMA SL Study — calibrazione stop loss")
    p.add_argument("--symbols", nargs="+", default=HMA_TOP9)
    p.add_argument("--from",    dest="fromdate", default=None)
    p.add_argument("--to",      dest="todate",   default=None)
    p.add_argument("--period",  type=int, default=DEFAULT_PERIOD)
    p.add_argument("--exitbar", type=int, default=DEFAULT_EXITBAR)
    p.add_argument("--out",     default=None, help="Salva summary CSV")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(f"\nHMA SL Study  period={args.period}  exitbar={args.exitbar}  "
          f"inverted={INVERTED}  finestra_mae=[entry:entry+{args.exitbar}]\n")
    summary = run_sl_study(
        symbols=args.symbols,
        fromdate=args.fromdate,
        todate=args.todate,
        period=args.period,
        exitbar=args.exitbar,
    )
    if args.out and not summary.empty:
        summary.to_csv(args.out, index=False)
        print(f"\nSalvato: {args.out}")
