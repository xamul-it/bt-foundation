#!/usr/bin/env python3
"""
HMA TP Study — "Crema sul latte"

Per ogni trade calcola:
- exit_pct naturale (uscita su segnale)
- mfe_window = max(high[entry : natural_exit_bar]) / entry_price - 1 (per long)
               min(low[entry : natural_exit_bar]) / entry_price - 1  (per short, negato)
- tp_gain(T) = guadagno atteso se si imposta TP a livello T:
    P(mfe_window >= T) * (T - exit_pct_medio_dei_trade_che_avrebbero_preso_TP)

Obiettivo: trovare T > mean(exit_pct) che massimizza il guadagno atteso del TP.
Il TP è parallelo all'uscita naturale — se viene raggiunto PRIMA del segnale di uscita,
chiude il trade; altrimenti si prende l'uscita naturale. Trade count invariato.

Uso:
    python bt-strategy-test/HMA/tp_study.py
    python bt-strategy-test/HMA/tp_study.py --period 8 --symbols KHC NFLX WBD
    python bt-strategy-test/HMA/tp_study.py --out /tmp/tp_study.csv
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


# ── Per-trade MFE nella finestra reale [entry : natural_exit] ──────────────────

def compute_tp_stats(
    df_rth: pd.DataFrame,
    trades: pd.DataFrame,
    tp_levels: np.ndarray,
) -> dict:
    """
    Per ogni trade calcola mfe_window = MFE massimo nella finestra
    [entry_bar : natural_exit_bar] (inclusi entrambi gli estremi).

    Poi per ogni livello T in tp_levels calcola:
    - p_hit       : P(mfe_window >= T)
    - mean_gain   : E[T - exit_pct | mfe_window >= T] — guadagno medio quando TP scatta
    - mean_cost   : E[exit_pct - T | mfe_window >= T AND exit_pct > T] — costo taglio trade buoni
    - net_gain    : E[exit_pct con TP] - E[exit_pct senza TP] su tutti i trade
    """
    if trades.empty:
        return {}

    idx = df_rth.index
    highs  = df_rth["high"].values
    lows   = df_rth["low"].values

    mfe_windows = []
    natural_exits = []

    for _, row in trades.iterrows():
        entry_ts = row["entry_time"]
        exit_ts  = row["exit_time"]
        side     = row["side"]
        entry_px = row["entry_price"]
        exit_pct = row["exit_pct"]

        # trova indici nel df_rth
        try:
            i0 = idx.get_loc(entry_ts)
            i1 = idx.get_loc(exit_ts)
        except KeyError:
            # timestamp con timezone — cerca più vicino
            i0 = idx.searchsorted(entry_ts)
            i1 = idx.searchsorted(exit_ts)
            if i0 >= len(idx): i0 = len(idx) - 1
            if i1 >= len(idx): i1 = len(idx) - 1

        if i1 <= i0:
            i1 = i0 + 1
        i1 = min(i1 + 1, len(idx))  # includi la barra di uscita

        if side == "long":
            peak = highs[i0:i1].max()
            mfe  = (peak / entry_px - 1) * 100
        else:
            trough = lows[i0:i1].min()
            mfe    = (entry_px / trough - 1) * 100

        mfe_windows.append(mfe)
        natural_exits.append(exit_pct)

    mfe_arr  = np.array(mfe_windows)
    exit_arr = np.array(natural_exits)

    mean_natural = exit_arr.mean()
    n = len(exit_arr)

    results = {
        "n_trades":     n,
        "mean_exit":    mean_natural,
        "mean_mfe":     mfe_arr.mean(),
        "mfe_p50":      np.percentile(mfe_arr, 50),
        "mfe_p75":      np.percentile(mfe_arr, 75),
        "mfe_p90":      np.percentile(mfe_arr, 90),
        "levels":       [],
    }

    for T in tp_levels:
        hit_mask = mfe_arr >= T

        p_hit = hit_mask.mean()

        if p_hit == 0:
            results["levels"].append({
                "T": T, "p_hit": 0,
                "mean_gain_when_hit": 0,
                "cost_cut_good": 0,
                "net_gain_per_trade": 0,
                "delta_mean_exit": 0,
            })
            continue

        # Quando TP scatta: guadagno = T invece di exit naturale
        # Ma solo se TP viene raggiunto PRIMA della exit naturale.
        # Approssimazione: se mfe_window >= T assumiamo che il picco si verifichi
        # nel corso del trade (non necessariamente prima dell'uscita naturale —
        # raffinamento futuro con timing preciso del picco).
        exit_with_tp = np.where(hit_mask, T, exit_arr)
        mean_with_tp = exit_with_tp.mean()
        net_gain = mean_with_tp - mean_natural

        # Costo: trade dove exit_naturale > T ma TP scatta ugualmente (taglio trade buoni)
        cut_mask = hit_mask & (exit_arr > T)
        mean_cost = (exit_arr[cut_mask] - T).mean() if cut_mask.any() else 0.0

        # Guadagno medio quando TP scatta (rispetto all'uscita naturale)
        mean_gain_hit = (T - exit_arr[hit_mask]).mean()

        results["levels"].append({
            "T":                    round(T, 4),
            "p_hit":                round(p_hit, 4),
            "mean_gain_when_hit":   round(mean_gain_hit, 4),
            "cost_cut_good":        round(mean_cost, 4),
            "net_gain_per_trade":   round(net_gain, 5),
            "delta_mean_exit":      round(mean_with_tp - mean_natural, 5),
        })

    return results


# ── Main ───────────────────────────────────────────────────────────────────────

def run_tp_study(
    symbols: list[str],
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

        df_rth, signals_rth = compute_rth_signals(df_full, hma_signal, period=period, inverted=INVERTED)
        if df_rth.empty:
            continue

        trades = engine.run(df_rth, strategy, symbol=sym, signals=signals_rth)
        if trades.empty:
            continue

        mean_exit = trades["exit_pct"].mean()

        # Livelli TP: da mean_exit fino a mean_exit * 10, 40 punti
        # e griglia fine tra 0 e 0.5%
        t_min = max(mean_exit * 1.0, 0.001)  # floor: leggermente sopra media
        t_max = 0.5
        tp_levels = np.unique(np.concatenate([
            np.linspace(t_min, t_max, 40),
            np.linspace(0.01, 0.30, 30),
        ]))
        tp_levels = tp_levels[tp_levels > mean_exit]
        tp_levels = np.sort(tp_levels)

        stats = compute_tp_stats(df_rth, trades, tp_levels)

        if verbose:
            print(f"\n{'='*60}")
            print(f"  {sym}  ({len(trades)} trade)  mean_exit={mean_exit:+.4f}%")
            print(f"  MFE window: mean={stats['mean_mfe']:+.4f}%  p50={stats['mfe_p50']:+.4f}%  p75={stats['mfe_p75']:+.4f}%  p90={stats['mfe_p90']:+.4f}%")
            print(f"\n  {'T%':>7}  {'P(hit)':>7}  {'gain|hit':>9}  {'cost_cut':>9}  {'net/trade':>10}")
            print(f"  {'-'*50}")

            # Mostra solo i livelli con net_gain > 0 e alcuni rappresentativi
            best_row = None
            best_net = -999
            for lv in stats["levels"]:
                if lv["net_gain_per_trade"] > best_net:
                    best_net = lv["net_gain_per_trade"]
                    best_row = lv

            # Stampa tabella ridotta: ogni 5 livelli + il best
            best_idx = stats["levels"].index(best_row) if best_row else -1
            for j, lv in enumerate(stats["levels"]):
                if j % 5 == 0 or j == best_idx:
                    print(
                        f"  {lv['T']:7.4f}  {lv['p_hit']:7.3f}  "
                        f"{lv['mean_gain_when_hit']:+9.4f}  "
                        f"{lv['cost_cut_good']:9.4f}  "
                        f"{lv['net_gain_per_trade']:+10.5f}"
                        + ("  ← BEST" if j == best_idx else "")
                    )

            if best_row:
                print(f"\n  BEST TP = {best_row['T']:.4f}%  net_gain/trade = {best_row['net_gain_per_trade']:+.5f}%"
                      f"  P(hit) = {best_row['p_hit']:.3f}")

        # Aggrega riga riassuntiva per simbolo
        if stats["levels"]:
            best = max(stats["levels"], key=lambda x: x["net_gain_per_trade"])
            all_rows.append({
                "symbol":         sym,
                "n_trades":       stats["n_trades"],
                "mean_exit":      round(stats["mean_exit"], 5),
                "mean_mfe":       round(stats["mean_mfe"], 5),
                "mfe_p50":        round(stats["mfe_p50"], 5),
                "mfe_p75":        round(stats["mfe_p75"], 5),
                "best_tp":        best["T"],
                "best_p_hit":     best["p_hit"],
                "best_net_gain":  best["net_gain_per_trade"],
            })

    summary = pd.DataFrame(all_rows)
    if verbose and not summary.empty:
        print(f"\n{'='*60}")
        print("RIEPILOGO")
        print(summary.to_string(index=False))
        if "best_net_gain" in summary.columns:
            tot = (summary["best_net_gain"] * summary["n_trades"]).sum() / summary["n_trades"].sum()
            print(f"\nNet gain medio pesato:  {tot:+.5f}%/trade")
            print(f"Mean exit medio pesato: {(summary['mean_exit'] * summary['n_trades']).sum() / summary['n_trades'].sum():+.5f}%/trade")

    return summary


def parse_args():
    p = argparse.ArgumentParser(description="HMA TP Study — crema sul latte")
    p.add_argument("--symbols", nargs="+", default=HMA_TOP9)
    p.add_argument("--from",  dest="fromdate", default=None)
    p.add_argument("--to",    dest="todate",   default=None)
    p.add_argument("--period",  type=int, default=DEFAULT_PERIOD)
    p.add_argument("--exitbar", type=int, default=DEFAULT_EXITBAR)
    p.add_argument("--out",     default=None, help="Salva summary CSV")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(f"\nHMA TP Study  period={args.period}  exitbar={args.exitbar}  inverted={INVERTED}\n")
    summary = run_tp_study(
        symbols=args.symbols,
        fromdate=args.fromdate,
        todate=args.todate,
        period=args.period,
        exitbar=args.exitbar,
    )
    if args.out and not summary.empty:
        summary.to_csv(args.out, index=False)
        print(f"\nSalvato: {args.out}")
