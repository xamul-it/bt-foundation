#!/usr/bin/env python3
"""
HMA Time-of-Day Analysis — Stadio 2

Due analisi complementari con i migliori parametri dal sweep (period=8):

1. Post-hoc analysis: raggruppa trades per slot 30-min di entry_time →
   mostra quali ore generano più P&L / migliore Sharpe.

2. Window sweep: per ogni finestra temporale (es. solo mattina, solo chiusura)
   azzerare i segnali fuori finestra e rieseguire la simulazione →
   metriche reali (non stimate) per ogni sottoinsieme temporale.

Uso:
    python bin/HMA/tod_sweep.py
    python bin/HMA/tod_sweep.py --period 8 --exitbar 6
    python bin/HMA/tod_sweep.py --symbols CSCO NFLX KHC
    python bin/HMA/tod_sweep.py --from 2025-01-01 --to 2025-03-31
    python bin/HMA/tod_sweep.py --out /tmp/tod_detail.csv
"""
import sys
from pathlib import Path

WORKSPACE  = Path(__file__).resolve().parent.parent.parent   # backtrader/
AGENT_ROOT = WORKSPACE / "bt-agent"
sys.path.insert(0, str(AGENT_ROOT))
sys.path.insert(0, str(WORKSPACE))

import argparse
from datetime import time as dtime
import pandas as pd
import numpy as np

from engine.backtest import PandasBacktest
from engine.metrics import compute_metrics
from strategies.generic import SignalStrategy
from bin.HMA.sim import load_symbol, compute_rth_signals, hma_signal, HMA_TOP9

# ── Defaults ───────────────────────────────────────────────────────────────────
DEFAULT_PERIOD  = 8
DEFAULT_EXITBAR = 6
INVERTED        = True
EXITBAR_MODE    = "bt_style"
MINUTES_BF_CLOSE = 15
MINUTES_AF_OPEN  = 10

# Slot 30-min RTH: 09:30 → 15:30 (ultimo slot con entry possibile)
RTH_SLOTS = [
    dtime(9,  30), dtime(10,  0), dtime(10, 30),
    dtime(11,  0), dtime(11, 30), dtime(12,  0),
    dtime(12, 30), dtime(13,  0), dtime(13, 30),
    dtime(14,  0), dtime(14, 30), dtime(15,  0),
    dtime(15, 30),
]

# Finestre temporali da testare nel window sweep
WINDOWS = [
    ("full_RTH",     dtime( 9, 30), dtime(16,  0)),
    ("open_30m",     dtime( 9, 30), dtime(10,  0)),
    ("open_1h",      dtime( 9, 30), dtime(10, 30)),
    ("open_2h",      dtime( 9, 30), dtime(11, 30)),
    ("morning",      dtime( 9, 30), dtime(12,  0)),
    ("midday",       dtime(11,  0), dtime(14,  0)),
    ("afternoon",    dtime(13,  0), dtime(16,  0)),
    ("close_2h",     dtime(13, 30), dtime(16,  0)),
    ("close_1h",     dtime(14, 30), dtime(16,  0)),
    ("close_30m",    dtime(15, 30), dtime(16,  0)),
    ("no_open_30m",  dtime(10,  0), dtime(16,  0)),
    ("no_close_30m", dtime( 9, 30), dtime(15, 30)),
]


# ── Post-hoc slot analysis ─────────────────────────────────────────────────────

def slot_label(t: dtime) -> str:
    """Restituisce l'etichetta dello slot 30-min per un timestamp."""
    for i in range(len(RTH_SLOTS) - 1):
        if RTH_SLOTS[i] <= t < RTH_SLOTS[i + 1]:
            return RTH_SLOTS[i].strftime("%H:%M")
    return RTH_SLOTS[-1].strftime("%H:%M")  # 15:30+


def analyze_by_slot(trades: pd.DataFrame) -> pd.DataFrame:
    """
    Raggruppa trades per slot 30-min di entry_time.
    Ritorna DataFrame con metriche per slot.
    """
    trades = trades.copy()
    trades["slot"] = trades["entry_time"].apply(
        lambda ts: slot_label(ts.time() if hasattr(ts, "time") else ts)
    )

    rows = []
    for slot in [s.strftime("%H:%M") for s in RTH_SLOTS]:
        sub = trades[trades["slot"] == slot]
        if len(sub) == 0:
            rows.append({"slot": slot, "trades": 0, "win_rate": 0.0,
                         "mean_pct": 0.0, "total_pct": 0.0,
                         "sharpe": 0.0, "sqn": 0.0})
            continue
        m = compute_metrics(sub)
        rows.append({
            "slot":      slot,
            "trades":    len(sub),
            "win_rate":  round(m["win_rate"] * 100, 1),
            "mean_pct":  round(sub["exit_pct"].mean(), 4),
            "total_pct": round(sub["exit_pct"].sum(), 2),
            "sharpe":    round(m["sharpe"], 3),
            "sqn":       round(m["sqn"], 3),
        })
    return pd.DataFrame(rows)


# ── Window sweep ───────────────────────────────────────────────────────────────

def _run_window(
    sym: str,
    df_full: pd.DataFrame,
    period: int,
    exitbar: int,
    entry_after: dtime,
    entry_before: dtime,
) -> dict:
    """
    Esegue la simulazione azzerando i segnali fuori dalla finestra [entry_after, entry_before).
    """
    df_rth, sig_rth = compute_rth_signals(
        df_full, hma_signal, period=period, inverted=INVERTED
    )
    # Azzera segnali fuori finestra
    t = sig_rth.index.time
    window_mask = (t >= entry_after) & (t < entry_before)
    sig_win = sig_rth.where(window_mask, 0)

    strat  = SignalStrategy(signal_fn=hma_signal, period=period, inverted=INVERTED)
    engine = PandasBacktest(
        exitbar=exitbar,
        exitbar_mode=EXITBAR_MODE,
        minutes_before_close=MINUTES_BF_CLOSE,
        minutes_after_open=MINUTES_AF_OPEN,
    )
    trades = engine.run(df_rth, strat, symbol=sym, signals=sig_win)
    m = compute_metrics(trades)
    return {
        "symbol":    sym,
        "trades":    len(trades),
        "win_rate":  round(m["win_rate"] * 100, 1),
        "total_pct": round(float(trades["exit_pct"].sum()), 3) if len(trades) else 0.0,
        "sharpe":    round(m["sharpe"], 3),
        "sqn":       round(m["sqn"], 3),
    }


def run_window_sweep(
    symbols: list,
    period: int,
    exitbar: int,
    fromdate: str | None,
    todate: str | None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Per ogni finestra temporale, esegue la simulazione su tutti i simboli e aggrega.
    """
    # Carica dati una volta
    loaded: dict[str, pd.DataFrame] = {}
    for sym in symbols:
        try:
            loaded[sym] = load_symbol(sym, fromdate, todate, rth_only=False)
        except FileNotFoundError as e:
            print(f"  [SKIP] {e}", file=sys.stderr)

    if not loaded:
        return pd.DataFrame()

    rows = []
    for win_name, after_t, before_t in WINDOWS:
        sym_rows = []
        for sym, df_full in loaded.items():
            r = _run_window(sym, df_full, period, exitbar, after_t, before_t)
            r["window"] = win_name
            sym_rows.append(r)

        # Aggrega su tutti i simboli
        trades_total = sum(r["trades"] for r in sym_rows)
        sharpe_vals  = [r["sharpe"] for r in sym_rows if r["trades"] >= 10]
        sqn_vals     = [r["sqn"]    for r in sym_rows if r["trades"] >= 10]
        total_pct    = sum(r["total_pct"] for r in sym_rows)
        wr_vals      = [r["win_rate"] for r in sym_rows if r["trades"] >= 10]

        rows.append({
            "window":    win_name,
            "after":     after_t.strftime("%H:%M"),
            "before":    before_t.strftime("%H:%M"),
            "trades":    trades_total,
            "mean_wr":   round(np.mean(wr_vals),    1) if wr_vals else 0.0,
            "total_pct": round(total_pct,            2),
            "mean_sh":   round(np.mean(sharpe_vals), 3) if sharpe_vals else 0.0,
            "min_sh":    round(np.min(sharpe_vals),  3) if sharpe_vals else 0.0,
            "mean_sqn":  round(np.mean(sqn_vals),    3) if sqn_vals else 0.0,
        })
        if verbose:
            r = rows[-1]
            print(f"  {win_name:14s} [{r['after']}–{r['before']}]  "
                  f"trades={r['trades']:6d}  sh={r['mean_sh']:+.3f}  "
                  f"sqn={r['mean_sqn']:+.3f}  total={r['total_pct']:+.1f}%",
                  flush=True)

    return pd.DataFrame(rows).sort_values("mean_sh", ascending=False)


# ── Full run (post-hoc + window sweep) ────────────────────────────────────────

def run_all(
    symbols: list,
    period: int,
    exitbar: int,
    fromdate: str | None,
    todate: str | None,
    verbose: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Esegue:
    1. Simulazione completa RTH → trades
    2. Post-hoc slot analysis
    3. Window sweep
    Returns: (trades_all, slot_agg, window_agg)
    """

    # ── Simulazione completa per post-hoc ────────────────────────────────────
    if verbose:
        print(f"\nCaricamento dati ({len(symbols)} simboli)...", flush=True)

    all_trades = []
    loaded: dict[str, pd.DataFrame] = {}
    for sym in symbols:
        try:
            loaded[sym] = load_symbol(sym, fromdate, todate, rth_only=False)
        except FileNotFoundError as e:
            print(f"  [SKIP] {e}", file=sys.stderr)

    if not loaded:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if verbose:
        print(f"Simulazione completa (period={period}, exitbar={exitbar})...", flush=True)

    strat  = SignalStrategy(signal_fn=hma_signal, period=period, inverted=INVERTED)
    engine = PandasBacktest(
        exitbar=exitbar,
        exitbar_mode=EXITBAR_MODE,
        minutes_before_close=MINUTES_BF_CLOSE,
        minutes_after_open=MINUTES_AF_OPEN,
    )
    for sym, df_full in loaded.items():
        df_rth, sig_rth = compute_rth_signals(df_full, hma_signal, period=period, inverted=INVERTED)
        trades = engine.run(df_rth, strat, symbol=sym, signals=sig_rth)
        all_trades.append(trades)

    trades_all = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()

    # ── Post-hoc slot analysis ────────────────────────────────────────────────
    if verbose:
        print(f"\nAnalisi post-hoc per slot 30-min ({len(trades_all)} trades totali)...", flush=True)

    slot_sym_rows = []
    for sym in loaded:
        sym_trades = trades_all[trades_all["symbol"] == sym]
        if len(sym_trades) == 0:
            continue
        sym_slots = analyze_by_slot(sym_trades)
        sym_slots["symbol"] = sym
        slot_sym_rows.append(sym_slots)

    # Aggrega slot su tutti i simboli
    if slot_sym_rows:
        slot_detail = pd.concat(slot_sym_rows, ignore_index=True)
        g = slot_detail.groupby("slot")
        slot_agg = pd.DataFrame({
            "trades":    g["trades"].sum(),
            "mean_wr":   g["win_rate"].mean().round(1),
            "mean_pct":  g["mean_pct"].mean().round(4),
            "total_pct": g["total_pct"].sum().round(2),
            "mean_sh":   g["sharpe"].mean().round(3),
        }).reset_index()
        # Mantieni ordine cronologico
        slot_order = [s.strftime("%H:%M") for s in RTH_SLOTS]
        slot_agg["_ord"] = slot_agg["slot"].map({s: i for i, s in enumerate(slot_order)})
        slot_agg = slot_agg.sort_values("_ord").drop(columns="_ord").reset_index(drop=True)
    else:
        slot_agg = pd.DataFrame()

    # ── Window sweep ──────────────────────────────────────────────────────────
    if verbose:
        print(f"\nWindow sweep ({len(WINDOWS)} finestre)...", flush=True)

    window_agg = run_window_sweep(
        list(loaded.keys()), period, exitbar, fromdate, todate, verbose=verbose
    )

    return trades_all, slot_agg, window_agg


# ── Report ─────────────────────────────────────────────────────────────────────

def print_report(
    trades_all: pd.DataFrame,
    slot_agg: pd.DataFrame,
    window_agg: pd.DataFrame,
    period: int,
    exitbar: int,
) -> None:
    print("\n" + "═" * 72)
    print(f"  HMA Time-of-Day Analysis  period={period}  exitbar={exitbar}")
    print("═" * 72)

    # ── Slot analysis ─────────────────────────────────────────────────────
    print("\n── Distribuzione per slot 30-min (aggregato 9 simboli) ──────────────")
    print(f"  {'slot':5s}  {'trades':>7s}  {'win%':>6s}  {'mean%':>7s}  {'total%':>8s}  {'sharpe':>7s}")
    print("  " + "-" * 54)
    for _, r in slot_agg.iterrows():
        bar_len = int(max(0, r["total_pct"]) / 500 * 20)  # barra max ~500%
        bar = "█" * bar_len
        print(f"  {r['slot']:5s}  {int(r['trades']):>7d}  {r['mean_wr']:>5.1f}%  "
              f"{r['mean_pct']:>+7.4f}  {r['total_pct']:>+8.2f}%  {r['mean_sh']:>+7.3f}  {bar}")

    # ── Day of week breakdown ──────────────────────────────────────────────
    if len(trades_all):
        trades_all = trades_all.copy()
        trades_all["dow"] = pd.to_datetime(trades_all["entry_time"]).dt.day_name()
        DOW_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        print("\n── Distribuzione per giorno della settimana ─────────────────────────")
        print(f"  {'day':10s}  {'trades':>7s}  {'win%':>6s}  {'mean_pct':>8s}  {'total%':>8s}")
        for dow in DOW_ORDER:
            sub = trades_all[trades_all["dow"] == dow]
            if len(sub) == 0:
                continue
            wr = (sub["exit_pct"] > 0).mean() * 100
            print(f"  {dow:10s}  {len(sub):>7d}  {wr:>5.1f}%  "
                  f"{sub['exit_pct'].mean():>+8.4f}  {sub['exit_pct'].sum():>+8.2f}%")

    # ── Window sweep ──────────────────────────────────────────────────────
    print("\n── Window sweep (ordinato per Sharpe medio) ─────────────────────────")
    cols = ["window", "after", "before", "trades", "mean_wr", "mean_sh", "min_sh", "mean_sqn", "total_pct"]
    print(window_agg[cols].to_string(index=False))

    # Baseline
    bl = window_agg[window_agg["window"] == "full_RTH"]
    if not bl.empty:
        b = bl.iloc[0]
        print(f"\n  Baseline full_RTH: trades={int(b['trades'])}  "
              f"mean_sh={b['mean_sh']:+.3f}  mean_sqn={b['mean_sqn']:+.3f}  "
              f"total={b['total_pct']:+.1f}%")


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="HMA time-of-day analysis")
    p.add_argument("--symbols",  nargs="+", default=HMA_TOP9)
    p.add_argument("--period",   type=int,  default=DEFAULT_PERIOD)
    p.add_argument("--exitbar",  type=int,  default=DEFAULT_EXITBAR)
    p.add_argument("--from",     dest="fromdate", default=None)
    p.add_argument("--to",       dest="todate",   default=None)
    p.add_argument("--out",      default=None, help="Salva trades CSV")
    p.add_argument("--quiet",    action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    import time
    args = parse_args()
    t0 = time.time()

    trades_all, slot_agg, window_agg = run_all(
        symbols=args.symbols,
        period=args.period,
        exitbar=args.exitbar,
        fromdate=args.fromdate,
        todate=args.todate,
        verbose=not args.quiet,
    )

    if trades_all.empty:
        print("Nessun trade.", file=sys.stderr)
        sys.exit(1)

    print_report(trades_all, slot_agg, window_agg, args.period, args.exitbar)

    elapsed = time.time() - t0
    print(f"\nCompletato in {elapsed:.1f}s  ({len(trades_all)} trades totali)")

    if args.out:
        trades_all.to_csv(args.out, index=False)
        print(f"Trades salvati: {args.out}")
