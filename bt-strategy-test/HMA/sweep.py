#!/usr/bin/env python3
"""
HMA Parameter Sweep — Stadio 1: period × exitbar

Testa tutte le combinazioni su tutti i simboli HMA_top9 in parallelo.
I segnali vengono pre-calcolati UNA VOLTA per (symbol, period) e riutilizzati
per tutti gli exitbar → nessun ricalcolo HMA ridondante.

Uso:
    python bt-strategy-test/HMA/sweep.py
    python bt-strategy-test/HMA/sweep.py --symbols CSCO NFLX KHC
    python bt-strategy-test/HMA/sweep.py --periods 8 12 16 20 --exitbars 4 6 8 10
    python bt-strategy-test/HMA/sweep.py --from 2025-01-01 --to 2025-03-31
    python bt-strategy-test/HMA/sweep.py --out /tmp/hma_sweep.csv
    python bt-strategy-test/HMA/sweep.py --detail        # mostra anche righe per simbolo
"""
import sys
from pathlib import Path

WORKSPACE  = Path(__file__).resolve().parents[2]   # backtrader/
AGENT_ROOT = WORKSPACE / "bt-agent"
sys.path.insert(0, str(AGENT_ROOT))
sys.path.insert(0, str(WORKSPACE))

import argparse
import itertools
import pandas as pd
from joblib import Parallel, delayed

from engine.backtest import PandasBacktest
from engine.metrics import compute_metrics
from strategies.generic import SignalStrategy
from sim import load_symbol, compute_rth_signals, hma_signal, HMA_TOP9

# ── Default grids ──────────────────────────────────────────────────────────────
DEFAULT_PERIODS  = [8, 10, 12, 14, 16, 18, 20, 24]
DEFAULT_EXITBARS = [4, 6, 8, 10, 12]

INVERTED          = True
EXITBAR_MODE      = "bt_style"
MINUTES_BF_CLOSE  = 15
MINUTES_AF_OPEN   = 10
BASELINE          = (16, 6)   # (period, exitbar) correnti in produzione


# ── Worker ─────────────────────────────────────────────────────────────────────

def _run_symbol(
    sym: str,
    df_full: pd.DataFrame,
    periods: list,
    exitbars: list,
    fromdate: str | None,
    todate: str | None,
) -> list[dict]:
    """
    Esegue tutta la griglia period×exitbar per UN simbolo.
    Ogni worker lavora su dati propri (no shared state → thread-safe).
    """
    rows = []
    # Pre-calcola segnali per ogni period (riutilizzati su tutti gli exitbar)
    sig_cache: dict[int, tuple] = {}
    for period in periods:
        df_rth, sig_rth = compute_rth_signals(
            df_full, hma_signal, period=period, inverted=INVERTED
        )
        sig_cache[period] = (df_rth, sig_rth)

    for period, exitbar in itertools.product(periods, exitbars):
        df_rth, sig_rth = sig_cache[period]
        strat = SignalStrategy(signal_fn=hma_signal, period=period, inverted=INVERTED)
        engine = PandasBacktest(
            exitbar=exitbar,
            exitbar_mode=EXITBAR_MODE,
            minutes_before_close=MINUTES_BF_CLOSE,
            minutes_after_open=MINUTES_AF_OPEN,
        )
        trades = engine.run(df_rth, strat, symbol=sym, signals=sig_rth)
        m = compute_metrics(trades)
        total = round(float(trades["exit_pct"].sum()), 3) if len(trades) else 0.0
        rows.append({
            "symbol":    sym,
            "period":    period,
            "exitbar":   exitbar,
            "trades":    len(trades),
            "win_rate":  round(m.get("win_rate", 0) * 100, 1),
            "expect":    round(m.get("expectancy_pct", 0), 4),
            "total_pct": total,
            "sharpe":    round(m.get("sharpe", 0), 3),
            "sqn":       round(m.get("sqn", 0), 3),
            "max_dd":    round(m.get("max_drawdown_pct", 0), 2),
        })
    return rows


# ── Main sweep ─────────────────────────────────────────────────────────────────

def run_sweep(
    symbols: list[str],
    periods: list[int],
    exitbars: list[int],
    fromdate: str | None = None,
    todate: str | None = None,
    n_jobs: int = -1,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Esegue la griglia period × exitbar su tutti i simboli.
    Returns DataFrame con una riga per (symbol, period, exitbar).
    """

    # ── Step 1: carica dati (un DataFrame per simbolo) ───────────────────────
    if verbose:
        print("Caricamento dati...", flush=True)

    loaded_data: dict[str, pd.DataFrame] = {}   # sym → df_full

    for sym in symbols:
        try:
            df_full = load_symbol(sym, fromdate, todate, rth_only=False)
            loaded_data[sym] = df_full
        except FileNotFoundError as e:
            print(f"  [SKIP] {e}", file=sys.stderr)

    if not loaded_data:
        print("Nessun dato trovato.", file=sys.stderr)
        return pd.DataFrame()

    n_combos = len(loaded_data) * len(periods) * len(exitbars)
    if verbose:
        print(f"Griglia: {len(loaded_data)} simboli × {len(periods)} period × "
              f"{len(exitbars)} exitbar = {n_combos} simulazioni\n", flush=True)

    # ── Step 2: loop sequenziale per simbolo (evita overhead fork su dati grandi)
    rows = []
    for sym, df_full in loaded_data.items():
        sym_rows = _run_symbol(sym, df_full, periods, exitbars, fromdate, todate)
        rows.extend(sym_rows)
        if verbose:
            best_sh = max(r["sharpe"] for r in sym_rows)
            print(f"  {sym:6s}  {len(sym_rows)} combo  best_sharpe={best_sh:+.3f}", flush=True)

    return pd.DataFrame(rows)


# ── Aggregation ────────────────────────────────────────────────────────────────

def aggregate(detail: pd.DataFrame) -> pd.DataFrame:
    """
    Aggrega i risultati per (period, exitbar) su tutti i simboli.
    Metriche aggregate:
    - n_sym     : simboli con ≥1 trade
    - trades    : trade totali
    - mean_wr   : win rate medio
    - mean_exp  : expectancy media (% per trade)
    - total_pct : somma total_pct (non pesata — proxy volume P&L)
    - mean_sh   : Sharpe medio
    - min_sh    : Sharpe peggiore (robustezza)
    - mean_sqn  : SQN medio
    - min_sqn   : SQN peggiore
    - mean_dd   : max drawdown medio
    """
    g = detail.groupby(["period", "exitbar"])
    agg = pd.DataFrame({
        "n_sym":    g["symbol"].nunique(),
        "trades":   g["trades"].sum(),
        "mean_wr":  g["win_rate"].mean().round(1),
        "mean_exp": g["expect"].mean().round(4),
        "total_pct":g["total_pct"].sum().round(2),
        "mean_sh":  g["sharpe"].mean().round(3),
        "min_sh":   g["sharpe"].min().round(3),
        "mean_sqn": g["sqn"].mean().round(3),
        "min_sqn":  g["sqn"].min().round(3),
        "mean_dd":  g["max_dd"].mean().round(2),
    }).reset_index()
    return agg.sort_values("mean_sh", ascending=False)


def pivot_heatmap(agg: pd.DataFrame, metric: str = "mean_sh") -> pd.DataFrame:
    """Pivot period × exitbar → metric (per lettura rapida)."""
    p = agg.pivot(index="period", columns="exitbar", values=metric)
    p.columns.name = f"{metric} \\ exitbar"
    return p.round(3)


def print_report(detail: pd.DataFrame, agg: pd.DataFrame,
                 top_n: int = 10, show_detail: bool = False) -> None:

    baseline_row = agg[(agg["period"] == BASELINE[0]) & (agg["exitbar"] == BASELINE[1])]

    print("═" * 72)
    print(f"  HMA Sweep  period={DEFAULT_PERIODS}  exitbar={DEFAULT_EXITBARS}")
    print("═" * 72)

    print(f"\n── Baseline (period={BASELINE[0]}, exitbar={BASELINE[1]}) ──────────────")
    if not baseline_row.empty:
        b = baseline_row.iloc[0]
        print(f"  trades={int(b['trades'])}  mean_sh={b['mean_sh']:+.3f}  "
              f"min_sh={b['min_sh']:+.3f}  mean_sqn={b['mean_sqn']:+.3f}  "
              f"total_pct={b['total_pct']:+.2f}%")

    print(f"\n── Top {top_n} combinazioni (ordinato per Sharpe medio) ────────────────")
    cols = ["period", "exitbar", "trades", "mean_wr",
            "mean_exp", "mean_sh", "min_sh", "mean_sqn", "min_sqn"]
    print(agg[cols].head(top_n).to_string(index=False))

    print("\n── Heatmap Sharpe medio (period × exitbar) ──────────────────────────")
    h = pivot_heatmap(agg, "mean_sh")
    # Evidenzia baseline con *
    h_str = h.to_string()
    print(h_str)

    print("\n── Heatmap SQN medio ────────────────────────────────────────────────")
    print(pivot_heatmap(agg, "mean_sqn").to_string())

    if show_detail:
        print(f"\n── Dettaglio per simbolo (top 20 per Sharpe) ────────────────────────")
        cols_d = ["symbol", "period", "exitbar", "trades", "win_rate",
                  "expect", "total_pct", "sharpe", "sqn"]
        print(detail[cols_d].sort_values("sharpe", ascending=False)
              .head(20).to_string(index=False))


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="HMA grid sweep period × exitbar")
    p.add_argument("--symbols", nargs="+", default=HMA_TOP9)
    p.add_argument("--periods",  nargs="+", type=int, default=DEFAULT_PERIODS)
    p.add_argument("--exitbars", nargs="+", type=int, default=DEFAULT_EXITBARS)
    p.add_argument("--from",   dest="fromdate", default=None)
    p.add_argument("--to",     dest="todate",   default=None)
    p.add_argument("--out",    default=None, help="Salva dettaglio CSV")
    p.add_argument("--detail", action="store_true", help="Mostra righe per simbolo")
    p.add_argument("--jobs",   type=int, default=-1)
    return p.parse_args()


if __name__ == "__main__":
    import time
    args = parse_args()

    t0 = time.time()
    detail = run_sweep(
        symbols=args.symbols,
        periods=args.periods,
        exitbars=args.exitbars,
        fromdate=args.fromdate,
        todate=args.todate,
        n_jobs=args.jobs,
    )

    if detail.empty:
        sys.exit(1)

    agg = aggregate(detail)
    print_report(detail, agg, show_detail=args.detail)

    elapsed = time.time() - t0
    print(f"\nCompletato in {elapsed:.1f}s  "
          f"({len(detail)} simulazioni, {len(args.symbols)} simboli)")

    if args.out:
        detail.to_csv(args.out, index=False)
        print(f"Dettaglio salvato: {args.out}")
