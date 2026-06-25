#!/usr/bin/env python3
"""
HMA MAE/MFE Study — Stadio 3a: Probability Matrices per SL/TP strutturali

Per ogni trade (period=8, full RTH) calcola:
- MAE: massima escursione avversa durante il trade (% da entry)
- MFE: massima escursione favorevole durante il trade (% da entry)
- Per ogni lookback N: SL = min(low[-N:]) / TP = max(high[-N:]) prima dell'entry
- P(MAE >= SL): quanto spesso il prezzo tocca lo SL strutturale durante il trade
- P(MFE >= TP): quanto spesso il prezzo raggiunge il TP strutturale durante il trade
- P(SL hit | vincitori): quanto spesso fermiamo un trade che sarebbe stato vincente

Output:
- Tabella probability matrix per N ∈ {4, 6, 8, 10, 16}
- Stima del mean_pct/trade con bracket orders (lower bound)
- Distribuzione dei valori SL/TP distanza % dall'entry

Uso:
    python bt-strategy-test/HMA/mae_mfe_study.py
    python bt-strategy-test/HMA/mae_mfe_study.py --period 8 --lookbacks 4 6 8 10 16
    python bt-strategy-test/HMA/mae_mfe_study.py --symbols CSCO NFLX KHC
    python bt-strategy-test/HMA/mae_mfe_study.py --out /tmp/mae_mfe.csv
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
from engine.metrics import compute_metrics
from strategies.generic import SignalStrategy
from sim import load_symbol, compute_rth_signals, hma_signal, HMA_TOP9

DEFAULT_PERIOD   = 8
DEFAULT_EXITBAR  = 6
INVERTED         = True
EXITBAR_MODE     = "bt_style"
MINUTES_BF_CLOSE = 15
MINUTES_AF_OPEN  = 10
DEFAULT_LOOKBACKS = [4, 6, 8, 10, 16]


# ── Per-trade MAE/MFE + lookback levels ────────────────────────────────────────

def _analyze_symbol(
    sym: str,
    df_rth: pd.DataFrame,
    trades: pd.DataFrame,
    lookbacks: list[int],
) -> list[dict]:
    """
    Per ogni trade del simbolo calcola MAE, MFE e i livelli SL/TP strutturali
    derivati dalle lookback bar precedenti all'entry.
    """
    if len(trades) == 0:
        return []

    # Mappa timestamp → indice intero in df_rth
    ts_to_idx = {ts: i for i, ts in enumerate(df_rth.index)}

    highs  = df_rth["high"].values
    lows   = df_rth["low"].values

    rows = []
    for _, t in trades.iterrows():
        entry_idx = ts_to_idx.get(t["entry_time"])
        exit_idx  = ts_to_idx.get(t["exit_time"])
        if entry_idx is None or exit_idx is None:
            continue
        if exit_idx < entry_idx:
            continue

        entry_price = t["entry_price"]
        side        = t["side"]

        # ── MAE e MFE durante il trade (barre entry → exit incluse) ──────────
        h_trade = highs[entry_idx : exit_idx + 1]
        l_trade = lows[entry_idx  : exit_idx + 1]

        if side == "long":
            mae_pct = (entry_price - l_trade.min()) / entry_price * 100
            mfe_pct = (h_trade.max() - entry_price) / entry_price * 100
        else:
            mae_pct = (h_trade.max() - entry_price) / entry_price * 100
            mfe_pct = (entry_price - l_trade.min()) / entry_price * 100

        row = {
            "symbol":          sym,
            "side":            side,
            "entry_time":      t["entry_time"],
            "bars_held":       t["bars_held"],
            "exit_pct_natural": t["exit_pct"],
            "outcome":         t["outcome"],
            "mae_pct":         round(mae_pct, 5),
            "mfe_pct":         round(mfe_pct, 5),
        }

        # ── Livelli strutturali per ogni lookback ─────────────────────────────
        for N in lookbacks:
            lb_start = max(0, entry_idx - N)
            lb_end   = entry_idx  # escluso: barre PRIMA dell'entry

            if lb_end <= lb_start:
                row[f"sl_dist_{N}"] = np.nan
                row[f"tp_dist_{N}"] = np.nan
                continue

            h_lb = highs[lb_start : lb_end]
            l_lb = lows[lb_start  : lb_end]

            if side == "long":
                sl_level = l_lb.min()   # supporto strutturale
                tp_level = h_lb.max()   # resistenza strutturale
            else:
                sl_level = h_lb.max()   # resistenza strutturale
                tp_level = l_lb.min()   # supporto strutturale

            sl_dist = abs(entry_price - sl_level) / entry_price * 100
            tp_dist = abs(tp_level    - entry_price) / entry_price * 100

            row[f"sl_dist_{N}"] = round(sl_dist, 5)
            row[f"tp_dist_{N}"] = round(tp_dist, 5)

        rows.append(row)

    return rows


# ── Probability matrix ─────────────────────────────────────────────────────────

def build_probability_matrix(
    detail: pd.DataFrame,
    lookbacks: list[int],
    slippages: list[float] = [0.01, 0.02, 0.03],
) -> pd.DataFrame:
    """
    Per ogni lookback N:
    - median/p25/p75 delle distanze SL e TP dall'entry
    - P(SL hit): fraction dove MAE >= SL_dist
    - P(SL hit | vincitori): idem ma solo su trade con exit_pct_natural > 0
    - P(TP hit): fraction dove MFE >= TP_dist
    - mean_pct_bracket: stima mean_pct con bracket orders (lower bound)
    - sh_bracket: Sharpe stimato con bracket orders
    """
    rows = []
    n_total = len(detail)

    for N in lookbacks:
        sc = f"sl_dist_{N}"
        tc = f"tp_dist_{N}"
        valid = detail.dropna(subset=[sc, tc])
        if len(valid) == 0:
            continue

        sl_vals = valid[sc].values
        tp_vals = valid[tc].values
        mae     = valid["mae_pct"].values
        mfe     = valid["mfe_pct"].values
        nat_pct = valid["exit_pct_natural"].values
        is_win  = nat_pct > 0

        sl_hit    = mae >= sl_vals
        tp_hit    = mfe >= tp_vals

        p_sl      = sl_hit.mean()
        p_tp      = tp_hit.mean()
        p_sl_win  = sl_hit[is_win].mean() if is_win.sum() > 0 else np.nan

        # ── Stima mean_pct con bracket orders ────────────────────────────────
        # Caso semplificato (lower bound senza re-simulazione):
        #   solo_TP:  MFE >= TP e MAE < SL  → exit a +tp_dist
        #   solo_SL:  MAE >= SL e MFE < TP  → exit a -sl_dist
        #   entrambi: conservativo → usa exit naturale
        #   nessuno:  exit naturale
        solo_tp  = tp_hit & ~sl_hit
        solo_sl  = sl_hit & ~tp_hit
        both     = tp_hit & sl_hit
        neither  = ~tp_hit & ~sl_hit

        simulated_pct = np.where(solo_tp,  tp_vals,
                        np.where(solo_sl, -sl_vals,
                                          nat_pct))   # both e neither → naturale
        mean_pct_bracket = simulated_pct.mean()
        improvement      = mean_pct_bracket - nat_pct.mean()

        # Sharpe bracket (approssimato)
        std_bracket = simulated_pct.std(ddof=1)
        sh_bracket  = (simulated_pct.mean() / std_bracket * np.sqrt(252)
                       if std_bracket > 0 else 0.0)

        rows.append({
            "N":              N,
            "n_trades":       len(valid),
            # SL
            "sl_med%":        round(np.median(sl_vals), 4),
            "sl_p25%":        round(np.percentile(sl_vals, 25), 4),
            "sl_p75%":        round(np.percentile(sl_vals, 75), 4),
            "P(SL_hit)":      round(p_sl * 100, 1),
            "P(SL|win)":      round(p_sl_win * 100, 1) if not np.isnan(p_sl_win) else np.nan,
            # TP
            "tp_med%":        round(np.median(tp_vals), 4),
            "tp_p25%":        round(np.percentile(tp_vals, 25), 4),
            "tp_p75%":        round(np.percentile(tp_vals, 75), 4),
            "P(TP_hit)":      round(p_tp * 100, 1),
            # Bracket estimate
            "mean_nat%":      round(nat_pct.mean(), 5),
            "mean_brk%":      round(mean_pct_bracket, 5),
            "delta%":         round(improvement, 5),
            "sh_brk":         round(sh_bracket, 3),
            # breakdown
            "only_TP%":       round(solo_tp.mean() * 100, 1),
            "only_SL%":       round(solo_sl.mean() * 100, 1),
            "both%":          round(both.mean() * 100, 1),
            "neither%":       round(neither.mean() * 100, 1),
        })

    return pd.DataFrame(rows)


# ── Main run ───────────────────────────────────────────────────────────────────

def run_study(
    symbols: list,
    period: int,
    exitbar: int,
    lookbacks: list[int],
    fromdate: str | None = None,
    todate:   str | None = None,
    verbose: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Ritorna (detail, matrix):
    - detail: DataFrame un record per trade con MAE, MFE, sl_dist_N, tp_dist_N
    - matrix: probability matrix aggregata
    """
    if verbose:
        print(f"Caricamento dati ({len(symbols)} simboli)...", flush=True)

    strat  = SignalStrategy(signal_fn=hma_signal, period=period, inverted=INVERTED)
    engine = PandasBacktest(
        exitbar=exitbar, exitbar_mode=EXITBAR_MODE,
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

        df_rth, sig_rth = compute_rth_signals(
            df_full, hma_signal, period=period, inverted=INVERTED
        )
        trades = engine.run(df_rth, strat, symbol=sym, signals=sig_rth)
        if len(trades) == 0:
            continue

        rows = _analyze_symbol(sym, df_rth, trades, lookbacks)
        all_rows.extend(rows)
        if verbose:
            print(f"  {sym:6s}  {len(trades):5d} trades  "
                  f"mae_med={np.median([r['mae_pct'] for r in rows]):+.4f}%  "
                  f"mfe_med={np.median([r['mfe_pct'] for r in rows]):+.4f}%",
                  flush=True)

    if not all_rows:
        return pd.DataFrame(), pd.DataFrame()

    detail = pd.DataFrame(all_rows)
    matrix = build_probability_matrix(detail, lookbacks)
    return detail, matrix


# ── Report ─────────────────────────────────────────────────────────────────────

def print_report(detail: pd.DataFrame, matrix: pd.DataFrame,
                 period: int, lookbacks: list[int]) -> None:

    print("\n" + "═" * 72)
    print(f"  HMA MAE/MFE Study  period={period}  lookbacks={lookbacks}")
    print("═" * 72)

    # ── Distribuzione globale MAE/MFE ──────────────────────────────────────
    n = len(detail)
    print(f"\n── Distribuzione MAE/MFE globale ({n:,} trades) ──────────────────────")
    for label, col in [("MAE (avverso)", "mae_pct"), ("MFE (favorevole)", "mfe_pct")]:
        v = detail[col]
        print(f"  {label:20s}  "
              f"med={v.median():+.4f}%  "
              f"p25={v.quantile(.25):+.4f}%  "
              f"p75={v.quantile(.75):+.4f}%  "
              f"p90={v.quantile(.90):+.4f}%  "
              f"p95={v.quantile(.95):+.4f}%")

    # ── MAE/MFE per outcome ───────────────────────────────────────────────
    print(f"\n── MAE/MFE per tipo di uscita ───────────────────────────────────────")
    for outcome in ["signal", "exitbar", "eod"]:
        sub = detail[detail["outcome"] == outcome]
        if len(sub) == 0:
            continue
        wins  = sub[sub["exit_pct_natural"] > 0]
        loses = sub[sub["exit_pct_natural"] <= 0]
        print(f"  {outcome:8s}  n={len(sub):6d}  "
              f"mae_med={sub['mae_pct'].median():+.4f}%  "
              f"mfe_med={sub['mfe_pct'].median():+.4f}%  "
              f"[wins={len(wins)} mae={wins['mae_pct'].median():+.4f}%"
              f"  los={len(loses)} mae={loses['mae_pct'].median():+.4f}%]")

    # ── Probability matrix ────────────────────────────────────────────────
    print(f"\n── Probability Matrix (livelli strutturali da lookback N barre) ─────")
    print(f"\n  {'N':>3}  {'sl_med%':>8}  {'sl_p25-75':>12}  "
          f"{'P(SL)':>7}  {'P(SL|win)':>10}  "
          f"{'tp_med%':>8}  {'tp_p25-75':>12}  {'P(TP)':>7}")
    print("  " + "-" * 76)
    for _, r in matrix.iterrows():
        print(f"  {int(r['N']):>3}  "
              f"{r['sl_med%']:>+8.4f}%  "
              f"[{r['sl_p25%']:+.3f}–{r['sl_p75%']:+.3f}]  "
              f"{r['P(SL_hit)']:>6.1f}%  "
              f"{r['P(SL|win)']:>9.1f}%  "
              f"{r['tp_med%']:>+8.4f}%  "
              f"[{r['tp_p25%']:+.3f}–{r['tp_p75%']:+.3f}]  "
              f"{r['P(TP_hit)']:>6.1f}%")

    # ── Stima bracket orders ──────────────────────────────────────────────
    print(f"\n── Stima P&L con bracket orders (lower bound, no re-sim) ────────────")
    print(f"  {'N':>3}  {'mean_nat%':>10}  {'mean_brk%':>10}  {'delta%':>8}  "
          f"{'sh_brk':>7}  {'only_TP':>8}  {'only_SL':>8}  {'both':>6}  {'neither':>8}")
    print("  " + "-" * 76)
    for _, r in matrix.iterrows():
        print(f"  {int(r['N']):>3}  "
              f"{r['mean_nat%']:>+10.5f}  "
              f"{r['mean_brk%']:>+10.5f}  "
              f"{r['delta%']:>+8.5f}  "
              f"{r['sh_brk']:>+7.3f}  "
              f"{r['only_TP%']:>7.1f}%  "
              f"{r['only_SL%']:>7.1f}%  "
              f"{r['both%']:>5.1f}%  "
              f"{r['neither%']:>7.1f}%")

    # ── Indicatore "qualità bracket" ──────────────────────────────────────
    print(f"\n── Indicatore: P(TP) / P(SL|win)  [più alto = meglio] ──────────────")
    for _, r in matrix.iterrows():
        ratio = r["P(TP_hit)"] / r["P(SL|win)"] if r["P(SL|win)"] > 0 else np.inf
        bar = "█" * int(ratio * 5)
        print(f"  N={int(r['N']):>2}  P(TP)={r['P(TP_hit)']:5.1f}%  "
              f"P(SL|win)={r['P(SL|win)']:5.1f}%  ratio={ratio:5.2f}  {bar}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="HMA MAE/MFE probability matrix study")
    p.add_argument("--symbols",   nargs="+", default=HMA_TOP9)
    p.add_argument("--period",    type=int,  default=DEFAULT_PERIOD)
    p.add_argument("--exitbar",   type=int,  default=DEFAULT_EXITBAR)
    p.add_argument("--lookbacks", nargs="+", type=int, default=DEFAULT_LOOKBACKS)
    p.add_argument("--from",      dest="fromdate", default=None)
    p.add_argument("--to",        dest="todate",   default=None)
    p.add_argument("--out",       default=None, help="Salva detail CSV")
    p.add_argument("--quiet",     action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    import time
    args = parse_args()
    t0 = time.time()

    detail, matrix = run_study(
        symbols=args.symbols,
        period=args.period,
        exitbar=args.exitbar,
        lookbacks=args.lookbacks,
        fromdate=args.fromdate,
        todate=args.todate,
        verbose=not args.quiet,
    )

    if detail.empty:
        print("Nessun trade.", file=sys.stderr)
        sys.exit(1)

    print_report(detail, matrix, args.period, args.lookbacks)

    elapsed = time.time() - t0
    print(f"\nCompletato in {elapsed:.1f}s  ({len(detail):,} trades analizzati)")

    if args.out:
        detail.to_csv(args.out, index=False)
        print(f"Detail salvato: {args.out}")
