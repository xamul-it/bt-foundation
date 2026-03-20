#!/usr/bin/env python3
"""
HMA overnight analysis — perché use_calendar=False dà risultati migliori
e perché NON è applicabile in paper trading.

Uso:
    python bin/HMA/overnight_analysis.py
    python bin/HMA/overnight_analysis.py --symbols NFLX CSCO
    python bin/HMA/overnight_analysis.py --from 2025-01-02 --to 2025-05-30
"""
import sys
from pathlib import Path

AGENT_ROOT = Path(__file__).resolve().parent.parent.parent / "bt-agent"
HMA_DIR    = Path(__file__).resolve().parent
sys.path.insert(0, str(AGENT_ROOT))
sys.path.insert(0, str(HMA_DIR))

import argparse
import pandas as pd
import numpy as np

from engine.metrics import compute_metrics
from sim import run_sim, hma_signal, HMA_TOP9, load_symbol
from strategies.generic import SignalStrategy
from engine.backtest import PandasBacktest


# ── helpers ───────────────────────────────────────────────────────────────────

def _is_overnight(row) -> bool:
    """True se entry e exit sono in giorni diversi."""
    return row["entry_time"].date() != row["exit_time"].date()


def run_overnight_sim(symbols, fromdate=None, todate=None,
                      period=16, exitbar=6, sl_pct=0.0, verbose=True):
    """Simula con overnight=True (use_calendar=False)."""
    strategy = SignalStrategy(signal_fn=hma_signal, period=period, inverted=True)
    engine = PandasBacktest(
        sl_pct=sl_pct,
        exitbar=exitbar,
        exitbar_mode="bt_style",
        minutes_before_close=15,
        overnight=True,
    )
    all_trades, rows = [], []
    for sym in symbols:
        try:
            df = load_symbol(sym, fromdate, todate)
        except FileNotFoundError as e:
            print(f"  [SKIP] {e}", file=sys.stderr)
            continue
        trades = engine.run(df, strategy, symbol=sym)
        all_trades.append(trades)
        if len(trades) == 0:
            continue
        m = compute_metrics(trades)
        rows.append({
            "symbol":    sym,
            "trades":    len(trades),
            "win_rate":  round(m["win_rate"] * 100, 1),
            "total_pct": round(trades["exit_pct"].sum(), 4),
            "sharpe":    round(m["sharpe"], 2),
            "sqn":       round(m["sqn"], 2),
        })
        if verbose:
            r = rows[-1]
            print(f"  {sym:6s}  {r['trades']:3d} trades  wr={r['win_rate']:5.1f}%  "
                  f"total={r['total_pct']:+.4f}%  sharpe={r['sharpe']:+.2f}  sqn={r['sqn']:+.2f}")

    trades_df  = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    summary_df = pd.DataFrame(rows)
    return trades_df, summary_df


def analyze_gap_contribution(intraday_trades: pd.DataFrame,
                              overnight_trades: pd.DataFrame,
                              symbols: list, fromdate=None, todate=None) -> pd.DataFrame:
    """
    Per ogni trade overnight: calcola il gap notturno (close→open giorno dopo)
    e la sua contribuzione al P&L del trade.
    """
    rows = []
    for sym in symbols:
        try:
            df = load_symbol(sym, fromdate, todate)
        except FileNotFoundError:
            continue

        # Calcola gap notturno per ogni giorno: close[16:00] → open[9:30 next day]
        daily_last  = df.groupby(df.index.date)["close"].last()
        daily_first = df.groupby(df.index.date)["open"].first()
        days = sorted(daily_last.index)
        gaps = {}
        for i in range(len(days) - 1):
            d0, d1 = days[i], days[i + 1]
            gap_pct = (daily_first[d1] / daily_last[d0] - 1) * 100
            gaps[d0] = {"close": daily_last[d0], "next_open": daily_first[d1], "gap_pct": gap_pct}

        # Trova trade overnight da overnight_trades
        sym_trades = overnight_trades[overnight_trades["symbol"] == sym] if not overnight_trades.empty else pd.DataFrame()
        for _, t in sym_trades.iterrows():
            if not _is_overnight(t):
                continue
            entry_day = t["entry_time"].date()
            g = gaps.get(entry_day)
            if g is None:
                continue
            gap_pct = g["gap_pct"]
            gap_in_favor = gap_pct if t["side"] == "long" else -gap_pct
            rows.append({
                "symbol":        sym,
                "entry_time":    t["entry_time"],
                "exit_time":     t["exit_time"],
                "side":          t["side"],
                "exit_pct":      round(t["exit_pct"], 4),
                "gap_pct":       round(gap_pct, 4),
                "gap_in_favor":  round(gap_in_favor, 4),
                "gap_share_pct": round(gap_in_favor / t["exit_pct"] * 100, 1) if abs(t["exit_pct"]) > 1e-6 else 0,
            })
    return pd.DataFrame(rows)


def print_separator(title=""):
    w = 72
    if title:
        pad = (w - len(title) - 2) // 2
        print("\n" + "─" * pad + f" {title} " + "─" * (w - pad - len(title) - 2))
    else:
        print("\n" + "─" * w)


# ── main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="HMA overnight analysis")
    p.add_argument("--symbols",  nargs="+", default=HMA_TOP9)
    p.add_argument("--from",     dest="fromdate", default=None)
    p.add_argument("--to",       dest="todate",   default=None)
    p.add_argument("--period",   type=int,   default=16)
    p.add_argument("--exitbar",  type=int,   default=6)
    p.add_argument("--sl",       type=float, default=0.0)
    return p.parse_args()


def main():
    args = parse_args()
    kw = dict(symbols=args.symbols, fromdate=args.fromdate, todate=args.todate,
              period=args.period, exitbar=args.exitbar, sl_pct=args.sl, verbose=False)

    # ── 1. Run entrambe le modalità ───────────────────────────────────────────
    print_separator("use_calendar=True  (intraday, EOD close)")
    id_trades, id_summary = run_sim(**kw)
    print(id_summary.to_string(index=False) if not id_summary.empty else "  nessun trade")

    print_separator("use_calendar=False  (overnight, nessun EOD close)")
    on_trades, on_summary = run_overnight_sim(**kw)
    print(on_summary.to_string(index=False) if not on_summary.empty else "  nessun trade")

    # ── 2. Delta per simbolo ──────────────────────────────────────────────────
    if not id_summary.empty and not on_summary.empty:
        print_separator("Delta  (overnight − intraday)")
        merged = id_summary[["symbol", "trades", "total_pct", "sharpe", "sqn"]].merge(
            on_summary[["symbol", "trades", "total_pct", "sharpe", "sqn"]],
            on="symbol", suffixes=("_intra", "_over")
        )
        merged["Δtrades"]    = merged["trades_over"]    - merged["trades_intra"]
        merged["Δtotal_pct"] = (merged["total_pct_over"] - merged["total_pct_intra"]).round(2)
        merged["Δsharpe"]    = (merged["sharpe_over"]    - merged["sharpe_intra"]).round(2)
        merged["Δsqn"]       = (merged["sqn_over"]       - merged["sqn_intra"]).round(2)
        print(merged[["symbol", "Δtrades", "Δtotal_pct", "Δsharpe", "Δsqn"]].to_string(index=False))

    # ── 3. Anatomia dei trade overnight ───────────────────────────────────────
    if not on_trades.empty:
        on_trades["is_overnight"] = on_trades.apply(_is_overnight, axis=1)
        n_total    = len(on_trades)
        n_over     = on_trades["is_overnight"].sum()
        pnl_over   = on_trades[on_trades["is_overnight"]]["exit_pct"].sum()
        pnl_intra  = on_trades[~on_trades["is_overnight"]]["exit_pct"].sum()

        print_separator("Anatomia trade overnight")
        print(f"  Trade totali:        {n_total}")
        print(f"  Trade overnight:     {n_over}  ({n_over/n_total*100:.1f}%)")
        print(f"  PnL trade intraday:  {pnl_intra:+.2f}%")
        print(f"  PnL trade overnight: {pnl_over:+.2f}%")
        if n_over > 0:
            wr_over = (on_trades[on_trades["is_overnight"]]["exit_pct"] > 0).mean()
            print(f"  Win rate overnight:  {wr_over*100:.1f}%")

    # ── 4. Contribuzione del gap notturno ─────────────────────────────────────
    if not on_trades.empty:
        gap_df = analyze_gap_contribution(id_trades, on_trades, args.symbols,
                                          args.fromdate, args.todate)
        if not gap_df.empty:
            print_separator("Contribuzione gap notturno ai trade overnight")
            avg_gap    = gap_df["gap_pct"].mean()
            avg_favor  = gap_df["gap_in_favor"].mean()
            avg_share  = gap_df["gap_share_pct"].median()
            pos_gaps   = (gap_df["gap_in_favor"] > 0).mean()
            print(f"  Gap medio assoluto:           {avg_gap:+.4f}%")
            print(f"  Gap medio a favore posizione: {avg_favor:+.4f}%")
            print(f"  Gap favorevole % casi:        {pos_gaps*100:.1f}%")
            print(f"  Gap = mediana % del P&L trade: {avg_share:.1f}%")
            print(f"\n  Primi 10 trade overnight per impatto gap:")
            top = gap_df.nlargest(10, "gap_in_favor")[
                ["symbol", "entry_time", "side", "exit_pct", "gap_pct", "gap_in_favor", "gap_share_pct"]
            ]
            print(top.to_string(index=False))

    # ── 5. Perché NON è applicabile in paper ──────────────────────────────────
    print_separator("Perché overnight NON è applicabile in paper trading")
    print("""
  1. FILL OVERNIGHT NON ESEGUIBILI
     Alpaca paper esegue ordini limite solo durante RTH (9:30-16:00).
     Un close/reverse overnight non ha controparte: l'ordine scade o
     viene eseguito all'open del giorno dopo con slippage imprevedibile.

  2. GAP RISK CATASTROFICO SENZA SL
     sl_pct=0 in env/psim. Un gap notturno contro posizione su notizie
     (earnings, macro) può essere -5%/-10% in un minuto. In backtest
     questo è un rischio "nascosto" perché i dati minute coprono solo RTH:
     il gap appare come una singola barra 9:30, il P&L sembra normale.
     In paper/live il conto subisce la perdita intera senza possibilità
     di stop in tempo reale.

  3. BIAS DA TREND RIALZISTA 2025
     I dati coprono gen-mag 2025. Il NASDAQ ha avuto bias rialzista
     in questo periodo. Le posizioni long overnight hanno catturato
     sistematicamente gap-up mattutini. Questo NON è replicabile
     su un futuro orizzonte non noto.

  4. MARGINE E OVERNIGHT BUYING POWER
     Alpaca applica overnight margin diverso dal day-trading margin.
     Con amount=2000 per simbolo su 9 simboli = $18k esposto overnight.
     Il broker può fare margin call se il NAV scende sotto il requisito.

  5. DATI RTH ONLY → SIMULAZIONE INCOMPLETA
     I CSV in config-common/data/minutes/ contengono solo barre RTH
     (9:30-16:00). La simulazione overnight "salta" dal close al next open
     senza vedere after-hours e pre-market. Il BT backtest fa lo stesso.
     Entrambi sovrastimano il P&L rispetto alla realtà dove l'AH e il PM
     possono muoversi significativamente prima dell'open RTH.

  CONCLUSIONE: use_calendar=False è utile per misurare il premio overnight
  come fonte di alpha, NON come strategia operativa senza risk management
  dedicato (stop loss overnight, position sizing ridotto, filtro earnings).
""")


if __name__ == "__main__":
    main()
