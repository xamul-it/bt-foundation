#!/usr/bin/env python3
"""
TEMPLATE — simulazione pandas per una nuova strategia.

Istruzioni:
  1. Copia questa cartella in bin/{NOME_STRATEGIA}/
  2. Modifica MY_SIGNAL_FN con la tua logica
  3. Imposta DEFAULTS con i parametri di default
  4. Esegui: python bin/{NOME_STRATEGIA}/sim.py

La signal_fn è l'UNICA cosa che devi scrivere.
Il resto (engine, metrics, report, MC) è già nel modulo bt-agent.

Signal fn contract:
    Input : df  — DataFrame OHLCV con colonne open/high/low/close/volume
                  e indice DatetimeIndex (UTC)
    Output: pd.Series con indice = df.index e valori:
                1  = long
               -1  = short
                0  = nessun segnale
"""
import sys
from pathlib import Path

AGENT_ROOT = Path(__file__).resolve().parent.parent.parent / "bt-agent"
sys.path.insert(0, str(AGENT_ROOT))

import argparse
import pandas as pd

from paths import DATA_DIR
from strategies.generic import SignalStrategy
from engine.backtest import PandasBacktest
from engine.metrics import compute_metrics

# ── CONFIGURAZIONE ────────────────────────────────────────────────────────────

STRATEGY_NAME = "MyStrategy"   # cambia con il nome della tua strategia

DEFAULTS = {
    "symbols":   ["NFLX"],       # simboli di default
    "period":    20,             # parametro esempio
    "exitbar":   6,
    "sl_pct":    0.0,
    "minutes_before_close": 15,
}


# ── SCRIVI QUI LA TUA SIGNAL FUNCTION ────────────────────────────────────────

def my_signal(df: pd.DataFrame, period: int = 20, **_kw) -> pd.Series:
    """
    Sostituisci questa funzione con la tua logica.

    Esempio: segnale sul crossover della media mobile semplice.
    """
    close = df["close"]
    ma = close.rolling(period).mean()

    sig = pd.Series(0, index=df.index, dtype=int)
    sig[close > ma] = 1    # sopra la media → long
    sig[close < ma] = -1   # sotto la media → short
    return sig


# ── BOILERPLATE (non modificare) ──────────────────────────────────────────────

def load_symbol(symbol: str, fromdate=None, todate=None) -> pd.DataFrame:
    path = DATA_DIR / f"{symbol}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Dati mancanti: {path}")
    df = pd.read_csv(path)
    ts_col = next(c for c in df.columns if c.lower() in ("timestamp", "datetime", "date"))
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True)
    df = df.set_index(ts_col).sort_index()
    df.columns = [c.lower() for c in df.columns]
    if fromdate:
        df = df[df.index >= pd.Timestamp(fromdate, tz="UTC")]
    if todate:
        df = df[df.index <= pd.Timestamp(todate + " 23:59:59", tz="UTC")]
    return df


def run_sim(symbols, fromdate=None, todate=None, period=DEFAULTS["period"],
            exitbar=DEFAULTS["exitbar"], sl_pct=DEFAULTS["sl_pct"], verbose=True):
    strategy = SignalStrategy(signal_fn=my_signal, period=period)
    engine   = PandasBacktest(
        sl_pct=sl_pct,
        exitbar=exitbar,
        exitbar_mode="bt_style",
        minutes_before_close=DEFAULTS["minutes_before_close"],
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
            if verbose:
                print(f"  {sym}: 0 trades")
            continue
        m = compute_metrics(trades)
        rows.append({"symbol": sym, "trades": len(trades),
                     "win_rate": round(m.get("win_rate", 0) * 100, 1),
                     "total_pct": round(trades["exit_pct"].sum(), 4),
                     "sharpe": round(m.get("sharpe", 0), 2)})
        if verbose:
            r = rows[-1]
            print(f"  {sym}: {r['trades']} trades  wr={r['win_rate']}%  "
                  f"total={r['total_pct']:+.4f}%  sharpe={r['sharpe']:+.2f}")
    trades_df  = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    summary_df = pd.DataFrame(rows)
    return trades_df, summary_df


def parse_args():
    p = argparse.ArgumentParser(description=f"{STRATEGY_NAME} pandas simulation")
    p.add_argument("--symbols",  nargs="+", default=DEFAULTS["symbols"])
    p.add_argument("--from",     dest="fromdate", default=None)
    p.add_argument("--to",       dest="todate",   default=None)
    p.add_argument("--period",   type=int,   default=DEFAULTS["period"])
    p.add_argument("--exitbar",  type=int,   default=DEFAULTS["exitbar"])
    p.add_argument("--sl",       type=float, default=DEFAULTS["sl_pct"])
    p.add_argument("--out",      default=None)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(f"\n{STRATEGY_NAME}  period={args.period}  exitbar={args.exitbar}  sl={args.sl}\n")
    trades_df, summary_df = run_sim(
        args.symbols, args.fromdate, args.todate,
        args.period, args.exitbar, args.sl,
    )
    if not summary_df.empty:
        print("\n── Riepilogo ─────────────────────────────────────")
        print(summary_df.to_string(index=False))
        print(f"\nTotale trades: {len(trades_df)}")
    if args.out and not trades_df.empty:
        trades_df.to_csv(args.out, index=False)
        print(f"Trades salvati: {args.out}")
