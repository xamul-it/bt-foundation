#!/usr/bin/env python3
"""
HMA pandas simulation — replica di intraday.HMA con env/psim.

Parametri (da env/psim):
    period=16  inverted=True  exitbar=6  minutes_before_close=15
    sl_pct=0   tp_pct=0       atr_min_pct=0

Signal: edge-detection (identico a intraday.HMA in bt-core).
    long  = HMA passa da rising  a falling (inverted=True)
    short = HMA passa da falling a rising  (inverted=True)

Uso:
    python bt-strategy-test/HMA/sim.py
    python bt-strategy-test/HMA/sim.py --symbols NFLX PLTR
    python bt-strategy-test/HMA/sim.py --from 2025-01-01 --to 2025-03-31
    python bt-strategy-test/HMA/sim.py --out /tmp/hma_trades.csv
"""
import sys
from pathlib import Path

# ── path setup ────────────────────────────────────────────────────────────────
AGENT_ROOT = Path(__file__).resolve().parents[2] / "bt-agent"
sys.path.insert(0, str(AGENT_ROOT))

import argparse
import pandas as pd

from paths import DATA_DIR
from indicators.hma import hma
from strategies.generic import SignalStrategy
from engine.backtest import PandasBacktest
from engine.metrics import compute_metrics

# ── psim defaults ─────────────────────────────────────────────────────────────
PERIOD             = 16
INVERTED           = True
EXITBAR            = 6
MINUTES_BF_CLOSE   = 15
SL_PCT             = 0.0
TP_PCT             = None

HMA_TOP9 = ["KHC", "PLTR", "CSX", "KDP", "INTC", "CSCO", "NFLX", "WBD", "CMCSA"]


# ── signal function ───────────────────────────────────────────────────────────

def hma_signal(df: pd.DataFrame, period: int = 16, inverted: bool = True, **_kw) -> pd.Series:
    """
    Edge-detection signal identico a intraday.HMA (bt-core).

    Con inverted=True (contrarian):
        long  = HMA stava salendo  → ora scende   (falling_now AND NOT falling_prev)
        short = HMA stava scendendo → ora sale    (rising_now  AND NOT rising_prev)

    Con inverted=False (trend-following):
        long  = HMA stava scendendo → ora sale
        short = HMA stava salendo  → ora scende
    """
    h = hma(df["close"], period)

    rising_now   = h.shift(1) < h       # prev < curr
    falling_now  = h.shift(1) > h       # prev > curr
    rising_prev  = h.shift(2) < h.shift(1)
    falling_prev = h.shift(2) > h.shift(1)

    if inverted:
        long_cond_now  = falling_now
        short_cond_now = rising_now
        long_cond_prev  = falling_prev
        short_cond_prev = rising_prev
    else:
        long_cond_now  = rising_now
        short_cond_now = falling_now
        long_cond_prev  = rising_prev
        short_cond_prev = falling_prev

    long_edge  = long_cond_now  & ~long_cond_prev
    short_edge = short_cond_now & ~short_cond_prev

    valid = h.notna() & h.shift(1).notna() & h.shift(2).notna()
    sig = pd.Series(0, index=df.index, dtype=int)
    sig[valid & long_edge]  = 1
    sig[valid & short_edge] = -1
    return sig


# ── data loader ───────────────────────────────────────────────────────────────

RTH_START = pd.Timestamp("09:30:00").time()  # ET
RTH_END   = pd.Timestamp("16:00:00").time()  # ET


def load_symbol(
    symbol: str,
    fromdate: str | None,
    todate: str | None,
    rth_only: bool = False,
) -> pd.DataFrame:
    """
    Carica CSV Alpaca e restituisce DataFrame in timezone America/New_York.

    rth_only=False (default): ritorna tutti i dati (RTH + AH + PM).
    rth_only=True: filtra a sole barre RTH (09:30–15:59 ET).

    Nota: per la simulazione intraday usare rth_only=False e calcolare i segnali
    sul dataset completo (replica il comportamento di BT che aggiorna gli indicatori
    su tutte le barre ma esegue la logica di trading solo su quelle RTH).
    """
    path = DATA_DIR / f"{symbol}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Dati non trovati: {path}")

    df = pd.read_csv(path)

    # colonne possibili: timestamp/datetime/date
    ts_col = next((c for c in df.columns if c.lower() in ("timestamp", "datetime", "date")), None)
    if ts_col is None:
        raise ValueError(f"Colonna timestamp non trovata in {path}")

    df[ts_col] = pd.to_datetime(df[ts_col], utc=True)
    df = df.set_index(ts_col).sort_index()
    df.index.name = "datetime"
    # rimuovi duplicati di timestamp (alcuni CSV Alpaca hanno righe duplicate)
    if not df.index.is_unique:
        df = df[~df.index.duplicated(keep="first")]

    # normalizza nomi colonne
    df.columns = [c.lower() for c in df.columns]

    # Converti a ET — necessario per il cutoff 15:45 ET dell'engine
    df.index = df.index.tz_convert("America/New_York")

    if fromdate:
        df = df[df.index >= pd.Timestamp(fromdate, tz="America/New_York")]
    if todate:
        df = df[df.index <= pd.Timestamp(todate + " 23:59:59", tz="America/New_York")]

    if rth_only:
        t = df.index.time
        df = df[(t >= RTH_START) & (t < RTH_END)]

    return df


def compute_rth_signals(
    df_full: pd.DataFrame,
    signal_fn,
    rth_start: "time" = RTH_START,
    rth_end: "time"   = RTH_END,
    **signal_params,
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Calcola i segnali sul dataset completo (AH+PM+RTH) e restituisce:
    - df_rth : DataFrame filtrato RTH-only (per la simulazione)
    - signals_rth : segnali con non-RTH azzerati (allineati a df_rth.index)

    Questo replica il comportamento di BT use_calendar=True:
    - gli indicatori (HMA) vengono aggiornati su TUTTE le barre → continuità
    - la logica di entry/exit viene eseguita solo su barre RTH
    """
    from datetime import time as _time

    # Calcola segnali su tutti i dati (continuità HMA attraverso AH/PM)
    signals_full = signal_fn(df_full, **signal_params)

    # Azzera segnali fuori RTH
    t = signals_full.index.time
    rth_mask = (t >= rth_start) & (t < rth_end)
    signals_rth = signals_full.where(rth_mask, 0)

    # Filtra il DataFrame a RTH-only
    df_rth = df_full[rth_mask]

    return df_rth, signals_rth


# ── main ──────────────────────────────────────────────────────────────────────

def run_sim(
    symbols: list[str],
    fromdate: str | None = None,
    todate: str | None = None,
    period: int = PERIOD,
    inverted: bool = INVERTED,
    exitbar: int = EXITBAR,
    minutes_before_close: int = MINUTES_BF_CLOSE,
    sl_pct: float = SL_PCT,
    tp_pct: float | None = TP_PCT,
    sl_on_close: bool = False,
    verbose: bool = True,
) -> pd.DataFrame:
    strategy = SignalStrategy(
        signal_fn=hma_signal,
        period=period,
        inverted=inverted,
    )
    engine = PandasBacktest(
        sl_pct=sl_pct,
        tp_pct=tp_pct,
        exitbar=exitbar,
        exitbar_mode="bt_style",
        minutes_before_close=minutes_before_close,
        minutes_after_open=10,  # replica BT: inValidMarket blocca entry nei primi 10 min
        sl_on_close=sl_on_close,  # True = replica bt-core (check su close), False = realistico (low/high)
    )

    all_trades = []
    summary_rows = []

    for sym in symbols:
        try:
            # Carica dati completi (AH+PM+RTH) per continuità HMA — replica BT
            df_full = load_symbol(sym, fromdate, todate, rth_only=False)
        except FileNotFoundError as e:
            print(f"  [SKIP] {e}", file=sys.stderr)
            continue

        # Calcola segnali su tutti i dati, poi filtra a RTH per la simulazione
        df_rth, signals_rth = compute_rth_signals(
            df_full, hma_signal, period=period, inverted=inverted
        )

        if df_rth.empty:
            continue

        date_range = f"{df_rth.index[0].date()} → {df_rth.index[-1].date()}  ({len(df_rth)} bars)"
        trades = engine.run(df_rth, strategy, symbol=sym, signals=signals_rth)
        all_trades.append(trades)

        if len(trades) == 0:
            if verbose:
                print(f"  {sym:6s}  [{date_range}]  0 trades")
            continue

        m = compute_metrics(trades)
        summary_rows.append({
            "symbol":    sym,
            "trades":    len(trades),
            "win_rate":  round(m.get("win_rate", 0) * 100, 1),
            "expect":    round(m.get("expectancy_pct", 0), 4),
            "total_pct": round(trades["exit_pct"].sum(), 4),
            "sharpe":    round(m.get("sharpe", 0), 2),
            "max_dd":    round(m.get("max_drawdown_pct", 0), 2),
            "sqn":       round(m.get("sqn", 0), 2),
        })
        if verbose:
            r = summary_rows[-1]
            print(
                f"  {sym:6s}  [{date_range}]  {r['trades']:3d} trades  "
                f"wr={r['win_rate']:5.1f}%  "
                f"expect={r['expect']:+.4f}%  "
                f"total={r['total_pct']:+.4f}%  "
                f"sharpe={r['sharpe']:+.2f}  "
                f"sqn={r['sqn']:+.2f}"
            )

    trades_df = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    summary_df = pd.DataFrame(summary_rows)
    return trades_df, summary_df


def parse_args():
    p = argparse.ArgumentParser(description="HMA pandas simulation (replica env/psim)")
    p.add_argument("--symbols", nargs="+", default=HMA_TOP9,
                   help="Simboli da simulare (default: HMA_top9)")
    p.add_argument("--from", dest="fromdate", default=None, help="Data inizio YYYY-MM-DD")
    p.add_argument("--to",   dest="todate",   default=None, help="Data fine   YYYY-MM-DD")
    p.add_argument("--period", type=int,   default=PERIOD)
    p.add_argument("--exitbar", type=int,  default=EXITBAR)
    p.add_argument("--sl",           type=float, default=SL_PCT, help="Stop loss %% (0=off)")
    p.add_argument("--sl-on-close",  action="store_true",        help="Check SL su close (replica bt-core)")
    p.add_argument("--out",          default=None,               help="Salva trades CSV in questo path")
    p.add_argument("--quiet",        action="store_true",        help="Nessun output per simbolo")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    print(f"\nHMA Simulation  period={args.period}  inverted={INVERTED}  "
          f"exitbar={args.exitbar}  sl={args.sl}  mode=bt_style\n")

    trades_df, summary_df = run_sim(
        symbols=args.symbols,
        fromdate=args.fromdate,
        todate=args.todate,
        period=args.period,
        exitbar=args.exitbar,
        sl_pct=args.sl,
        sl_on_close=args.sl_on_close,
        verbose=not args.quiet,
    )

    if not summary_df.empty:
        print("\n── Riepilogo ─────────────────────────────────────────────")
        print(summary_df.to_string(index=False))
        print(f"\nTotale trades: {len(trades_df)}")

    if args.out and not trades_df.empty:
        trades_df.to_csv(args.out, index=False)
        print(f"\nTrades salvati: {args.out}")
