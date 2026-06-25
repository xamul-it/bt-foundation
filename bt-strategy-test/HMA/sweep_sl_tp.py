#!/usr/bin/env python3
"""
HMA SL/TP Sweep — Monte Carlo

Testa combinazioni SL×TP sulla strategia HMA usando campionamento casuale
dei giorni di trading (stile Monte Carlo) per rompere l'autocorrelazione.

Caratteristiche:
- Stessi giorni campionati per tutte le combo SL×TP (confronto fair)
- Usa high/low della barra per verificare SL/TP (non il close)
- Caso ambiguo (stessa barra tocca SL e TP): per default SL-first (worst-case)
- Seed fisso → risultati riproducibili

Uso:
    python3 bt-strategy-test/HMA/sweep_sl_tp.py
    python3 bt-strategy-test/HMA/sweep_sl_tp.py --samples 100 --seed 42
    python3 bt-strategy-test/HMA/sweep_sl_tp.py --optimistic    # TP-first come 01-hma_backtest.py
    python3 bt-strategy-test/HMA/sweep_sl_tp.py --no-atr        # SL×TP×time (senza ATR)
    python3 bt-strategy-test/HMA/sweep_sl_tp.py --no-time       # SL×TP×ATR (senza time filter)
    python3 bt-strategy-test/HMA/sweep_sl_tp.py --no-atr --no-time  # solo SL×TP (24 combo)
"""

import argparse
import random
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Parametri sweep
# ---------------------------------------------------------------------------
SL_VALUES  = [0.0, 0.003, 0.005, 0.010, 0.015, 0.020]   # 0 = disabled
TP_VALUES  = [0.0, 0.005, 0.010, 0.020]                   # 0 = disabled
ATR_VALUES = [0.0, 0.001, 0.0015, 0.002, 0.003]            # 0 = disabled (atr/close fraction)
# Note: mediana ATR/close ≈ 0.17%, p75 ≈ 0.24%, p90 ≈ 0.34%

# Finestre orarie per entry (ET → UTC +5h; es. 9:30 ET = 14:30 UTC)
# Formato: (label, start_utc_min, end_utc_min)
# end è escluso: si entra se start <= bar_utc_min < end
_H = lambda h, m=0: h * 60 + m
TIME_WINDOWS = [
    ("all",         _H(14,30), _H(21, 0)),   # 9:30–16:00 ET (baseline)
    ("9:30-10:30",  _H(14,30), _H(15,30)),   # prima ora
    ("9:30-11:30",  _H(14,30), _H(16,30)),   # prime 2 ore
    ("9:30-12:30",  _H(14,30), _H(17,30)),   # prime 3 ore
    ("10:30-16:00", _H(15,30), _H(21, 0)),   # salta prima ora
    ("11:30-16:00", _H(16,30), _H(21, 0)),   # salta prime 2 ore
    ("14:00-16:00", _H(19, 0), _H(21, 0)),   # ultime 2 ore (power hour)
]

HMA_PERIOD  = 14
HMA_EXITBAR = 6   # non usato nel pandas sim (serve per backtrader), tenuto per doc
INVERTED    = True

SYMBOLS = ["WBD", "PLTR", "CSX", "INTC", "KDP", "KHC", "CSCO", "CMCSA", "NFLX"]
DATA_DIR = Path("/home/htpc/backtrader/data/m/alpaca")

RTH_START = (9, 30)
RTH_END   = (16, 0)


# ---------------------------------------------------------------------------
# HMA calculation — WMA vettorizzato con numpy (no rolling.apply, 10x più veloce)
# ---------------------------------------------------------------------------
def _wma(values: np.ndarray, period: int) -> np.ndarray:
    """WMA vettorizzato via convoluzione numpy."""
    w = np.arange(1, period + 1, dtype=float)
    w /= w.sum()
    # convolve con padding: mode='full' poi taglio
    out = np.convolve(values, w[::-1], mode='full')[:len(values)]
    out[:period - 1] = np.nan
    return out


def calculate_hma(close: pd.Series, period: int) -> pd.Series:
    half = max(1, period // 2)
    sq   = max(1, int(np.sqrt(period)))
    v    = close.values.astype(float)
    raw  = 2 * _wma(v, half) - _wma(v, period)
    hma  = _wma(raw, sq)
    return pd.Series(hma, index=close.index)


def calculate_atr_pct(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """ATR/close intra-day (senza gap overnight).
    Prima barra di ogni giorno: TR = H-L (non include Cprev di ieri).
    Rolling mean semplice su period barre RTH: NaN per le prime (period-1) barre del giorno,
    quindi l'ingresso è ritardato di ~period minuti dall'apertura."""
    h = df['high'].values.astype(float)
    l = df['low'].values.astype(float)
    c = df['close'].values.astype(float)
    c_prev = np.roll(c, 1); c_prev[0] = c[0]

    # True Range standard
    tr = np.maximum(h - l, np.maximum(np.abs(h - c_prev), np.abs(l - c_prev)))

    # Prima barra di ogni giorno → TR = H-L (elimina gap overnight)
    day_num    = df.index.year * 400 + df.index.day_of_year
    day_start  = np.concatenate(([True], day_num.values[1:] != day_num.values[:-1]))
    tr[day_start] = h[day_start] - l[day_start]

    atr = pd.Series(tr).rolling(period).mean().values
    return pd.Series(atr / c, index=df.index)


# ---------------------------------------------------------------------------
# Single-day simulation
# ---------------------------------------------------------------------------
def simulate_day(day_df: pd.DataFrame, sl_pct: float, tp_pct: float,
                 atr_min_pct: float = 0.0,
                 entry_start_min: int = 0, entry_end_min: int = 9999,
                 worst_case: bool = True) -> list:
    """
    Simula la strategia HMA su un giorno.
    Usa numpy arrays per il loop interno (100x più veloce di df.iloc[i]).

    worst_case=True      → caso ambiguo (SL e TP nella stessa barra): SL vince
    atr_min_pct > 0      → salta entry se ATR/close < soglia
    entry_start/end_min  → finestra oraria entry (minuti UTC da mezzanotte)
                           le uscite (SL/TP/segnale) avvengono sempre
    """
    df  = day_df.dropna(subset=['hma'])
    n   = len(df)
    if n < HMA_PERIOD + 10:
        return []

    # Estrai numpy arrays una volta sola — elimina overhead .iloc
    opens    = df['open'].values
    highs    = df['high'].values
    lows     = df['low'].values
    closes   = df['close'].values
    hmas     = df['hma'].values
    atrs     = df['atr_pct'].values if atr_min_pct > 0 else None
    bar_mins = (df.index.hour * 60 + df.index.minute).to_numpy()

    trades    = []
    pos_side  = 0       # 1=long, -1=short, 0=flat
    entry_px  = 0.0
    entry_idx = 0
    sl_px     = 0.0
    tp_px     = 0.0

    for i in range(1, n):
        hc = hmas[i]
        hp = hmas[i - 1]
        if np.isnan(hc) or np.isnan(hp):
            continue

        signal_long  = hp > hc   # HMA scende → long contrarian
        signal_short = hp < hc

        # --- Verifica SL/TP su high/low della barra ---
        if pos_side != 0:
            hi = highs[i]
            lo = lows[i]

            if pos_side == 1:   # long
                hit_sl = (sl_px > 0) and (lo <= sl_px)
                hit_tp = (tp_px > 0) and (hi >= tp_px)
            else:               # short
                hit_sl = (sl_px > 0) and (hi >= sl_px)
                hit_tp = (tp_px > 0) and (lo <= tp_px)

            if hit_sl and hit_tp:
                hit_tp = not worst_case   # worst_case → SL vince

            if hit_tp:
                ep  = entry_px
                ex  = tp_px
                pnl = (ex / ep - 1) if pos_side == 1 else (ep / ex - 1)
                trades.append({'side': pos_side, 'outcome': 'tp',
                               'pnl_pct': pnl * 100, 'bars': i - entry_idx,
                               'ambiguous': False})
                pos_side = 0
                continue

            if hit_sl:
                ep   = entry_px
                ex   = sl_px
                pnl  = (ex / ep - 1) if pos_side == 1 else (ep / ex - 1)
                # Ambiguo: SL scattato ma nella stessa barra c'era TP raggiungibile
                amb  = (tp_px > 0) and (
                    (pos_side == 1 and hi >= tp_px) or
                    (pos_side == -1 and lo <= tp_px)
                )
                trades.append({'side': pos_side, 'outcome': 'sl',
                               'pnl_pct': pnl * 100, 'bars': i - entry_idx,
                               'ambiguous': amb})
                pos_side = 0
                continue

        # --- Segnale HMA: entra / inverti ---
        price = closes[i]   # entry al close della barra segnale (come backtrader)

        # Filtri entry: finestra oraria + ATR
        bm = bar_mins[i]
        can_enter = (entry_start_min <= bm < entry_end_min) and (
            (atr_min_pct == 0) or (
                atrs is not None and not np.isnan(atrs[i]) and atrs[i] >= atr_min_pct
            )
        )

        if signal_long and pos_side != 1:
            if pos_side == -1:   # chiudi short
                pnl = (entry_px / price - 1)
                trades.append({'side': -1, 'outcome': 'signal',
                               'pnl_pct': pnl * 100, 'bars': i - entry_idx,
                               'ambiguous': False})
                pos_side = 0
            if can_enter:
                entry_px  = price
                entry_idx = i
                sl_px     = price * (1 - sl_pct) if sl_pct > 0 else 0.0
                tp_px     = price * (1 + tp_pct) if tp_pct > 0 else 0.0
                pos_side  = 1

        elif signal_short and pos_side != -1:
            if pos_side == 1:    # chiudi long
                pnl = (price / entry_px - 1)
                trades.append({'side': 1, 'outcome': 'signal',
                               'pnl_pct': pnl * 100, 'bars': i - entry_idx,
                               'ambiguous': False})
                pos_side = 0
            if can_enter:
                entry_px  = price
                entry_idx = i
                sl_px     = price * (1 + sl_pct) if sl_pct > 0 else 0.0
                tp_px     = price * (1 - tp_pct) if tp_pct > 0 else 0.0
            pos_side  = -1

    # EOD: chiudi posizione aperta
    if pos_side != 0:
        price = closes[-1]
        pnl   = (price / entry_px - 1) if pos_side == 1 else (entry_px / price - 1)
        trades.append({'side': pos_side, 'outcome': 'eod',
                       'pnl_pct': pnl * 100, 'bars': n - 1 - entry_idx,
                       'ambiguous': False})

    return trades


# ---------------------------------------------------------------------------
# Load data — HMA precalcolato su tutto il dataset (molto più veloce)
# ---------------------------------------------------------------------------
def load_symbol(symbol: str) -> pd.DataFrame | None:
    path = DATA_DIR / f"{symbol}.csv"
    if not path.exists():
        print(f"[WARN] {symbol}: file non trovato ({path})", file=sys.stderr)
        return None
    # Leggi senza parse_dates (2x più veloce), poi converti
    df = pd.read_csv(path)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, format='ISO8601')
    df = df.set_index('timestamp').sort_index()

    # Filtra RTH in UTC (09:30-16:00 ET = 14:30-21:00 UTC)
    # Aritmetica intera sull'indice: molto più veloce di .time()
    mins = df.index.hour * 60 + df.index.minute
    df = df[(mins >= 14 * 60 + 30) & (mins < 21 * 60)]

    if len(df) < 1000:
        return None

    # Precalcola HMA e ATR su tutto il dataset vettorizzato
    df['hma']     = calculate_hma(df['close'], HMA_PERIOD)
    df['atr_pct'] = calculate_atr_pct(df, period=14)

    return df


# ---------------------------------------------------------------------------
# Main sweep
# ---------------------------------------------------------------------------
def run_sweep(args):
    random.seed(args.seed)
    np.random.seed(args.seed)

    worst_case = not args.optimistic

    # Carica tutti i simboli
    symbol_data = {}
    for sym in SYMBOLS:
        df = load_symbol(sym)
        if df is not None and len(df) >= 1000:
            symbol_data[sym] = df

    if not symbol_data:
        print("Nessun dato trovato.", file=sys.stderr)
        sys.exit(1)

    # Campiona gli stessi giorni e precomputa le slice (evita normalize() ripetuto)
    print(f"Monte Carlo SL×TP sweep — period={HMA_PERIOD}, inverted={INVERTED}")
    print(f"Simboli: {list(symbol_data.keys())}")
    print(f"Campioni/simbolo: {args.samples}, seed={args.seed}")
    print(f"Caso ambiguo: {'SL-first (worst-case)' if worst_case else 'TP-first (ottimistico)'}")
    atr_values   = [0.0]        if args.no_atr  else ATR_VALUES
    time_windows = [TIME_WINDOWS[0]] if args.no_time else TIME_WINDOWS
    n_combos = len(SL_VALUES) * len(TP_VALUES) * len(atr_values) * len(time_windows)
    print(f"Combo: {len(SL_VALUES)} SL × {len(TP_VALUES)} TP × "
          f"{len(atr_values)} ATR × {len(time_windows)} time = {n_combos}")
    print("Precomputing day slices...", end=" ", flush=True)

    day_slices = {}  # {sym: [(date, day_df), ...]}
    for sym, df in symbol_data.items():
        date_idx = df.index.normalize()
        days_available = date_idx.unique().tolist()
        n = min(args.samples, len(days_available))
        sampled_days = random.sample(days_available, n)
        day_slices[sym] = []
        for day in sampled_days:
            day_df = df[date_idx == day]
            if len(day_df) >= HMA_PERIOD + 10:
                day_slices[sym].append((day.date(), day_df))

    total_days = sum(len(v) for v in day_slices.values())
    print(f"{total_days} slices ready.\n")

    results = []

    for tw_label, tw_start, tw_end in time_windows:
        for atr in atr_values:
            for sl in SL_VALUES:
                for tp in TP_VALUES:
                    all_trades = []
                    for sym, slices in day_slices.items():
                        for date, day_df in slices:
                            trades = simulate_day(day_df, sl, tp,
                                                  atr_min_pct=atr,
                                                  entry_start_min=tw_start,
                                                  entry_end_min=tw_end,
                                                  worst_case=worst_case)
                            for t in trades:
                                t['symbol'] = sym
                                t['date']   = date
                            all_trades.extend(trades)

                    if not all_trades:
                        continue

                    df_t = pd.DataFrame(all_trades)
                    n_trades   = len(df_t)
                    win_rate   = (df_t['pnl_pct'] > 0).mean() * 100
                    expectancy = df_t['pnl_pct'].mean()
                    pf_num     = df_t[df_t['pnl_pct'] > 0]['pnl_pct'].sum()
                    pf_den     = df_t[df_t['pnl_pct'] < 0]['pnl_pct'].abs().sum()
                    profit_f   = pf_num / pf_den if pf_den > 0 else float('inf')
                    avg_win    = df_t[df_t['pnl_pct'] > 0]['pnl_pct'].mean() if (df_t['pnl_pct'] > 0).any() else 0
                    avg_loss   = df_t[df_t['pnl_pct'] < 0]['pnl_pct'].mean() if (df_t['pnl_pct'] < 0).any() else 0
                    n_ambig    = df_t['ambiguous'].sum()
                    sl_hits    = (df_t['outcome'] == 'sl').sum()
                    tp_hits    = (df_t['outcome'] == 'tp').sum()

                    results.append({
                        'time': tw_label, 'sl_pct': sl, 'tp_pct': tp, 'atr_min_pct': atr,
                        'trades': n_trades, 'win%': win_rate,
                        'expect%': expectancy, 'PF': profit_f,
                        'avgW%': avg_win, 'avgL%': avg_loss,
                        'SL_hits': sl_hits, 'TP_hits': tp_hits,
                        'ambiguous': n_ambig,
                    })

    # Tabella risultati
    df_res = pd.DataFrame(results).sort_values('expect%', ascending=False)

    print(f"{'time':>12} {'SL%':>6} {'TP%':>6} {'trades':>7} {'win%':>6} {'E[pnl%]':>8} "
          f"{'PF':>6} {'avgW%':>7} {'avgL%':>7} {'SL_n':>5} {'TP_n':>5}")
    print("-" * 90)
    for _, r in df_res.iterrows():
        sl_s = f"{r['sl_pct']*100:.1f}%"  if r['sl_pct'] > 0 else "  off"
        tp_s = f"{r['tp_pct']*100:.1f}%"  if r['tp_pct'] > 0 else "  off"
        print(f"{r['time']:>12} {sl_s:>6} {tp_s:>6} {r['trades']:7.0f} {r['win%']:6.1f}% "
              f"{r['expect%']:8.4f}% {r['PF']:6.3f} {r['avgW%']:7.4f}% "
              f"{r['avgL%']:7.4f}% {r['SL_hits']:5.0f} {r['TP_hits']:5.0f}")

    # Salva CSV
    out = Path(__file__).parent / "sweep_sl_tp_results.csv"
    df_res.to_csv(out, index=False)
    print(f"\nRisultati salvati in: {out}")

    best = df_res.iloc[0]
    sl_s = f"{best['sl_pct']*100:.1f}%" if best['sl_pct'] > 0 else "off"
    tp_s = f"{best['tp_pct']*100:.1f}%" if best['tp_pct'] > 0 else "off"
    print(f"\nMIGLIOR COMBO: time={best['time']}, SL={sl_s}, TP={tp_s} → "
          f"E[pnl%]={best['expect%']:.4f}%, PF={best['PF']:.3f}, win%={best['win%']:.1f}%")

    # Riepilogo per finestra oraria (baseline sl=0/tp=0)
    print("\nE[pnl%] medio per finestra oraria (tutti SL×TP):")
    by_time = df_res.groupby('time')['expect%'].mean().sort_values(ascending=False)
    for tw, e in by_time.items():
        print(f"  {tw:>12}: {e:+.4f}%")


def parse_args():
    p = argparse.ArgumentParser(description="HMA SL/TP sweep Monte Carlo")
    p.add_argument("--samples", type=int, default=60,
                   help="Giorni campionati per simbolo (default: 60)")
    p.add_argument("--seed", type=int, default=42,
                   help="Seed random (default: 42)")
    p.add_argument("--optimistic", action="store_true",
                   help="Caso ambiguo: TP-first invece di SL-first (default: SL-first)")
    p.add_argument("--no-atr", action="store_true",
                   help="Disabilita sweep ATR (ATR=0 fisso)")
    p.add_argument("--no-time", action="store_true",
                   help="Disabilita sweep time-of-day (usa solo finestra 'all')")
    return p.parse_args()


if __name__ == "__main__":
    run_sweep(parse_args())
