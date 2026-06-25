"""
vol_ah_walkforward.py — Validazione walk-forward del modello 2D condizionato
=============================================================================
Per ogni mese M nel periodo di test:
  1. Calibra il modello 2D EWMA su tutti i dati fino a fine M-1 (train)
  2. Simula il mese M con quel modello (test out-of-sample)
  3. Confronta due strategie di sizing:
       - equal:  capitale diviso ugualmente tra candidati
       - model2d: capitale proporzionale al rendimento atteso dal modello

Metrica principale: rendimento giornaliero per sessione attiva.
Se model2d ≈ equal → il modello non aggiunge valore.
Se model2d < equal  → il lag produce sizing subottimale (peggiora).
Se model2d > equal  → il modello aggiunge alfa reale.

Uso:
  python vol_ah_walkforward.py [--years 3] [--top 5]
                               [--min-vol 0.025] [--max-vol 0.045]
                               [--half-life 30] [--no-plot]
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT         = Path(__file__).resolve().parent.parent.parent
TICKERS_FILE = ROOT / "config-common/tickers/stable_ah_top10.json"
BINS_2D      = [0.025, 0.030, 0.035, 0.040, 0.045]
RISK_FREE    = 0.05 / 252


# ────────────────────────────────────────────────────────────────
# Data
# ────────────────────────────────────────────────────────────────

def load_tickers():
    with open(TICKERS_FILE) as f:
        return [t for t in json.load(f) if t != "SPY"]


def download_daily(tickers, years):
    import datetime, yfinance as yf
    end   = datetime.date.today()
    start = end - datetime.timedelta(days=years * 365)
    return yf.download(tickers, start=start, end=end,
                       auto_adjust=True, progress=False)


def build_features(raw, tickers):
    frames = []
    for ticker in tickers:
        try:
            o = raw["Open"][ticker]
            h = raw["High"][ticker]
            l = raw["Low"][ticker]
            c = raw["Close"][ticker]
        except KeyError:
            continue
        df = pd.DataFrame({"open": o, "high": h, "low": l, "close": c}).dropna()
        df["rth_vol"] = (df["high"] - df["low"]) / df["open"]
        df["rth_ret"] = (df["close"] - df["open"]) / df["open"]
        df["ah_ret"]  = (df["open"].shift(-1) - df["close"]) / df["close"]
        df = df.dropna()
        df["ticker"] = ticker
        df.index.name = "date"
        frames.append(df[["ticker", "rth_vol", "rth_ret", "ah_ret"]])
    return pd.concat(frames).reset_index()


# ────────────────────────────────────────────────────────────────
# Modello 2D EWMA
# ────────────────────────────────────────────────────────────────

def fit_model_2d(train_df: pd.DataFrame, bins: list, half_life: int) -> dict:
    """
    Ritorna dict {ticker: {(left, right, direction): ewma_mean}}.
    Celle con n < 5 vengono omesse.
    """
    df = train_df.copy()
    df["vol_bin"]   = pd.cut(df["rth_vol"], bins=bins)
    df["direction"] = np.where(df["rth_ret"] < 0, "down", "up")

    max_date = pd.Timestamp(df["date"].max())
    lam      = np.log(2) / half_life
    df["w"]  = np.exp(-lam * (max_date - pd.to_datetime(df["date"])).dt.days)

    model = {}
    for (ticker, vol_bin, direction), g in df.groupby(
        ["ticker", "vol_bin", "direction"], observed=True
    ):
        if pd.isna(vol_bin) or len(g) < 5:
            continue
        w     = g["w"]
        mu    = (g["ah_ret"] * w).sum() / w.sum()
        model.setdefault(ticker, {})[(vol_bin.left, vol_bin.right, direction)] = {
            "mu": mu, "n": len(g)
        }

    return model


def lookup(model: dict, ticker: str, rth_vol: float,
           rth_ret: float) -> tuple[float, int] | tuple[None, int]:
    """Restituisce (ewma_mean, n) oppure (None, 0) se cella mancante."""
    direction = "down" if rth_ret < 0 else "up"
    tbl = model.get(ticker, {})
    for (left, right, d), val in tbl.items():
        if d == direction and left < rth_vol <= right:
            return val["mu"], val["n"]
    return None, 0


# ────────────────────────────────────────────────────────────────
# Simulazione di un mese
# ────────────────────────────────────────────────────────────────

def simulate_month(month_df: pd.DataFrame, model: dict,
                   top: int, min_vol: float, max_vol: float,
                   min_n: int = 10) -> pd.DataFrame:
    """
    Per ogni giorno del mese simula la sessione overnight con tre strategie:

      equal    — nessun modello, prende i primi `top` candidati in ordine
                 alfabetico (comportamento base della strategia)
      hybrid   — usa il modello per il ranking SOLO se la cella ha n >= min_n,
                 altrimenti mette il candidato in fondo; sizing sempre uguale
      model_w  — ranking modello + sizing proporzionale a exp_ret positivo
    """
    rows = []
    for date, day in month_df.groupby("date"):
        cands = day[
            (day["rth_vol"] >= min_vol) &
            (day["rth_vol"] <= max_vol)
        ].copy()
        if cands.empty:
            continue

        # Lookup modello per ogni candidato
        cands[["exp_ret", "n_bin"]] = cands.apply(
            lambda r: pd.Series(lookup(model, r["ticker"], r["rth_vol"], r["rth_ret"])),
            axis=1,
        )

        # ── equal: ordine alfabetico, nessun modello ─────────────
        top_equal = cands.sort_values("ticker").head(top)
        ret_equal = top_equal["ah_ret"].mean() if not top_equal.empty else np.nan

        # ── hybrid: ranking modello se stabile, altrimenti in fondo ─
        # Score: exp_ret se n_bin >= min_n, -inf altrimenti (va in fondo)
        cands["score"] = np.where(
            cands["n_bin"] >= min_n,
            cands["exp_ret"].fillna(-np.inf),
            -np.inf,
        )
        has_stable = (cands["n_bin"] >= min_n).any()
        if has_stable:
            top_hybrid = cands.sort_values("score", ascending=False).head(top)
        else:
            # Nessun parametro stabile → comportamento identico a equal
            top_hybrid = cands.sort_values("ticker").head(top)
        ret_hybrid = top_hybrid["ah_ret"].mean() if not top_hybrid.empty else np.nan

        # ── model_w: ranking + sizing proporzionale ───────────────
        top_mw    = cands.sort_values("exp_ret", ascending=False,
                                      na_position="last").head(top)
        pos_mask  = (top_mw["exp_ret"] > 0) & (top_mw["n_bin"] >= min_n)
        if pos_mask.any():
            w        = top_mw.loc[pos_mask, "exp_ret"]
            w        = w / w.sum()
            ret_mw   = (top_mw.loc[pos_mask, "ah_ret"] * w).sum()
            n_mw     = pos_mask.sum()
        else:
            ret_mw   = top_mw["ah_ret"].mean() if not top_mw.empty else np.nan
            n_mw     = len(top_mw)

        rows.append({
            "date":       date,
            "n_equal":    len(top_equal),
            "ret_equal":  ret_equal,
            "n_hybrid":   len(top_hybrid),
            "stable":     int(has_stable),
            "ret_hybrid": ret_hybrid,
            "n_mw":       n_mw,
            "ret_mw":     ret_mw,
        })

    return pd.DataFrame(rows)


# ────────────────────────────────────────────────────────────────
# Metriche
# ────────────────────────────────────────────────────────────────

def sharpe(rets: pd.Series) -> float:
    if rets.std() == 0 or len(rets) < 2:
        return np.nan
    return (rets.mean() - RISK_FREE) / rets.std() * np.sqrt(252)


def summary_stats(rets: pd.Series, label: str) -> dict:
    return {
        "strategy":   label,
        "n_days":     len(rets),
        "mean%":      rets.mean() * 100,
        "std%":       rets.std()  * 100,
        "sharpe":     sharpe(rets),
        "total%":     (1 + rets).prod() * 100 - 100,
        "win_rate%":  (rets > 0).mean() * 100,
    }


# ────────────────────────────────────────────────────────────────
# Walk-forward
# ────────────────────────────────────────────────────────────────

def walkforward(df: pd.DataFrame, bins: list, half_life: int,
                top: int, min_vol: float, max_vol: float,
                min_n: int = 10) -> pd.DataFrame:
    df = df.copy()
    df["date"]      = pd.to_datetime(df["date"])
    df["year_month"] = df["date"].dt.to_period("M")

    periods  = sorted(df["year_month"].unique())
    all_rows = []

    # Serve almeno 1 mese di train → partiamo dal secondo periodo
    for i, test_period in enumerate(periods[1:], start=1):
        train_mask = df["year_month"] < test_period
        test_mask  = df["year_month"] == test_period

        train_df = df[train_mask]
        test_df  = df[test_mask]

        if len(train_df) < 50 or test_df.empty:
            continue

        model     = fit_model_2d(train_df, bins, half_life)
        month_sim = simulate_month(test_df, model, top, min_vol, max_vol, min_n)

        if month_sim.empty:
            continue

        month_sim["period"] = str(test_period)
        all_rows.append(month_sim)

    return pd.concat(all_rows).reset_index(drop=True) if all_rows else pd.DataFrame()


# ────────────────────────────────────────────────────────────────
# Plot
# ────────────────────────────────────────────────────────────────

def plot_results(results: pd.DataFrame, out_dir: Path) -> None:
    import matplotlib.pyplot as plt

    r = results.copy()
    r["date"] = pd.to_datetime(r["date"])
    r = r.sort_values("date")

    cum_eq  = (1 + r["ret_equal"]).cumprod()
    cum_hy  = (1 + r["ret_hybrid"]).cumprod()
    cum_mw  = (1 + r["ret_mw"]).cumprod()

    fig, axes = plt.subplots(3, 1, figsize=(14, 12))

    ax = axes[0]
    ax.plot(r["date"], cum_eq, label="Equal (no model)",    color="steelblue")
    ax.plot(r["date"], cum_hy, label="Hybrid (rank+equal)", color="seagreen",  linewidth=1.5)
    ax.plot(r["date"], cum_mw, label="Model weighted",      color="tomato",    linewidth=1,  linestyle="--")
    ax.set_title("Crescita cumulativa (walk-forward out-of-sample)", fontsize=12)
    ax.set_ylabel("Moltiplicatore capitale")
    ax.legend(); ax.grid(alpha=0.3)

    ax = axes[1]
    monthly = r.groupby("period").agg(
        eq=("ret_equal",  lambda x: (1+x).prod()-1),
        hy=("ret_hybrid", lambda x: (1+x).prod()-1),
        mw=("ret_mw",     lambda x: (1+x).prod()-1),
    ).reset_index()
    x = np.arange(len(monthly)); w = 0.28
    ax.bar(x - w, monthly["eq"] * 100, width=w, label="Equal",   color="steelblue", alpha=0.75)
    ax.bar(x,     monthly["hy"] * 100, width=w, label="Hybrid",  color="seagreen",  alpha=0.75)
    ax.bar(x + w, monthly["mw"] * 100, width=w, label="Mod. W",  color="tomato",    alpha=0.75)
    ax.set_xticks(x)
    ax.set_xticklabels(monthly["period"], rotation=45, ha="right", fontsize=7)
    ax.axhline(0, color="k", linewidth=0.5)
    ax.set_title("Rendimento mensile %", fontsize=12); ax.legend(); ax.grid(alpha=0.3, axis="y")

    ax = axes[2]
    diff = r["ret_hybrid"] - r["ret_equal"]
    colors = ["seagreen" if d >= 0 else "tomato" for d in diff]
    ax.bar(r["date"], diff * 100, color=colors, alpha=0.7, width=1)
    ax.axhline(0, color="k", linewidth=0.5)
    ax.set_title("Differenza giornaliera: Hybrid − Equal (%)", fontsize=12)
    ax.set_ylabel("Δ Return %"); ax.grid(alpha=0.3, axis="y")

    plt.tight_layout()
    path = out_dir / "vol_ah_walkforward.png"
    fig.savefig(path, dpi=130)
    plt.close(fig)
    print(f"  → {path}")


# ────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Walk-forward validazione modello 2D")
    parser.add_argument("--years",     type=int,   default=3)
    parser.add_argument("--top",       type=int,   default=5)
    parser.add_argument("--min-vol",   type=float, default=0.025)
    parser.add_argument("--max-vol",   type=float, default=0.045)
    parser.add_argument("--half-life", type=int,   default=30)
    parser.add_argument("--min-n",     type=int,   default=10,
                        help="n minimo per usare coefficiente (default: 10)")
    parser.add_argument("--no-plot",   action="store_true")
    parser.add_argument("--out-dir",   type=Path,
                        default=Path(__file__).parent / "out")
    args = parser.parse_args()

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    tickers = load_tickers()
    print(f"Ticker: {tickers}")
    print(f"Download {args.years} anni dati daily...")
    raw = download_daily(tickers, args.years)
    df  = build_features(raw, tickers)
    print(f"Osservazioni: {len(df)}")

    print(f"\nWalk-forward mensile  "
          f"[vol {args.min_vol*100:.1f}%-{args.max_vol*100:.1f}%"
          f"  top={args.top}  half_life={args.half_life}gg]")

    results = walkforward(df, BINS_2D, args.half_life,
                          args.top, args.min_vol, args.max_vol,
                          min_n=args.min_n)

    if results.empty:
        print("Nessun risultato — verifica i parametri.")
        return

    # ── Statistiche globali ───────────────────────────────────────
    print("\n" + "═" * 60)
    print(f"STATISTICHE GLOBALI (out-of-sample)  min_n={args.min_n}")
    print("═" * 60)
    stats_df = pd.DataFrame([
        summary_stats(results["ret_equal"],  "equal   (no model)"),
        summary_stats(results["ret_hybrid"], "hybrid  (rank+equal)"),
        summary_stats(results["ret_mw"],     "model_w (rank+sized)"),
    ])
    print(stats_df.to_string(index=False,
          float_format=lambda x: f"{x:.4f}" if isinstance(x, float) else str(x)))

    # ── Breakdown mensile ─────────────────────────────────────────
    print("\n" + "═" * 60)
    print("BREAKDOWN MENSILE  (eq_tot / hy_tot / mw_tot %)")
    print("═" * 60)
    monthly = results.groupby("period").agg(
        days=("date",       "count"),
        eq_tot=("ret_equal",  lambda x: ((1+x).prod()-1) * 100),
        hy_tot=("ret_hybrid", lambda x: ((1+x).prod()-1) * 100),
        mw_tot=("ret_mw",     lambda x: ((1+x).prod()-1) * 100),
        stable_days=("stable", "sum"),
    ).reset_index()
    monthly["Δhy-eq"] = monthly["hy_tot"] - monthly["eq_tot"]
    monthly["Δmw-eq"] = monthly["mw_tot"] - monthly["eq_tot"]

    print(monthly.to_string(index=False,
          float_format=lambda x: f"{x:+.3f}" if isinstance(x, float) else str(x)))

    beats_hy = (monthly["Δhy-eq"] > 0).sum()
    beats_mw = (monthly["Δmw-eq"] > 0).sum()
    total    = len(monthly)
    print(f"\n  Mesi hybrid > equal:   {beats_hy}/{total} ({beats_hy/total*100:.0f}%)")
    print(f"  Mesi model_w > equal:  {beats_mw}/{total} ({beats_mw/total*100:.0f}%)")
    stable_pct = results["stable"].mean() * 100
    print(f"  Giorni con almeno 1 cella stabile (n≥{args.min_n}): "
          f"{stable_pct:.0f}% delle sessioni")

    # ── Test statistico ───────────────────────────────────────────
    for label, col in [("hybrid", "ret_hybrid"), ("model_w", "ret_mw")]:
        diff = results[col] - results["ret_equal"]
        t, p = stats.ttest_1samp(diff.dropna(), 0)
        sig  = ("MIGLIORE" if t > 0 else "PEGGIORE") if p < 0.05 else "non sign."
        print(f"  t-test {label} vs equal:  t={t:+.3f}  p={p:.4f}  → {sig}")

    # ── Output ───────────────────────────────────────────────────
    csv_path = out_dir / "vol_ah_walkforward.csv"
    results.to_csv(csv_path, index=False)
    print(f"\nRisultati → {csv_path}")

    if not args.no_plot:
        print("Generazione grafici...")
        plot_results(results, out_dir)


if __name__ == "__main__":
    main()
