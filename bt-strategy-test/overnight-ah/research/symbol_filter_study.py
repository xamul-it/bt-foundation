"""
symbol_filter_study.py — Analisi tecnica per-simbolo come predittore AH
=======================================================================
Ipotesi: il comportamento recente di un titolo (momentum, drawdown, notti
AH consecutive negative) predice se la notte successiva sarà negativa.

Feature calcolate al giorno t (per predire AH ret notte t→t+1):
  cc_ret_5d     : (close[t] - close[t-5])  / close[t-5]
  cc_ret_20d    : (close[t] - close[t-20]) / close[t-20]
  dd_20d        : drawdown da picco 20gg    (≤ 0)
  ah_lag1       : rendimento AH notte precedente  (close[t-1]→open[t])
  ah_roll3      : media ultimi 3 rendimenti AH
  ah_roll5      : media ultimi 5 rendimenti AH
  consec_neg_ah : notti AH negative consecutive
  rth_ret_1d    : rendimento RTH del giorno t (close-open)/open

Analisi:
  1. Correlazione (Pearson + Spearman) feature → ah_ret
  2. Binned: E[ah_ret | feature_bin], Sharpe per bin
  3. Walk-forward: salta il titolo se feature < soglia
     → costo/beneficio vs strategia base (tutti i titoli)

Uso:
  python symbol_filter_study.py [--years 3] [--no-plot] [--out-dir out]
"""

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

ROOT         = Path(__file__).resolve().parent.parent.parent
TICKERS_FILE = ROOT / "config-common/tickers/stable_ah_top10.json"
_OUT_DIR     = Path(__file__).parent / "out"

RISK_FREE_DAILY = 0.05 / 252

FEATURES = [
    "cc_ret_5d",
    "cc_ret_20d",
    "dd_20d",
    "ah_lag1",
    "ah_roll3",
    "ah_roll5",
    "consec_neg_ah",
    "rth_ret_1d",
]


# ─────────────────────────────────────────────────────────────────
# Dati
# ─────────────────────────────────────────────────────────────────

def load_tickers() -> list[str]:
    with open(TICKERS_FILE) as f:
        return [t for t in json.load(f) if t != "SPY"]


def download_daily(tickers: list[str], years: int) -> pd.DataFrame:
    import datetime
    import yfinance as yf
    end   = datetime.date.today()
    start = end - datetime.timedelta(days=years * 365)
    raw   = yf.download(tickers, start=start, end=end,
                        auto_adjust=True, progress=False)
    return raw


def build_features(raw: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    frames = []
    for ticker in tickers:
        try:
            o = raw["Open"][ticker]
            c = raw["Close"][ticker]
        except KeyError:
            print(f"  [warn] {ticker}: dati mancanti, saltato")
            continue

        df = pd.DataFrame({"open": o, "close": c}).dropna()

        # Target
        df["ah_ret"] = (df["open"].shift(-1) - df["close"]) / df["close"]

        # Feature momentum close-to-close
        df["cc_ret_5d"]  = df["close"].pct_change(5)
        df["cc_ret_20d"] = df["close"].pct_change(20)

        # Drawdown da picco 20gg (sempre ≤ 0)
        peak = df["close"].rolling(20, min_periods=1).max()
        df["dd_20d"] = (df["close"] - peak) / peak

        # AH return notte precedente e medie rolling
        df["ah_lag1"]  = df["ah_ret"].shift(1)
        df["ah_roll3"] = df["ah_ret"].shift(1).rolling(3).mean()
        df["ah_roll5"] = df["ah_ret"].shift(1).rolling(5).mean()

        # Notti AH negative consecutive (al giorno t = quante notti negative di fila fino a t-1)
        neg = (df["ah_ret"].shift(1) < 0).astype(int)
        consec = neg.copy()
        for i in range(1, len(df)):
            if neg.iloc[i] == 1:
                consec.iloc[i] = consec.iloc[i - 1] + 1
            else:
                consec.iloc[i] = 0
        df["consec_neg_ah"] = consec

        # RTH direction del giorno corrente
        df["rth_ret_1d"] = (df["close"] - df["open"]) / df["open"]

        df = df.dropna()
        df["ticker"] = ticker
        df.index.name = "date"
        frames.append(df[["ticker", "ah_ret"] + FEATURES])

    return pd.concat(frames).reset_index()


# ─────────────────────────────────────────────────────────────────
# Analisi 1 — Correlazione
# ─────────────────────────────────────────────────────────────────

def correlation_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for feat in FEATURES:
        sub = df[["ticker", feat, "ah_ret"]].dropna()
        # Pool
        pr, pp = stats.pearsonr(sub[feat], sub["ah_ret"])
        sr, sp = stats.spearmanr(sub[feat], sub["ah_ret"])
        rows.append({"feature": feat, "ticker": "ALL",
                     "pearson": pr, "pearson_p": pp,
                     "spearman": sr, "spearman_p": sp, "n": len(sub)})
        # Per ticker
        for tk, g in sub.groupby("ticker"):
            if len(g) < 30:
                continue
            pr2, pp2 = stats.pearsonr(g[feat], g["ah_ret"])
            sr2, sp2 = stats.spearmanr(g[feat], g["ah_ret"])
            rows.append({"feature": feat, "ticker": tk,
                         "pearson": pr2, "pearson_p": pp2,
                         "spearman": sr2, "spearman_p": sp2, "n": len(g)})
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────
# Analisi 2 — Binned E[ah_ret | feature]
# ─────────────────────────────────────────────────────────────────

def binned_analysis(df: pd.DataFrame, feat: str, n_bins: int = 6) -> pd.DataFrame:
    sub = df[["ticker", feat, "ah_ret"]].dropna().copy()
    try:
        sub["bin"] = pd.qcut(sub[feat], q=n_bins, duplicates="drop")
    except ValueError:
        return pd.DataFrame()
    grp = sub.groupby("bin", observed=True)["ah_ret"]
    result = grp.agg(
        mean="mean",
        std="std",
        n="count",
    ).reset_index()
    result["sharpe"] = (result["mean"] - RISK_FREE_DAILY) / result["std"].replace(0, np.nan)
    result["feature"] = feat
    return result


# ─────────────────────────────────────────────────────────────────
# Analisi 3 — Walk-forward con filtro per-simbolo
# ─────────────────────────────────────────────────────────────────

def walkforward(df: pd.DataFrame, feat: str, threshold: float,
                direction: str = "lt",
                max_concurrent: int = 10) -> pd.DataFrame:
    """
    Simula la strategia base vs strategia filtrata.
    direction='lt': skip se feat < threshold (es. ah_roll3 < -0.005)
    direction='gt': skip se feat > threshold

    Ritorna DataFrame giornaliero con ret_base e ret_filtered.
    """
    tickers = df["ticker"].unique().tolist()
    dates   = sorted(df["date"].unique())

    rows = []
    for dt in dates:
        day = df[df["date"] == dt]

        # Tutti i ticker con dati validi per questa data
        valid = day.dropna(subset=["ah_ret", feat])
        if valid.empty:
            continue

        n = min(max_concurrent, len(valid))

        # Base: tutti i ticker validi (primi n per stabilità)
        base_tickers = valid.head(n)
        ret_base = base_tickers["ah_ret"].mean() if len(base_tickers) > 0 else np.nan

        # Filtrato: escludi ticker dove la feature supera la soglia
        if direction == "lt":
            filt = valid[valid[feat] >= threshold]
        else:
            filt = valid[valid[feat] <= threshold]

        filt = filt.head(n)
        ret_filt = filt["ah_ret"].mean() if len(filt) > 0 else ret_base  # se tutti filtrati, stesso del base

        rows.append({
            "date":         dt,
            "n_base":       len(base_tickers),
            "ret_base":     ret_base,
            "n_filtered":   len(filt),
            "ret_filtered": ret_filt,
        })

    return pd.DataFrame(rows)


def cost_benefit(wf: pd.DataFrame) -> dict:
    def sharpe(r):
        r = r.dropna()
        if r.std() == 0 or len(r) < 5:
            return np.nan
        return r.mean() / r.std() * np.sqrt(252)

    def max_dd(r):
        cum = (1 + r.fillna(0)).cumprod()
        peak = cum.cummax()
        dd = (cum - peak) / (1 + peak)
        return dd.min() * 100

    cum_base = ((1 + wf["ret_base"].fillna(0)).cumprod() - 1).iloc[-1] * 100
    cum_filt = ((1 + wf["ret_filtered"].fillna(0)).cumprod() - 1).iloc[-1] * 100

    return {
        "sharpe_base":    round(sharpe(wf["ret_base"]), 3),
        "sharpe_filt":    round(sharpe(wf["ret_filtered"]), 3),
        "delta_sharpe":   round(sharpe(wf["ret_filtered"]) - sharpe(wf["ret_base"]), 3),
        "ret_base_pct":   round(cum_base, 2),
        "ret_filt_pct":   round(cum_filt, 2),
        "max_dd_base":    round(max_dd(wf["ret_base"]), 2),
        "max_dd_filt":    round(max_dd(wf["ret_filtered"]), 2),
        "n_days_total":   len(wf),
        "n_days_filtered_count": int((wf["n_filtered"] < wf["n_base"]).sum()),
    }


def sweep_thresholds(df: pd.DataFrame, feat: str,
                     thresholds: list[float], direction: str,
                     max_concurrent: int = 10) -> pd.DataFrame:
    rows = []
    for thr in thresholds:
        wf = walkforward(df, feat, thr, direction, max_concurrent)
        m  = cost_benefit(wf)
        rows.append({"feature": feat, "threshold": thr, "direction": direction, **m})
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────
# Plot
# ─────────────────────────────────────────────────────────────────

def plot_correlations(corr: pd.DataFrame, out_dir: Path):
    pool = corr[corr["ticker"] == "ALL"].set_index("feature")
    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(FEATURES))
    ax.bar(x - 0.2, pool.loc[FEATURES, "pearson"],  width=0.35, label="Pearson",  color="steelblue")
    ax.bar(x + 0.2, pool.loc[FEATURES, "spearman"], width=0.35, label="Spearman", color="darkorange")
    ax.axhline(0, color="black", lw=0.7)
    ax.set_xticks(x)
    ax.set_xticklabels(FEATURES, rotation=30, ha="right")
    ax.set_ylabel("Correlazione con ah_ret")
    ax.set_title("Correlazione feature → rendimento AH (pooled tutti i ticker)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    p = out_dir / "symbol_filter_correlations.png"
    plt.savefig(p, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"Plot: {p}")


def plot_binned(df: pd.DataFrame, feat: str, out_dir: Path):
    bd = binned_analysis(df, feat, n_bins=6)
    if bd.empty:
        return
    fig, ax = plt.subplots(figsize=(10, 4))
    x = range(len(bd))
    bars = ax.bar(x, bd["mean"] * 100, color=["crimson" if v < 0 else "steelblue" for v in bd["mean"]])
    ax.set_xticks(list(x))
    ax.set_xticklabels([str(b) for b in bd["bin"]], rotation=30, ha="right", fontsize=7)
    ax.set_ylabel("Mean AH ret (%)")
    ax.set_title(f"E[AH ret | {feat}]  (tutti i ticker)")
    ax.axhline(0, color="black", lw=0.7)
    ax.grid(True, alpha=0.3)
    for bar, n, sh in zip(bars, bd["n"], bd["sharpe"]):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.001,
                f"n={n}\nSh={sh:.2f}" if not np.isnan(sh) else f"n={n}",
                ha="center", va="bottom", fontsize=6.5)
    plt.tight_layout()
    p = out_dir / f"symbol_filter_bin_{feat}.png"
    plt.savefig(p, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"Plot: {p}")


def plot_sweep(sweep_df: pd.DataFrame, feat: str, out_dir: Path):
    fig, axes = plt.subplots(1, 3, figsize=(14, 4))
    x = sweep_df["threshold"].astype(str)
    axes[0].bar(x, sweep_df["delta_sharpe"], color=["green" if v > 0 else "crimson" for v in sweep_df["delta_sharpe"]])
    axes[0].set_title("Δ Sharpe")
    axes[0].axhline(0, color="black", lw=0.7)
    axes[1].bar(x, sweep_df["ret_filt_pct"] - sweep_df["ret_base_pct"],
                color=["green" if v > 0 else "crimson" for v in (sweep_df["ret_filt_pct"] - sweep_df["ret_base_pct"])])
    axes[1].set_title("Δ Rendimento totale (pp)")
    axes[1].axhline(0, color="black", lw=0.7)
    axes[2].bar(x, sweep_df["max_dd_filt"] - sweep_df["max_dd_base"],
                color=["green" if v > 0 else "crimson" for v in (sweep_df["max_dd_filt"] - sweep_df["max_dd_base"])])
    axes[2].set_title("Δ Max DD (pp, positivo = miglioramento)")
    axes[2].axhline(0, color="black", lw=0.7)
    for ax in axes:
        ax.set_xlabel(f"Soglia {feat}")
        ax.grid(True, alpha=0.3)
        ax.set_xticklabels(x, rotation=30, ha="right", fontsize=8)
    plt.suptitle(f"Sweep filtro: skip se {feat} < soglia")
    plt.tight_layout()
    p = out_dir / f"symbol_filter_sweep_{feat}.png"
    plt.savefig(p, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"Plot: {p}")


# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years",    type=int,   default=3)
    ap.add_argument("--no-plot",  action="store_true")
    ap.add_argument("--out-dir",  default=str(_OUT_DIR))
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tickers = load_tickers()
    print(f"Ticker: {tickers}")
    print("Download dati...")
    raw = download_daily(tickers, years=args.years)

    print("Calcolo feature...")
    df = build_features(raw, tickers)
    print(f"  {len(df)} righe, {df['ticker'].nunique()} ticker, "
          f"{df['date'].min().date()} – {df['date'].max().date()}")

    # ── 1. Correlazioni ──────────────────────────────────────────
    print("\n=== Correlazione feature → ah_ret (pooled) ===")
    corr = correlation_table(df)
    pool_corr = corr[corr["ticker"] == "ALL"][["feature", "pearson", "pearson_p", "spearman", "spearman_p", "n"]]
    print(pool_corr.to_string(index=False))
    corr.to_csv(out_dir / "symbol_filter_correlations.csv", index=False)

    # ── 2. Binned per le feature più rilevanti ───────────────────
    print("\n=== Binned E[ah_ret | feature] ===")
    for feat in FEATURES:
        bd = binned_analysis(df, feat)
        if bd.empty:
            continue
        print(f"\n  {feat}:")
        print(bd[["bin", "mean", "std", "sharpe", "n"]].to_string(index=False))
        if not args.no_plot:
            plot_binned(df, feat, out_dir)

    # ── 3. Sweep soglie per le feature AH-lag ───────────────────
    # Feature candidate: ah_lag1, ah_roll3, ah_roll5, cc_ret_5d, dd_20d
    sweeps = {
        "ah_lag1":    ([-0.02, -0.015, -0.01, -0.005, 0.0],  "lt"),
        "ah_roll3":   ([-0.015, -0.01, -0.005, 0.0, 0.005],  "lt"),
        "ah_roll5":   ([-0.01, -0.005, 0.0, 0.005],           "lt"),
        "cc_ret_5d":  ([-0.10, -0.07, -0.05, -0.03, -0.01],  "lt"),
        "cc_ret_20d": ([-0.15, -0.10, -0.07, -0.05, -0.03],  "lt"),
        "dd_20d":     ([-0.10, -0.07, -0.05, -0.03, -0.02],  "lt"),
    }

    print("\n=== Sweep filtri per-simbolo ===")
    all_sweeps = []
    for feat, (thresholds, direction) in sweeps.items():
        sw = sweep_thresholds(df, feat, thresholds, direction)
        all_sweeps.append(sw)
        print(f"\n  {feat} (skip se {direction} soglia):")
        print(sw[["threshold", "sharpe_base", "sharpe_filt", "delta_sharpe",
                   "ret_base_pct", "ret_filt_pct", "max_dd_base", "max_dd_filt",
                   "n_days_filtered_count"]].to_string(index=False))
        if not args.no_plot:
            plot_sweep(sw, feat, out_dir)

    all_sweeps_df = pd.concat(all_sweeps)
    all_sweeps_df.to_csv(out_dir / "symbol_filter_sweep.csv", index=False)

    # ── Riepilogo top filtri per Δ Sharpe ────────────────────────
    print("\n=== Top 10 filtri per Δ Sharpe ===")
    top = all_sweeps_df.sort_values("delta_sharpe", ascending=False).head(10)
    print(top[["feature", "threshold", "delta_sharpe", "ret_filt_pct",
               "max_dd_filt", "n_days_filtered_count"]].to_string(index=False))

    if not args.no_plot:
        plot_correlations(corr, out_dir)

    print(f"\nOutput in: {out_dir}")


if __name__ == "__main__":
    main()
