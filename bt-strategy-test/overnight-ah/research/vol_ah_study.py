"""
vol_ah_study.py — Correlazione volatilità RTH → rendimento AH
==============================================================
Per ogni ticker in stable_ah_top10.json:
  - RTH vol = (High - Low) / Open  del giorno D
  - AH ret  = (Open[D+1] - Close[D]) / Close[D]

Analisi:
  1. Correlazione Pearson/Spearman per ticker
  2. Binned: mean AH ret per fascia di volatilità (+ Sharpe per bin)
  3. Regressione lineare + lookup binned per expected return
  4. Rolling correlation/slope per stabilità temporale dei coefficienti
  5. Ranking odierno con filtro max_vol: expected AH ret → peso allocazione

Uso:
  python vol_ah_study.py [--years 3] [--top 5] [--max-vol 0.04]
                         [--rolling] [--roll-window 60]
                         [--bins 0,1,2,3,4,5,7,10]
                         [--no-plot] [--out-dir out]
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent.parent
TICKERS_FILE = ROOT / "config-common/tickers/stable_ah_top10.json"

DEFAULT_BINS = [0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.12, 1.0]
BINS_2D      = [0.025, 0.030, 0.035, 0.040, 0.045]   # bin per modello condizionato
RISK_FREE_DAILY = 0.05 / 252  # ~5% annuo


# ────────────────────────────────────────────────────────────────
# Data
# ────────────────────────────────────────────────────────────────

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
    """DataFrame lungo: date, ticker, rth_vol, rth_ret, ah_ret."""
    frames = []
    for ticker in tickers:
        try:
            o = raw["Open"][ticker]
            h = raw["High"][ticker]
            l = raw["Low"][ticker]
            c = raw["Close"][ticker]
        except KeyError:
            print(f"  [warn] {ticker}: dati mancanti, saltato")
            continue

        df = pd.DataFrame({"open": o, "high": h, "low": l, "close": c}).dropna()
        df["rth_vol"] = (df["high"] - df["low"]) / df["open"]      # sempre ≥ 0
        df["rth_ret"] = (df["close"] - df["open"]) / df["open"]    # con segno
        df["ah_ret"]  = (df["open"].shift(-1) - df["close"]) / df["close"]
        df = df.dropna()
        df["ticker"] = ticker
        df.index.name = "date"
        frames.append(df[["ticker", "rth_vol", "rth_ret", "ah_ret"]])

    return pd.concat(frames).reset_index()


# ────────────────────────────────────────────────────────────────
# Analisi statica
# ────────────────────────────────────────────────────────────────

def correlation_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for ticker, g in df.groupby("ticker"):
        if len(g) < 30:
            continue
        r_p, p_p = stats.pearsonr(g["rth_vol"], g["ah_ret"])
        r_s, p_s = stats.spearmanr(g["rth_vol"], g["ah_ret"])
        rows.append({
            "ticker":      ticker,
            "n":           len(g),
            "mean_vol%":   g["rth_vol"].mean() * 100,
            "mean_ah%":    g["ah_ret"].mean() * 100,
            "pearson_r":   r_p,
            "pearson_p":   p_p,
            "spearman_r":  r_s,
            "spearman_p":  p_s,
        })
    return pd.DataFrame(rows).sort_values("pearson_r", ascending=False)


def direction_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analisi del rendimento RTH con segno (rth_ret) come predittore di AH ret.
    Per ogni ticker:
      - correlazione rth_ret → ah_ret  (mean-reversion = r negativo)
      - AH ret medio condizionato: giornate RTH positive vs negative
    """
    rows = []
    for ticker, g in df.groupby("ticker"):
        if len(g) < 30:
            continue
        r_p, p_p = stats.pearsonr(g["rth_ret"], g["ah_ret"])
        r_s, p_s = stats.spearmanr(g["rth_ret"], g["ah_ret"])

        up   = g[g["rth_ret"] > 0]["ah_ret"]
        down = g[g["rth_ret"] < 0]["ah_ret"]
        flat = g[g["rth_ret"] == 0]["ah_ret"]

        rows.append({
            "ticker":       ticker,
            "pearson_r":    r_p,
            "pearson_p":    p_p,
            "spearman_r":   r_s,
            "n_up":         len(up),
            "ah_if_up%":    up.mean()   * 100 if len(up)   > 0 else np.nan,
            "n_down":       len(down),
            "ah_if_down%":  down.mean() * 100 if len(down) > 0 else np.nan,
            "diff%":        (down.mean() - up.mean()) * 100
                            if len(up) > 0 and len(down) > 0 else np.nan,
        })
    out = pd.DataFrame(rows).sort_values("pearson_r")
    return out


def binned_analysis(df: pd.DataFrame, bins: list[float]) -> pd.DataFrame:
    df = df.copy()
    df["vol_bin"] = pd.cut(df["rth_vol"], bins=bins)
    agg = (
        df.groupby(["ticker", "vol_bin"], observed=False)["ah_ret"]
        .agg(["mean", "std", "count"])
        .rename(columns={"mean": "mean_ah", "std": "std_ah", "count": "n"})
        .reset_index()
    )
    # Sharpe overnight per bin (annualizzazione indicativa: *sqrt(252))
    agg["sharpe"] = (agg["mean_ah"] - RISK_FREE_DAILY) / agg["std_ah"].replace(0, np.nan)
    agg["mean_ah%"] = agg["mean_ah"] * 100
    agg["std_ah%"]  = agg["std_ah"]  * 100
    return agg[agg["n"] >= 5]


def fit_models(df: pd.DataFrame) -> dict:
    """Regressione lineare per ticker: vol → ah_ret."""
    models = {}
    for ticker, g in df.groupby("ticker"):
        if len(g) < 30:
            continue
        slope, intercept, r, p, _ = stats.linregress(g["rth_vol"], g["ah_ret"])
        models[ticker] = dict(slope=slope, intercept=intercept, r=r, p=p)
    return models


# ────────────────────────────────────────────────────────────────
# Expected return: binned lookup (non-parametrico) + fallback lineare
# ────────────────────────────────────────────────────────────────

def build_bin_lookup(binned: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Per ticker: DataFrame con vol_bin → mean_ah (per lookup diretto)."""
    lookup = {}
    for ticker, g in binned.groupby("ticker"):
        lookup[ticker] = g.set_index("vol_bin")[["mean_ah", "std_ah", "n", "sharpe"]].copy()
    return lookup


def expected_return_for_vol(ticker: str, vol: float,
                             bin_lookup: dict, models: dict,
                             bins: list[float]) -> dict:
    """
    Stima E[AH ret] per (ticker, vol) con due metodi:
      - binned: media del bin di appartenenza (non-parametrico)
      - linear: intercept + slope * vol
    Restituisce dict con entrambi + n del bin.
    """
    result = {"binned": np.nan, "linear": np.nan, "n_bin": 0, "sharpe_bin": np.nan}

    # Lookup binned
    if ticker in bin_lookup:
        tbl = bin_lookup[ticker]
        for interval in tbl.index:
            if interval.left <= vol < interval.right:
                row = tbl.loc[interval]
                result["binned"]    = float(row["mean_ah"])
                result["n_bin"]     = int(row["n"])
                result["sharpe_bin"] = float(row["sharpe"])
                break

    # Lineare
    if ticker in models:
        m = models[ticker]
        result["linear"] = m["intercept"] + m["slope"] * vol

    return result


# ────────────────────────────────────────────────────────────────
# Modello 2D condizionato: vol_bin × direction con EWMA
# ────────────────────────────────────────────────────────────────

def binned_2d_ewma(df: pd.DataFrame, bins: list[float],
                   half_life: int = 30) -> pd.DataFrame:
    """
    Per ogni (ticker, vol_bin, direction):
      - direction = 'down' se rth_ret < 0, 'up' altrimenti
      - media EWMA di ah_ret con decadimento esponenziale (half_life giorni)
    Osservazioni fuori dai bin vengono ignorate (NaN vol_bin).
    """
    df = df.copy()
    df["vol_bin"]   = pd.cut(df["rth_vol"], bins=bins)
    df["direction"] = np.where(df["rth_ret"] < 0, "down", "up")

    # Peso EWMA: exp(-λ * days_ago), λ = ln2 / half_life
    df = df.sort_values("date")
    max_date = pd.Timestamp(df["date"].max())
    df["days_ago"] = (max_date - pd.to_datetime(df["date"])).dt.days
    lam = np.log(2) / half_life
    df["w"] = np.exp(-lam * df["days_ago"])

    rows = []
    for (ticker, vol_bin, direction), g in df.groupby(
        ["ticker", "vol_bin", "direction"], observed=True
    ):
        if pd.isna(vol_bin) or len(g) < 5:
            continue
        w      = g["w"]
        w_sum  = w.sum()
        mu     = (g["ah_ret"] * w).sum() / w_sum
        var    = (w * (g["ah_ret"] - mu) ** 2).sum() / w_sum
        sigma  = np.sqrt(var) if var > 0 else np.nan
        rows.append({
            "ticker":    ticker,
            "vol_bin":   str(vol_bin),
            "direction": direction,
            "ewma_mean": mu,
            "ewma_std":  sigma,
            "n":         len(g),
            "sharpe":    (mu - RISK_FREE_DAILY) / sigma if sigma and sigma > 0 else np.nan,
        })

    return pd.DataFrame(rows).sort_values(["ticker", "vol_bin", "direction"])


def lookup_2d(model: dict, ticker: str, rth_vol: float, rth_ret: float) -> float | None:
    """
    Dato il modello 2D caricato da JSON, restituisce ewma_mean per
    (ticker, vol corrente, direzione RTH).  None se cella non trovata.
    """
    if ticker not in model:
        return None
    direction = "down" if rth_ret < 0 else "up"
    tbl = model[ticker]
    for key, val in tbl.items():
        k_dir = key.split("|")[1]
        if k_dir != direction:
            continue
        k_left, k_right = (float(x) for x in key.split("|")[0].split("-"))
        if k_left < rth_vol <= k_right:
            return val["ewma_mean"]
    return None


def save_model_2d(df2d: pd.DataFrame, path: Path) -> None:
    """
    Serializza il modello 2D in JSON leggibile dalla strategia.
    Formato chiave: "{left}-{right}|{direction}"
    """
    model: dict = {}
    for _, row in df2d.iterrows():
        ticker = row["ticker"]
        # vol_bin è una stringa tipo "(0.025, 0.03]"
        s = row["vol_bin"].strip("(]").replace(" ", "")
        left, right = s.split(",")
        key = f"{left}-{right}|{row['direction']}"
        model.setdefault(ticker, {})[key] = {
            "ewma_mean": round(row["ewma_mean"], 8),
            "ewma_std":  round(row["ewma_std"],  8) if not np.isnan(row["ewma_std"]) else None,
            "n":         int(row["n"]),
            "sharpe":    round(row["sharpe"], 4)     if not np.isnan(row["sharpe"])  else None,
        }
    with open(path, "w") as f:
        json.dump(model, f, indent=2)
    print(f"  → modello 2D salvato in {path}")


# ────────────────────────────────────────────────────────────────
# Analisi rolling (stabilità temporale)
# ────────────────────────────────────────────────────────────────

def rolling_stats(df: pd.DataFrame, window: int) -> pd.DataFrame:
    """
    Per ogni ticker calcola correlazione rolling e slope rolling.
    Restituisce DataFrame lungo: date, ticker, roll_r, roll_slope.
    """
    frames = []
    for ticker, g in df.groupby("ticker"):
        g = g.sort_values("date").set_index("date")
        roll_r     = []
        roll_slope = []
        dates      = []

        arr = g[["rth_vol", "ah_ret"]].values
        for i in range(window, len(arr) + 1):
            w = arr[i - window: i]
            x, y = w[:, 0], w[:, 1]
            if np.std(x) < 1e-9:
                roll_r.append(np.nan)
                roll_slope.append(np.nan)
            else:
                slope, intercept, r, p, _ = stats.linregress(x, y)
                roll_r.append(r)
                roll_slope.append(slope)
            dates.append(g.index[i - 1])

        tmp = pd.DataFrame({
            "date":       dates,
            "ticker":     ticker,
            "roll_r":     roll_r,
            "roll_slope": roll_slope,
        })
        frames.append(tmp)

    return pd.concat(frames).reset_index(drop=True)


def rolling_stability_summary(roll: pd.DataFrame) -> pd.DataFrame:
    """Variabilità della rolling correlation per ticker → suggerisce freq. ricalcolo."""
    rows = []
    for ticker, g in roll.groupby("ticker"):
        g = g.dropna(subset=["roll_r"])
        rows.append({
            "ticker":        ticker,
            "mean_r":        g["roll_r"].mean(),
            "std_r":         g["roll_r"].std(),
            "min_r":         g["roll_r"].min(),
            "max_r":         g["roll_r"].max(),
            "mean_slope":    g["roll_slope"].mean(),
            "std_slope":     g["roll_slope"].std(),
        })
    out = pd.DataFrame(rows).sort_values("std_r", ascending=False)
    return out


# ────────────────────────────────────────────────────────────────
# Allocazione
# ────────────────────────────────────────────────────────────────

def optimal_weights(expected: dict[str, float],
                    max_exposure: float = 0.95) -> dict[str, float]:
    """Pesi proporzionali al rendimento atteso positivo."""
    pos = {t: r for t, r in expected.items() if r > 0}
    if not pos:
        pos = {t: 1.0 for t in expected}
    total = sum(pos.values())
    return {t: r / total * max_exposure for t, r in pos.items()}


# ────────────────────────────────────────────────────────────────
# Plot
# ────────────────────────────────────────────────────────────────

def _subplot_grid(n: int, cols: int = 4):
    import matplotlib.pyplot as plt
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(cols * 4, rows * 3.5))
    return fig, np.array(axes).flatten()


def plot_scatter(df: pd.DataFrame, models: dict, out_dir: Path) -> None:
    import matplotlib.pyplot as plt

    tickers = sorted(df["ticker"].unique())
    fig, axes = _subplot_grid(len(tickers))

    for i, ticker in enumerate(tickers):
        ax = axes[i]
        g  = df[df["ticker"] == ticker]
        ax.scatter(g["rth_vol"] * 100, g["ah_ret"] * 100,
                   alpha=0.25, s=8, color="steelblue")
        if ticker in models:
            m = models[ticker]
            x = np.linspace(g["rth_vol"].min(), g["rth_vol"].max(), 100)
            y = m["intercept"] + m["slope"] * x
            ax.plot(x * 100, y * 100, "r-", linewidth=1.5,
                    label=f"r={m['r']:.2f}  p={m['p']:.3f}")
            ax.legend(fontsize=7)
        ax.axhline(0, color="k", linewidth=0.4, linestyle="--")
        ax.set_title(ticker, fontsize=10, fontweight="bold")
        ax.set_xlabel("RTH vol %", fontsize=8)
        ax.set_ylabel("AH ret %",  fontsize=8)
        ax.tick_params(labelsize=7)

    for j in range(i + 1, len(axes)):
        axes[j].set_visible(False)

    fig.suptitle("RTH Volatility vs AH Overnight Return", fontsize=13)
    plt.tight_layout()
    path = out_dir / "vol_ah_scatter.png"
    fig.savefig(path, dpi=130)
    plt.close(fig)
    print(f"  → {path}")


def plot_binned(binned: pd.DataFrame, out_dir: Path) -> None:
    import matplotlib.pyplot as plt

    tickers = sorted(binned["ticker"].unique())
    fig, axes = _subplot_grid(len(tickers))

    for i, ticker in enumerate(tickers):
        ax = axes[i]
        g  = binned[binned["ticker"] == ticker].copy()
        labels = [str(b).replace(", ", "–") for b in g["vol_bin"]]
        colors = ["tomato" if v < 0 else "steelblue" for v in g["mean_ah%"]]
        bars = ax.bar(range(len(g)), g["mean_ah%"],
                      yerr=g["std_ah%"], capsize=3, alpha=0.75, color=colors)
        ax.set_xticks(range(len(g)))
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=6)
        ax.axhline(0, color="k", linewidth=0.4, linestyle="--")
        ax.set_title(ticker, fontsize=10, fontweight="bold")
        ax.set_ylabel("mean AH ret %", fontsize=8)
        for bar, n in zip(bars, g["n"]):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.005,
                    f"n={n}", ha="center", va="bottom", fontsize=6)
        ax.tick_params(labelsize=7)

    for j in range(i + 1, len(axes)):
        axes[j].set_visible(False)

    fig.suptitle("Mean AH Return by RTH Volatility Bin", fontsize=13)
    plt.tight_layout()
    path = out_dir / "vol_ah_binned.png"
    fig.savefig(path, dpi=130)
    plt.close(fig)
    print(f"  → {path}")


def plot_heatmap(binned: pd.DataFrame, out_dir: Path,
                 metric: str = "mean_ah%", title_sfx: str = "AH ret %") -> None:
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors

    pivot = binned.pivot_table(
        index="ticker", columns="vol_bin", values=metric, aggfunc="mean",
        observed=False,
    )
    fig, ax = plt.subplots(figsize=(max(8, len(pivot.columns) * 1.3), len(pivot) * 0.9 + 1))
    vmin, vmax = pivot.values[~np.isnan(pivot.values)].min(), pivot.values[~np.isnan(pivot.values)].max()
    norm = mcolors.TwoSlopeNorm(vmin=vmin, vcenter=0, vmax=max(vmax, 0.001))
    im = ax.imshow(pivot.values, aspect="auto", cmap="RdYlGn", norm=norm)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(
        [str(c).replace(", ", "–") for c in pivot.columns],
        rotation=45, ha="right", fontsize=8,
    )
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=9)
    for r in range(pivot.shape[0]):
        for c in range(pivot.shape[1]):
            v = pivot.values[r, c]
            if not np.isnan(v):
                ax.text(c, r, f"{v:.3f}", ha="center", va="center", fontsize=7)
    plt.colorbar(im, ax=ax, label=title_sfx)
    ax.set_title(f"Heatmap: {title_sfx} (ticker × RTH vol bin)", fontsize=12)
    plt.tight_layout()
    slug = metric.replace("%", "pct").replace("_", "-")
    path = out_dir / f"vol_ah_heatmap_{slug}.png"
    fig.savefig(path, dpi=130)
    plt.close(fig)
    print(f"  → {path}")


def plot_rolling(roll: pd.DataFrame, out_dir: Path, window: int) -> None:
    import matplotlib.pyplot as plt

    tickers = sorted(roll["ticker"].unique())
    fig, axes = _subplot_grid(len(tickers))

    for i, ticker in enumerate(tickers):
        ax = axes[i]
        g  = roll[roll["ticker"] == ticker].sort_values("date")
        ax.plot(g["date"], g["roll_r"], color="steelblue", linewidth=1)
        ax.axhline(0,  color="k",   linewidth=0.5, linestyle="--")
        ax.axhline(g["roll_r"].mean(), color="tomato", linewidth=0.8,
                   linestyle=":", label=f"μ={g['roll_r'].mean():.2f}")
        ax.fill_between(g["date"], g["roll_r"], 0,
                        where=g["roll_r"] > 0, alpha=0.15, color="green")
        ax.fill_between(g["date"], g["roll_r"], 0,
                        where=g["roll_r"] < 0, alpha=0.15, color="red")
        ax.set_ylim(-0.6, 0.6)
        ax.set_title(ticker, fontsize=10, fontweight="bold")
        ax.set_ylabel(f"roll-{window}d Pearson r", fontsize=7)
        ax.legend(fontsize=7)
        ax.tick_params(labelsize=7)

    for j in range(i + 1, len(axes)):
        axes[j].set_visible(False)

    fig.suptitle(f"Rolling {window}-day Correlation: RTH vol → AH ret", fontsize=13)
    plt.tight_layout()
    path = out_dir / f"vol_ah_rolling_{window}d.png"
    fig.savefig(path, dpi=130)
    plt.close(fig)
    print(f"  → {path}")


# ────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Studio vol RTH → rendimento AH")
    parser.add_argument("--years",      type=int,   default=3)
    parser.add_argument("--top",        type=int,   default=5,
                        help="Top N asset da selezionare (default: 5)")
    parser.add_argument("--min-vol",    type=float, default=None,
                        help="Filtra candidati con vol < soglia (es. 0.01)")
    parser.add_argument("--max-vol",    type=float, default=None,
                        help="Filtra candidati con vol > soglia (es. 0.04)")
    parser.add_argument("--bins",       type=str,   default="",
                        help="Bins percentuali, es. 0,1,2,3,4,5,7,10")
    parser.add_argument("--half-life",  type=int,   default=30,
                        help="Half-life EWMA in giorni per modello 2D (default: 30)")
    parser.add_argument("--rolling",    action="store_true",
                        help="Calcola rolling correlation (stabilità temporale)")
    parser.add_argument("--roll-window", type=int,  default=60,
                        help="Finestra rolling in giorni (default: 60)")
    parser.add_argument("--no-plot",    action="store_true")
    parser.add_argument("--out-dir",    type=Path,
                        default=Path(__file__).parent / "out")
    args = parser.parse_args()

    bins = (
        [float(x) / 100 for x in args.bins.split(",")]
        if args.bins else DEFAULT_BINS
    )

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Dati ──────────────────────────────────────────────────────
    tickers = load_tickers()
    print(f"Ticker ({len(tickers)}): {tickers}")
    print(f"Download {args.years} anni di dati daily via yfinance...")
    raw = download_daily(tickers, args.years)
    df  = build_features(raw, tickers)
    print(f"Osservazioni totali: {len(df)}")

    # ── Correlazione statica ───────────────────────────────────────
    print("\n" + "═" * 65)
    print("CORRELAZIONE RTH vol → AH return (full period)")
    print("═" * 65)
    corr = correlation_table(df)
    print(corr.to_string(index=False,
          float_format=lambda x: f"{x:+.4f}" if isinstance(x, float) else str(x)))

    # ── Segno RTH → AH (mean-reversion) ──────────────────────────
    print("\n" + "═" * 65)
    print("SEGNO RTH → AH return  (r negativo = mean-reversion overnight)")
    print("═" * 65)
    dirn = direction_analysis(df)
    print(dirn.to_string(index=False,
          float_format=lambda x: f"{x:+.4f}" if isinstance(x, float) else str(x)))
    print("\n  Legenda:")
    print("  pearson_r < 0  → il titolo tende a rimbalzare AH dopo sessione RTH negativa")
    print("  ah_if_down%    → AH medio nelle sessioni in cui RTH ha chiuso in perdita")
    print("  ah_if_up%      → AH medio nelle sessioni in cui RTH ha chiuso in guadagno")
    print("  diff%          → ah_if_down% - ah_if_up%  (positivo = mean-reversion)")

    # ── Modello 2D condizionato (vol_bin × direction, EWMA) ──────
    print("\n" + "═" * 65)
    print(f"MODELLO 2D: E[AH ret | vol_bin, direction]  EWMA half_life={args.half_life}gg")
    print("═" * 65)
    df2d = binned_2d_ewma(df, BINS_2D, half_life=args.half_life)
    df2d["ewma_mean%"] = df2d["ewma_mean"] * 100
    df2d["ewma_std%"]  = df2d["ewma_std"]  * 100
    cols2d = ["ticker", "vol_bin", "direction", "n", "ewma_mean%", "ewma_std%", "sharpe"]
    print(df2d[cols2d].to_string(index=False,
          float_format=lambda x: f"{x:.4f}"))

    model_2d_path = out_dir / "vol_ah_model_2d.json"
    save_model_2d(df2d, model_2d_path)

    # ── Binned + Sharpe ───────────────────────────────────────────
    print("\n" + "═" * 65)
    print("BINNED 1D: mean AH ret% e Sharpe per fascia vol (n≥5)")
    print("═" * 65)
    binned = binned_analysis(df, bins)
    cols_show = ["ticker", "vol_bin", "n", "mean_ah%", "std_ah%", "sharpe"]
    print(binned[cols_show].to_string(index=False,
          float_format=lambda x: f"{x:.4f}"))

    # ── Modelli e lookup ──────────────────────────────────────────
    models     = fit_models(df)
    bin_lookup = build_bin_lookup(binned)

    model_rows = [{"ticker": t, **m} for t, m in models.items()]
    model_path = out_dir / "vol_ah_models.csv"
    pd.DataFrame(model_rows).to_csv(model_path, index=False)
    print(f"\nModelli 1D salvati → {model_path}")

    # ── Rolling (opzionale) ───────────────────────────────────────
    if args.rolling:
        print(f"\n" + "═" * 65)
        print(f"ROLLING CORRELATION (finestra={args.roll_window}gg)")
        print("═" * 65)
        roll = rolling_stats(df, args.roll_window)
        stab = rolling_stability_summary(roll)

        print("\nStabilità coefficienti (std_r alta = coefficienti instabili):")
        print(stab.to_string(index=False,
              float_format=lambda x: f"{x:+.4f}" if isinstance(x, float) else str(x)))

        print("\nInterpretazione frequenza ricalcolo:")
        for _, row in stab.iterrows():
            sr = row["std_r"]
            if sr < 0.08:
                freq = "trimestrale — correlazione stabile"
            elif sr < 0.15:
                freq = "mensile — correlazione moderatamente variabile"
            else:
                freq = "settimanale — correlazione instabile, usare con cautela"
            print(f"  {row['ticker']:<6}  std_r={sr:.3f}  → ricalcolo {freq}")

        roll_path = out_dir / f"vol_ah_rolling_{args.roll_window}d.csv"
        roll.to_csv(roll_path, index=False)
        print(f"\nRolling data → {roll_path}")

        if not args.no_plot:
            plot_rolling(roll, out_dir, args.roll_window)

    # ── Ranking odierno ───────────────────────────────────────────
    vol_range_tag = ""
    if args.min_vol or args.max_vol:
        lo = f"{args.min_vol*100:.1f}%" if args.min_vol else "0%"
        hi = f"{args.max_vol*100:.1f}%" if args.max_vol else "∞"
        vol_range_tag = f"  [filtro vol {lo} – {hi}]"
    print("\n" + "═" * 65)
    print(f"RANKING ODIERNO — top {args.top}{vol_range_tag}")
    print("═" * 65)

    last_vols: dict[str, float] = {}
    for ticker in tickers:
        try:
            o = raw["Open"][ticker].dropna().iloc[-1]
            h = raw["High"][ticker].dropna().iloc[-1]
            l = raw["Low"][ticker].dropna().iloc[-1]
            if o > 0:
                last_vols[ticker] = (h - l) / o
        except Exception:
            pass

    # Filtra per range vol
    candidates = dict(last_vols)
    if args.min_vol:
        lo_excl  = {t for t, v in candidates.items() if v < args.min_vol}
        candidates = {t: v for t, v in candidates.items() if v >= args.min_vol}
        if lo_excl:
            print(f"  Esclusi per vol < {args.min_vol*100:.1f}%: {', '.join(sorted(lo_excl))}")
    if args.max_vol:
        hi_excl  = {t for t, v in candidates.items() if v > args.max_vol}
        candidates = {t: v for t, v in candidates.items() if v <= args.max_vol}
        if hi_excl:
            print(f"  Esclusi per vol > {args.max_vol*100:.1f}%: {', '.join(sorted(hi_excl))}")

    # Expected return per candidato
    exp_data = {}
    for ticker, vol in candidates.items():
        er = expected_return_for_vol(ticker, vol, bin_lookup, models, bins)
        exp_data[ticker] = er

    ranking = sorted(exp_data.items(),
                     key=lambda x: x[1]["binned"] if not np.isnan(x[1]["binned"])
                                   else x[1]["linear"],
                     reverse=True)

    top_tickers = [t for t, _ in ranking[: args.top]]
    top_expected_binned = {
        t: exp_data[t]["binned"] if not np.isnan(exp_data[t]["binned"])
           else exp_data[t]["linear"]
        for t in top_tickers
    }
    weights = optimal_weights(top_expected_binned)

    # Header
    print(f"\n{'Ticker':<8} {'Vol%':>7} {'E[AH]% bin':>12} {'E[AH]% lin':>12}"
          f" {'Sharpe bin':>11} {'n bin':>6} {'Peso%':>7}  {'':>7}")
    print("─" * 75)

    for ticker, er in ranking:
        vol_s    = f"{last_vols.get(ticker, 0) * 100:.2f}"
        bin_s    = f"{er['binned']*100:+.4f}" if not np.isnan(er['binned']) else "  n/a  "
        lin_s    = f"{er['linear']*100:+.4f}" if not np.isnan(er['linear']) else "  n/a  "
        sh_s     = f"{er['sharpe_bin']:+.3f}" if not np.isnan(er['sharpe_bin']) else "  n/a "
        n_s      = str(er['n_bin'])
        w_s      = f"{weights.get(ticker, 0) * 100:.1f}" if ticker in weights else "—"
        chosen   = "◄ TOP" if ticker in top_tickers else ""
        excl_tag = "" if ticker in candidates else "[vol>max]"
        print(f"{ticker:<8} {vol_s:>7} {bin_s:>12} {lin_s:>12}"
              f" {sh_s:>11} {n_s:>6} {w_s:>7}  {chosen}{excl_tag}")

    # Ticker esclusi dal filtro
    for ticker in sorted(set(last_vols) - set(candidates)):
        vol_s = f"{last_vols[ticker] * 100:.2f}"
        print(f"{ticker:<8} {vol_s:>7} {'—':>12} {'—':>12} {'—':>11} {'—':>6} {'—':>7}  [vol>max]")

    print("\n  Allocazione suggerita (top selezionati):")
    for t in top_tickers:
        w   = weights.get(t, 0)
        exp = top_expected_binned.get(t, 0)
        print(f"    {t}: {w*100:.1f}% del capitale  →  E[profitto] = {exp*100:+.4f}% per trade")

    # ── Plot ──────────────────────────────────────────────────────
    if not args.no_plot:
        print("\nGenerazione grafici...")
        plot_scatter(df, models, out_dir)
        plot_binned(binned, out_dir)
        plot_heatmap(binned, out_dir, "mean_ah%", "mean AH ret %")
        plot_heatmap(binned, out_dir, "sharpe",   "Sharpe overnight")


if __name__ == "__main__":
    main()
