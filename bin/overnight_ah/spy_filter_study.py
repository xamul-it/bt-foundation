"""
spy_filter_study.py — Analisi filtro SPY per strategie overnight_ah
====================================================================
Carica i risultati del walk-forward e i dati SPY giornalieri.
Per ogni sessione di trading calcola il contesto SPY (trend, momentum, drawdown).
Identifica le condizioni SPY durante i due drawdown principali e testa
vari filtri soglia in termini di costo/beneficio.

Usage:
    python spy_filter_study.py [--wf-csv PATH] [--spy-csv PATH] [--out-dir DIR]
"""

import argparse
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_HERE    = Path(__file__).parent
_OUT_DIR = _HERE / "out"
_WF_CSV  = _HERE / "out" / "vol_ah_walkforward.csv"
_SPY_CSV = Path(__file__).parent.parent.parent / "config-common" / "data" / "d" / "yahoo" / "SPY.csv"

# Drawdown periods di interesse (inclusive)
DD_PERIODS = [
    ("DD1 Jul-Aug 2024",   "2024-07-10", "2024-08-05"),
    ("DD2 Jan-Apr 2025",   "2025-01-15", "2025-04-10"),
]

# ---------------------------------------------------------------------------
# Funzioni di supporto
# ---------------------------------------------------------------------------

def load_spy(spy_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(spy_csv, parse_dates=["Date"])
    df = df.rename(columns={"Date": "date", "Close": "close", "Open": "open",
                             "High": "high", "Low": "low", "Volume": "volume"})
    df["date"] = df["date"].dt.tz_localize(None).dt.normalize()
    df = df.sort_values("date").reset_index(drop=True)

    c = df["close"]

    # Rendimenti rolling
    for n in [1, 3, 5, 10, 20]:
        df[f"spy_ret_{n}d"] = c.pct_change(n)

    # Posizione vs SMA
    for n in [10, 20, 50]:
        df[f"spy_sma{n}"] = c.rolling(n).mean()
        df[f"spy_above_sma{n}"] = (c > df[f"spy_sma{n}"]).astype(int)

    # Rolling drawdown da picco mobile 20 giorni
    for w in [10, 20]:
        peak = c.rolling(w, min_periods=1).max()
        df[f"spy_dd_{w}d"] = (c - peak) / peak  # negativo = sotto il picco

    # Consecutive negative close-close days
    daily_ret = c.pct_change()
    neg = (daily_ret < 0).astype(int)
    consec = neg.copy()
    for i in range(1, len(df)):
        if neg.iloc[i] == 1:
            consec.iloc[i] = consec.iloc[i - 1] + 1
        else:
            consec.iloc[i] = 0
    df["spy_consec_neg"] = consec

    # Realized vol 10d
    df["spy_vol_10d"] = daily_ret.rolling(10).std() * np.sqrt(252)

    return df[["date", "close",
               "spy_ret_1d", "spy_ret_3d", "spy_ret_5d", "spy_ret_10d", "spy_ret_20d",
               "spy_above_sma10", "spy_above_sma20", "spy_above_sma50",
               "spy_dd_10d", "spy_dd_20d",
               "spy_consec_neg", "spy_vol_10d"]].copy()


def load_wf(wf_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(wf_csv, parse_dates=["date"])
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df.sort_values("date").reset_index(drop=True)


def label_dd(date_series: pd.Series) -> pd.Series:
    labels = pd.Series("normal", index=date_series.index)
    for name, start, end in DD_PERIODS:
        mask = (date_series >= start) & (date_series <= end)
        labels[mask] = name
    return labels


def cumulative_return(ret_series: pd.Series) -> pd.Series:
    return (1 + ret_series).cumprod() - 1


# ---------------------------------------------------------------------------
# Analisi: confronto feature SPY in DD vs normal
# ---------------------------------------------------------------------------

def feature_comparison(merged: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    rows = []
    for feat in features:
        for period_name, start, end in DD_PERIODS:
            dd_mask  = (merged["dd_label"] == period_name)
            norm_mask = (merged["dd_label"] == "normal")
            dd_vals   = merged.loc[dd_mask, feat].dropna()
            norm_vals = merged.loc[norm_mask, feat].dropna()
            rows.append({
                "feature":     feat,
                "period":      period_name,
                "dd_mean":     dd_vals.mean(),
                "dd_p25":      dd_vals.quantile(0.25),
                "dd_median":   dd_vals.median(),
                "dd_p75":      dd_vals.quantile(0.75),
                "normal_mean": norm_vals.mean(),
                "normal_median": norm_vals.median(),
                "delta":       dd_vals.mean() - norm_vals.mean(),
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Cost/benefit di un filtro binario
# ---------------------------------------------------------------------------

def test_filter(merged: pd.DataFrame, condition: pd.Series, ret_col: str = "ret_equal") -> dict:
    """
    condition: True = salta la sessione (non entro)
    Ritorna un dizionario con le metriche di costo/beneficio.
    """
    skipped  = merged[condition]
    kept     = merged[~condition]

    n_total   = len(merged)
    n_skipped = condition.sum()
    n_kept    = n_total - n_skipped

    # Rendimento cumulato senza filtro
    cum_base  = cumulative_return(merged[ret_col])

    # Rendimento cumulato con filtro (sessioni saltate = 0)
    ret_filtered = merged[ret_col].copy()
    ret_filtered[condition] = 0.0
    cum_filt  = cumulative_return(ret_filtered)

    # Sharpe annualizzato (252 sessioni)
    def sharpe(r):
        if r.std() == 0:
            return np.nan
        return r.mean() / r.std() * np.sqrt(252)

    # Sessioni saltate nei DD vs fuori
    dd_mask  = merged["dd_label"] != "normal"
    dd_skip  = condition[dd_mask].sum()
    dd_total = dd_mask.sum()
    ok_skip  = condition[~dd_mask].sum()
    ok_total = (~dd_mask).sum()

    # Max DD
    def max_dd(cum):
        peak = cum.cummax()
        dd   = (cum - peak) / (1 + peak)
        return dd.min()

    return {
        "n_total":        n_total,
        "n_skipped":      int(n_skipped),
        "pct_skipped":    n_skipped / n_total * 100,
        "dd_skip_rate":   dd_skip / dd_total * 100 if dd_total > 0 else 0,
        "ok_skip_rate":   ok_skip / ok_total * 100 if ok_total > 0 else 0,
        "base_total_ret": cum_base.iloc[-1] * 100,
        "filt_total_ret": cum_filt.iloc[-1] * 100,
        "base_sharpe":    sharpe(merged[ret_col]),
        "filt_sharpe":    sharpe(ret_filtered),
        "base_max_dd":    max_dd(cum_base) * 100,
        "filt_max_dd":    max_dd(cum_filt) * 100,
        "ret_skipped_sessions_mean_bps": skipped[ret_col].mean() * 10000,
    }


# ---------------------------------------------------------------------------
# Sweep di soglie per ogni feature
# ---------------------------------------------------------------------------

def sweep_thresholds(merged: pd.DataFrame, ret_col: str = "ret_equal") -> pd.DataFrame:
    filters_to_test = [
        # (nome, colonna, direzione, soglie)
        # direzione "lt" = salta quando col < soglia (bear signal)
        # direzione "gt" = salta quando col > soglia
        ("spy_ret_5d < X",    "spy_ret_5d",    "lt", [-0.07, -0.05, -0.04, -0.03, -0.02, -0.01]),
        ("spy_ret_10d < X",   "spy_ret_10d",   "lt", [-0.10, -0.07, -0.05, -0.03, -0.02]),
        ("spy_ret_20d < X",   "spy_ret_20d",   "lt", [-0.12, -0.10, -0.07, -0.05, -0.03]),
        ("spy_dd_20d < X",    "spy_dd_20d",    "lt", [-0.08, -0.06, -0.05, -0.04, -0.03, -0.02]),
        ("spy_dd_10d < X",    "spy_dd_10d",    "lt", [-0.06, -0.05, -0.04, -0.03, -0.02]),
        ("spy_consec_neg >= X","spy_consec_neg","gt", [2, 3, 4, 5]),
        ("spy_vol_10d > X",   "spy_vol_10d",   "gt", [0.15, 0.20, 0.25, 0.30]),
        ("spy_above_sma20==0","spy_above_sma20","lt", [1]),   # salta se sotto SMA20
        ("spy_above_sma50==0","spy_above_sma50","lt", [1]),   # salta se sotto SMA50
    ]

    rows = []
    for fname, col, direction, thresholds in filters_to_test:
        for thr in thresholds:
            if direction == "lt":
                cond = merged[col] < thr
            else:
                cond = merged[col] >= thr
            m = test_filter(merged, cond, ret_col)
            rows.append({
                "filter":         f"{fname} ({thr})",
                "pct_skip":       round(m["pct_skipped"], 1),
                "dd_skip_rate":   round(m["dd_skip_rate"], 1),
                "ok_skip_rate":   round(m["ok_skip_rate"], 1),
                "base_ret_pct":   round(m["base_total_ret"], 2),
                "filt_ret_pct":   round(m["filt_total_ret"], 2),
                "delta_ret_pp":   round(m["filt_total_ret"] - m["base_total_ret"], 2),
                "base_sharpe":    round(m["base_sharpe"], 3),
                "filt_sharpe":    round(m["filt_sharpe"], 3),
                "delta_sharpe":   round(m["filt_sharpe"] - m["base_sharpe"], 3),
                "base_max_dd_pct":round(m["base_max_dd"], 2),
                "filt_max_dd_pct":round(m["filt_max_dd"], 2),
                "delta_max_dd_pp":round(m["filt_max_dd"] - m["base_max_dd"], 2),
                "avg_ret_skipped_bps": round(m["ret_skipped_sessions_mean_bps"], 2),
            })
    return pd.DataFrame(rows).sort_values("delta_sharpe", ascending=False)


# ---------------------------------------------------------------------------
# Plot
# ---------------------------------------------------------------------------

def plot_cumulative(merged: pd.DataFrame, best_filters: list, out_dir: Path):
    fig, axes = plt.subplots(2, 1, figsize=(14, 9), sharex=True)

    # Panel 1: equity curve base vs filtri migliori
    ax1 = axes[0]
    dates = merged["date"]
    cum_base = cumulative_return(merged["ret_equal"])
    ax1.plot(dates, cum_base * 100, label="No filter", color="steelblue", lw=1.5)

    colors = ["darkorange", "green", "crimson"]
    for i, (label, cond) in enumerate(best_filters[:3]):
        ret_f = merged["ret_equal"].copy()
        ret_f[cond] = 0.0
        cum_f = cumulative_return(ret_f)
        ax1.plot(dates, cum_f * 100, label=label, color=colors[i], lw=1.2, linestyle="--")

    # Ombreggia DD periods
    for name, start, end in DD_PERIODS:
        ax1.axvspan(pd.Timestamp(start), pd.Timestamp(end), alpha=0.12, color="red", label=name)

    ax1.set_ylabel("Cumulative return (%)")
    ax1.set_title("Walk-forward: impatto filtro SPY sulla curva equity (equal sizing)")
    ax1.legend(fontsize=8)
    ax1.grid(True, alpha=0.3)

    # Panel 2: SPY close normalizzato + regime indicators
    ax2 = axes[1]
    spy_close = merged["close"] / merged["close"].iloc[0] * 100
    ax2.plot(dates, spy_close, color="gray", lw=1.2, label="SPY (idx=100)")
    ax2.axhline(100, color="gray", lw=0.5, linestyle=":")

    for name, start, end in DD_PERIODS:
        ax2.axvspan(pd.Timestamp(start), pd.Timestamp(end), alpha=0.12, color="red")

    ax2.set_ylabel("SPY (base 100)")
    ax2.set_xlabel("Data")
    ax2.legend(fontsize=8)
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    fig.autofmt_xdate()

    plt.tight_layout()
    out_path = out_dir / "spy_filter_equity.png"
    plt.savefig(out_path, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"Plot salvato: {out_path}")


def plot_spy_features_during_dd(merged: pd.DataFrame, features: list[str], out_dir: Path):
    fig, axes = plt.subplots(len(features), 1, figsize=(14, 3 * len(features)), sharex=True)
    if len(features) == 1:
        axes = [axes]

    dates = merged["date"]
    for ax, feat in zip(axes, features):
        ax.plot(dates, merged[feat], color="steelblue", lw=1.0, label=feat)
        ax.axhline(0, color="black", lw=0.5, linestyle=":")
        for name, start, end in DD_PERIODS:
            ax.axvspan(pd.Timestamp(start), pd.Timestamp(end), alpha=0.15, color="red")
        ax.set_ylabel(feat, fontsize=8)
        ax.grid(True, alpha=0.3)

    axes[0].set_title("SPY features durante i drawdown (aree rosse)")
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    axes[-1].xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    fig.autofmt_xdate()
    plt.tight_layout()
    out_path = out_dir / "spy_features_dd.png"
    plt.savefig(out_path, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"Plot salvato: {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wf-csv",  default=str(_WF_CSV),  help="Walkforward CSV")
    ap.add_argument("--spy-csv", default=str(_SPY_CSV), help="SPY daily CSV (yahoo)")
    ap.add_argument("--out-dir", default=str(_OUT_DIR), help="Output directory")
    ap.add_argument("--ret-col", default="ret_equal",   help="Colonna rendimento (ret_equal, ret_hybrid, ret_mw)")
    ap.add_argument("--no-plot", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Carica dati
    print("Carico SPY...")
    spy = load_spy(Path(args.spy_csv))
    print(f"  SPY: {len(spy)} righe, {spy['date'].min().date()} – {spy['date'].max().date()}")

    print("Carico walk-forward...")
    wf = load_wf(Path(args.wf_csv))
    print(f"  WF: {len(wf)} righe, {wf['date'].min().date()} – {wf['date'].max().date()}")

    # Join: ogni sessione WF ottiene il contesto SPY del giorno precedente (lag=1)
    # L'idea è usare solo informazioni disponibili a mercato chiuso del giorno prima
    spy_lag = spy.copy()
    spy_lag["date"] = spy_lag["date"] + pd.Timedelta(days=1)
    # Ma il mercato non apre ogni giorno → usiamo merge_asof con direction="backward"
    merged = pd.merge_asof(
        wf.sort_values("date"),
        spy.sort_values("date"),
        on="date",
        direction="backward",
        tolerance=pd.Timedelta("5d"),
    )

    # Aggiunge etichetta DD
    merged["dd_label"] = label_dd(merged["date"])

    print(f"\nSessioni totali dopo merge: {len(merged)}")
    print(f"  normal: {(merged['dd_label']=='normal').sum()}")
    for name, _, _ in DD_PERIODS:
        print(f"  {name}: {(merged['dd_label']==name).sum()}")

    # ---------------------------------------------------------------------------
    # 1. Feature comparison durante DD vs normal
    # ---------------------------------------------------------------------------
    features = ["spy_ret_5d", "spy_ret_10d", "spy_ret_20d",
                "spy_dd_10d", "spy_dd_20d",
                "spy_consec_neg", "spy_vol_10d",
                "spy_above_sma20", "spy_above_sma50"]

    print("\n=== Confronto feature SPY: DD vs normal ===")
    comp = feature_comparison(merged, features)
    print(comp.to_string(index=False))
    comp.to_csv(out_dir / "spy_feature_comparison.csv", index=False)

    # ---------------------------------------------------------------------------
    # 2. Sweep soglie
    # ---------------------------------------------------------------------------
    print(f"\n=== Sweep filtri SPY (rendimento base: {args.ret_col}) ===")
    sweep = sweep_thresholds(merged, ret_col=args.ret_col)
    print(sweep.head(20).to_string(index=False))
    sweep.to_csv(out_dir / "spy_filter_sweep.csv", index=False)

    # ---------------------------------------------------------------------------
    # 3. Dettaglio sui 3 filtri migliori (delta_sharpe)
    # ---------------------------------------------------------------------------
    print("\n=== Top 5 filtri per delta Sharpe ===")
    top5 = sweep.head(5)
    for _, row in top5.iterrows():
        print(f"\n  {row['filter']}")
        print(f"    Skip: {row['pct_skip']:.1f}% sessioni totali")
        print(f"    Skip in DD:  {row['dd_skip_rate']:.1f}%  |  Skip fuori DD: {row['ok_skip_rate']:.1f}%")
        print(f"    Ret base: {row['base_ret_pct']:.2f}%  →  filtrato: {row['filt_ret_pct']:.2f}%  (Δ {row['delta_ret_pp']:+.2f}pp)")
        print(f"    Sharpe base: {row['base_sharpe']:.3f}  →  filtrato: {row['filt_sharpe']:.3f}  (Δ {row['delta_sharpe']:+.3f})")
        print(f"    Max DD base: {row['base_max_dd_pct']:.2f}%  →  filtrato: {row['filt_max_dd_pct']:.2f}%  (Δ {row['delta_max_dd_pp']:+.2f}pp)")
        print(f"    Ret medio sessioni saltate: {row['avg_ret_skipped_bps']:.1f} bps")

    # ---------------------------------------------------------------------------
    # 4. Plot
    # ---------------------------------------------------------------------------
    if not args.no_plot:
        # Costruisci condizioni top-3
        top3 = sweep.head(3)
        best_filters = []
        for _, row in top3.iterrows():
            fname = row["filter"]
            # Ricostruisce la condizione dal nome del filtro (approccio semplice: ri-sweeppa)
        # Ri-sweeppa per ottenere le condizioni reali
        filters_defs = [
            ("spy_ret_5d < X",    "spy_ret_5d",    "lt", [-0.07, -0.05, -0.04, -0.03, -0.02, -0.01]),
            ("spy_ret_10d < X",   "spy_ret_10d",   "lt", [-0.10, -0.07, -0.05, -0.03, -0.02]),
            ("spy_ret_20d < X",   "spy_ret_20d",   "lt", [-0.12, -0.10, -0.07, -0.05, -0.03]),
            ("spy_dd_20d < X",    "spy_dd_20d",    "lt", [-0.08, -0.06, -0.05, -0.04, -0.03, -0.02]),
            ("spy_dd_10d < X",    "spy_dd_10d",    "lt", [-0.06, -0.05, -0.04, -0.03, -0.02]),
            ("spy_consec_neg >= X","spy_consec_neg","gt", [2, 3, 4, 5]),
            ("spy_vol_10d > X",   "spy_vol_10d",   "gt", [0.15, 0.20, 0.25, 0.30]),
            ("spy_above_sma20==0","spy_above_sma20","lt", [1]),
            ("spy_above_sma50==0","spy_above_sma50","lt", [1]),
        ]
        label_to_cond = {}
        for fname, col, direction, thresholds in filters_defs:
            for thr in thresholds:
                key = f"{fname} ({thr})"
                if direction == "lt":
                    label_to_cond[key] = merged[col] < thr
                else:
                    label_to_cond[key] = merged[col] >= thr

        best_filters_list = []
        for _, row in top3.iterrows():
            k = row["filter"]
            if k in label_to_cond:
                best_filters_list.append((k, label_to_cond[k]))

        if best_filters_list:
            plot_cumulative(merged, best_filters_list, out_dir)

        plot_spy_features_during_dd(
            merged,
            ["spy_ret_5d", "spy_ret_10d", "spy_dd_20d", "spy_consec_neg", "spy_vol_10d"],
            out_dir,
        )

    print(f"\nOutput salvato in: {out_dir}")


if __name__ == "__main__":
    main()
