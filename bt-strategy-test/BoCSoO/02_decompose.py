#!/usr/bin/env python3
"""
02_decompose.py — Decomposizione AH/RTH dei rendimenti giornalieri Yahoo
========================================================================
Scompone il rendimento giornaliero di ogni asset in:
  - AH  (After Hours / Overnight): gap Adj Close(t-1) → Open(t)
  - RTH (Regular Trading Hours):  movimento intraday Open(t) → Close(t)

Formula (log returns, additivi):
    r_total = log(AdjClose_t / AdjClose_{t-1})   ← aggiustato dividendi+split
    r_rth   = log(Close_t   / Open_t)             ← intraday, stesso giorno
    r_ah    = r_total - r_rth                     ← residuo = overnight gap

Output:
    out/decompose_results.json  →  consumato da 03_report.py
    out/classified_ah.csv       →  asset AH-dominant
    out/classified_rth.csv      →  asset RTH-dominant
    out/classified_mixed.csv    →  asset misti

Usage:
    python 02_decompose.py

Richiede:
    - config-common/data/d/yahoo/*.csv   (100 asset + SPY)
    - pip: pandas numpy
"""

import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Configurazione
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent.parent

DATA_DIR = REPO_ROOT / "config-common" / "data" / "d" / "yahoo"
OUT_DIR  = BASE_DIR / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Soglie classificazione (basate sul periodo "all")
AH_THRESHOLD  = 0.60   # AH% > 60% del rendimento totale → AH dominant
RTH_THRESHOLD = 0.60   # RTH% > 60% → RTH dominant

# Periodi predefiniti per il report (start_date, None = dal primo dato)
PERIODS = {
    "all":   None,
    "2010+": "2010-01-01",
    "2015+": "2015-01-01",
    "2020+": "2020-01-01",
    "2023+": "2023-01-01",
}

# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def load_asset(symbol: str) -> pd.DataFrame | None:
    """Carica CSV Yahoo e restituisce DataFrame con index Date (tz-naive)."""
    path = DATA_DIR / f"{symbol}.csv"
    if not path.exists():
        return None

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = pd.read_csv(path, parse_dates=["Date"])

    df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.normalize().dt.tz_localize(None)
    df = df.set_index("Date").sort_index()
    df = df.rename(columns={"Adj Close": "AdjClose"})

    # Mantieni solo colonne necessarie
    required = {"Open", "Close", "AdjClose", "Volume"}
    if not required.issubset(df.columns):
        return None

    df = df[list(required)].dropna()
    df = df[(df["Volume"] > 0) & (df["Open"] > 0) & (df["Close"] > 0) & (df["AdjClose"] > 0)]
    return df if len(df) >= 60 else None


# ---------------------------------------------------------------------------
# Decomposizione
# ---------------------------------------------------------------------------

def compute_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Aggiunge colonne r_total, r_rth, r_ah al DataFrame."""
    out = df[["Open", "Close", "AdjClose"]].copy()
    out["r_total"] = np.log(df["AdjClose"] / df["AdjClose"].shift(1))
    out["r_rth"]   = np.log(df["Close"]    / df["Open"])
    out["r_ah"]    = out["r_total"] - out["r_rth"]
    return out.dropna(subset=["r_total", "r_rth", "r_ah"])


# ---------------------------------------------------------------------------
# Metriche
# ---------------------------------------------------------------------------

def compute_metrics(r: pd.Series) -> dict:
    """
    Calcola metriche di performance su una serie di log-return giornalieri.
    Annualizzazione: radice di 252 trading days.
    """
    r = r.dropna()
    n = len(r)
    if n < 20:
        return {}

    mean = float(r.mean())
    std  = float(r.std(ddof=1))

    sharpe = (mean / std * np.sqrt(252)) if std > 0 else 0.0

    neg = r[r < 0]
    down_std = float(neg.std(ddof=1)) if len(neg) > 1 else 0.0
    sortino = (mean / down_std * np.sqrt(252)) if down_std > 0 else 0.0

    # Curva equity su log-return cumulati
    cum_log = r.cumsum()
    equity  = np.expm1(cum_log)          # = exp(cumsum) - 1
    eq1     = equity + 1                 # equity che parte da 1
    peak    = eq1.cummax()
    dd      = (eq1 - peak) / peak

    max_dd   = float(dd.min())
    dd_days  = int((dd < -0.001).sum())
    total_ret = float(equity.iloc[-1])

    return {
        "total_return": round(total_ret  * 100, 2),
        "sharpe":       round(sharpe,           3),
        "sortino":      round(sortino,          3),
        "max_dd":       round(max_dd     * 100, 2),
        "dd_days":      dd_days,
        "n_days":       n,
    }


def compute_beta(r_asset: pd.Series, r_spy: pd.Series) -> float:
    """Beta OLS dell'asset vs SPY (allineati per data)."""
    aligned = pd.concat([r_asset, r_spy], axis=1).dropna()
    if len(aligned) < 60:
        return float("nan")
    cov = np.cov(aligned.iloc[:, 0].values, aligned.iloc[:, 1].values)
    return float(cov[0, 1] / cov[1, 1]) if cov[1, 1] > 0 else float("nan")


# ---------------------------------------------------------------------------
# Serializzazione serie mensili
# ---------------------------------------------------------------------------

def to_monthly(r: pd.Series) -> tuple[list[str], list[float]]:
    """Ricampiona log-return giornalieri in mensili (somma = approx corretta)."""
    m = r.resample("ME").sum()
    dates  = [d.strftime("%Y-%m") for d in m.index]
    values = [round(float(v), 6) for v in m.values]
    return dates, values


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    symbols_available = sorted(p.stem for p in DATA_DIR.glob("*.csv"))
    print(f"Asset trovati in {DATA_DIR}: {len(symbols_available)}")

    # ── Caricamento ──────────────────────────────────────────────────────────
    all_returns: dict[str, pd.DataFrame] = {}
    for sym in symbols_available:
        df = load_asset(sym)
        if df is None:
            print(f"  ⚠  {sym}: scartato (dati insufficienti)")
            continue
        ret = compute_returns(df)
        if len(ret) < 60:
            print(f"  ⚠  {sym}: scartato (return insufficienti)")
            continue
        all_returns[sym] = ret
        print(f"  ✓  {sym}: {len(ret)} giorni  ({ret.index[0].date()} – {ret.index[-1].date()})")

    symbols = list(all_returns.keys())
    spy_ret = all_returns.get("SPY")
    print(f"\nAsset caricati: {len(symbols)}")

    # ── Metriche per periodo ─────────────────────────────────────────────────
    period_metrics: dict[str, dict] = {}
    for period_name, start_date in PERIODS.items():
        period_metrics[period_name] = {}
        for sym in symbols:
            r = all_returns[sym]
            if start_date:
                r = r[r.index >= start_date]
            if len(r) < 20:
                continue

            total_log = float(r["r_total"].sum())
            ah_log    = float(r["r_ah"].sum())
            rth_log   = float(r["r_rth"].sum())

            # Gestisci asset piatti o in perdita (total_log ≈ 0)
            if abs(total_log) < 1e-6:
                ah_pct  = 50.0
                rth_pct = 50.0
            else:
                ah_pct  = round(ah_log  / total_log * 100, 1)
                rth_pct = round(rth_log / total_log * 100, 1)

            if ah_pct > AH_THRESHOLD * 100:
                classification = "AH"
            elif rth_pct > RTH_THRESHOLD * 100:
                classification = "RTH"
            else:
                classification = "Mixed"

            period_metrics[period_name][sym] = {
                "total":          compute_metrics(r["r_total"]),
                "ah":             compute_metrics(r["r_ah"]),
                "rth":            compute_metrics(r["r_rth"]),
                "ah_pct":         ah_pct,
                "rth_pct":        rth_pct,
                "classification": classification,
            }

    # ── Classificazione finale (periodo "all") ────────────────────────────────
    classifications = {
        sym: period_metrics["all"][sym]["classification"]
        for sym in symbols
        if sym in period_metrics.get("all", {})
    }

    # ── Stabilità: quante classificazioni distinte nei 5 periodi ─────────────
    # stable=True solo se n_unique==1 (stesso carattere in tutti i periodi con dati)
    stability_data: dict[str, dict] = {}
    for sym in symbols:
        classes_seen = [
            period_metrics[p][sym]["classification"]
            for p in PERIODS
            if sym in period_metrics.get(p, {})
        ]
        n_unique  = len(set(classes_seen))
        stability_data[sym] = {
            "n_unique":  n_unique,
            "n_periods": len(classes_seen),
            "stable":    n_unique == 1,
        }

    # ── Serie mensili (per filtro date dinamico nel report) ──────────────────
    # Date globali: unione di tutti i mesi presenti
    all_months: set[str] = set()
    for sym in symbols:
        dates_m, _ = to_monthly(all_returns[sym]["r_ah"])
        all_months.update(dates_m)
    all_months_sorted = sorted(all_months)

    monthly_data: dict[str, dict] = {}
    for sym in symbols:
        r = all_returns[sym]
        d_ah,  v_ah  = to_monthly(r["r_ah"])
        d_rth, v_rth = to_monthly(r["r_rth"])
        _,     v_tot = to_monthly(r["r_total"])

        # Allinea alle date globali (0 per mesi mancanti)
        local_map = {d: (a, rt, t) for d, a, rt, t in zip(d_ah, v_ah, v_rth, v_tot)}
        ah_al, rth_al, tot_al = [], [], []
        for m in all_months_sorted:
            v = local_map.get(m, (0.0, 0.0, 0.0))
            ah_al.append(v[0]);  rth_al.append(v[1]);  tot_al.append(v[2])

        monthly_data[sym] = {"ah": ah_al, "rth": rth_al, "total": tot_al}

    # ── Fattori per scatter plot ──────────────────────────────────────────────
    factor_data: dict[str, dict] = {}
    for sym in symbols:
        r   = all_returns[sym]
        df  = load_asset(sym)  # ricarico per il volume
        vol = float(np.log1p(
            df["Volume"].replace(0, np.nan).dropna()
        ).mean()) if df is not None else float("nan")

        beta = compute_beta(r["r_total"], spy_ret["r_total"]) if spy_ret is not None else float("nan")

        factor_data[sym] = {
            "ah_pct":        period_metrics["all"].get(sym, {}).get("ah_pct", 0),
            "log_volume":    round(vol, 3),
            "volatility":    round(float(r["r_total"].std() * np.sqrt(252)), 4),
            "log_price":     round(float(np.log(df["AdjClose"].dropna()).mean()), 3) if df is not None else float("nan"),
            "beta":          round(beta, 3) if not np.isnan(beta) else None,
            "classification": classifications.get(sym, "Mixed"),
        }

    # ── CSV classificazione ──────────────────────────────────────────────────
    rows = []
    for sym in symbols:
        m = period_metrics["all"].get(sym, {})
        if not m:
            continue
        rows.append({
            "symbol":         sym,
            "classification": classifications.get(sym, "Mixed"),
            "ah_pct":         m.get("ah_pct", 0),
            "rth_pct":        m.get("rth_pct", 0),
            "total_return_%": m["total"].get("total_return", 0),
            "ah_sharpe":      m["ah"].get("sharpe", 0),
            "rth_sharpe":     m["rth"].get("sharpe", 0),
            "ah_max_dd_%":    m["ah"].get("max_dd", 0),
            "rth_max_dd_%":   m["rth"].get("max_dd", 0),
        })

    df_class = pd.DataFrame(rows).sort_values("ah_pct", ascending=False)
    counts = {"AH": 0, "RTH": 0, "Mixed": 0}
    for cls, fname in [
        ("AH",    "classified_ah.csv"),
        ("RTH",   "classified_rth.csv"),
        ("Mixed", "classified_mixed.csv"),
    ]:
        subset = df_class[df_class["classification"] == cls]
        subset.to_csv(OUT_DIR / fname, index=False)
        counts[cls] = len(subset)
        print(f"  → {len(subset):3d} {cls}-dominant → out/{fname}")

    # ── Top-10 asset stabili per Sharpe (orizzonte 2-3 anni) ─────────────────
    # Usa periodo "2023+" (≈3 anni); fallback su "2020+" se dati insufficienti.
    # Esclude SPY (è il benchmark, non un'opportunità di trading).
    TICKER_OUT = REPO_ROOT / "config-common" / "tickers"
    TICKER_OUT.mkdir(parents=True, exist_ok=True)

    def total_sharpe_recent(sym: str) -> float:
        for period in ("2023+", "2020+"):
            m = period_metrics.get(period, {}).get(sym, {})
            if m and m["total"].get("n_days", 0) >= 252:
                return m["total"].get("sharpe", -999.0)
        return -999.0

    for cls_filter, fname in [("AH", "stable_ah_top10.json"), ("RTH", "stable_rth_top10.json")]:
        candidates = [
            s for s in symbols
            if stability_data[s]["stable"]
            and classifications.get(s) == cls_filter
            and s != "SPY"
        ]
        ranked = sorted(candidates, key=total_sharpe_recent, reverse=True)
        top10  = ranked[:10]

        print(f"\n=== Top 10 stabili {cls_filter} per Sharpe (2023+) ===")
        for sym in top10:
            print(f"  {sym:<8}  Sharpe={total_sharpe_recent(sym):.3f}")

        ticker_path = TICKER_OUT / fname
        with open(ticker_path, "w") as f:
            json.dump(top10, f, indent=4)
        print(f"✓ → {ticker_path}")

    # ── JSON principale ──────────────────────────────────────────────────────
    result = {
        "symbols":          symbols,
        "dates_monthly":    all_months_sorted,
        "periods":          list(PERIODS.keys()),
        "ah_threshold":     AH_THRESHOLD,
        "rth_threshold":    RTH_THRESHOLD,
        "period_metrics":   period_metrics,
        "monthly_data":     monthly_data,
        "factor_data":      factor_data,
        "classifications":  classifications,
        "stability":        stability_data,
        "counts":           counts,
    }

    json_path = OUT_DIR / "decompose_results.json"
    with open(json_path, "w") as f:
        json.dump(result, f, separators=(",", ":"))

    mb = json_path.stat().st_size / 1e6
    print(f"\n✓ {json_path}  ({mb:.1f} MB)")
    print(f"✓ Classificazione full-period: AH={counts['AH']}  RTH={counts['RTH']}  Mixed={counts['Mixed']}")


if __name__ == "__main__":
    run()
