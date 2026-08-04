#!/usr/bin/env python3
"""Fase 5 (v2) dello studio mesi negativi OvernightAH: CUSUM con baseline
MOBILE, non congelata una volta per sempre.

Perche' questa v2 esiste: la v1 (`bad_months_spc_cusum.py`, tuttora presente
come registro di cosa e' stato provato e perche' non basta) congela
mediana/MAD una sola volta sui primi `baseline_min_days` giorni osservati
("fase I" in stile controllo statistico di processo classico) e non le
ricalcola mai piu'. Questo funziona bene quando quella finestra iniziale e'
rappresentativa del regime che segue (verificato: train/validation/OOS
segmentati con warmup di ~1 anno recente, sempre entro il 2015-2026, davano
risultati solidi). **Ma fallisce clamorosamente su un run continuo
2000-2026**: la baseline si congela durante il crollo dot-com (2000-2001) e
resta quella per i 25 anni successivi, attraverso bolla, 2008, QE, COVID,
rialzo tassi 2022, boom AI — regimi radicalmente diversi. Risultato
osservato: capitale finale -90% e drawdown/Sharpe **peggiori** del baseline
senza overlay, non migliori. Nessuna versione precedente di questo studio
aveva testato un run continuo cosi' lungo (solo segmenti isolati), quindi il
problema non era emerso prima.

Critica dell'utente (corretta, guida questo redesign): anche "ricongelare
ogni tanto" (es. una volta l'anno) sarebbe fragile allo stesso modo — se
l'anno di riferimento scelto e' per caso estremo (pessimo o eccezionale),
quello diventa la nuova normalita' fino al prossimo ricongelamento,
producendo isteria (normalita' che salta da un estremo all'altro invece di
seguire un'impronta statistica che si evolve gradualmente). La normalita'
deve aggiornarsi di continuo e gradualmente, pesando piu' i dati recenti
senza scartare bruscamente il passato — esattamente cosa fa una finestra
MOBILE che scorre ogni giorno, a differenza di uno snapshot fisso
ricalcolato a intervalli discreti.

Disegno v2:
  1. Ogni giorno, mediana/MAD ricalcolate sulla finestra mobile dei
     `window_days` giorni STRETTAMENTE precedenti (ex-ante, nessun
     lookahead) — non piu' un singolo congelamento "fase I".
  2. z_t = (x_t - mediana_finestra) / scala_finestra.
  3. CUSUM come in v1: C_t = min(0, C_{t-1} + z_t - k); fermata se C_t < -h.
  4. Ripartenza come in v1 (K giorni entro i limiti + test bootstrap), ma il
     campione di riferimento per il bootstrap e' ora gli z-score della
     finestra mobile CORRENTE (che continua a scorrere anche durante la
     fermata), non un pool fisso congelato all'inizio.
  5. Calibrazione: griglia 2D su (window_days, h) sul train, conferma su
     validation, oos guardato solo a conferma — stessa disciplina di v1.

Usage:
  bt-core/.venv/bin/python bt-strategy-test/overnight-ah/research/bad_months_spc_cusum_rolling.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bad_months_signals import load_daily_panel  # noqa: E402
from bad_months_spc_cusum import (  # noqa: E402
    daily_potential_ah_gain_series,
    evaluate_rule,
    load_daily_returns,
    load_negative_months,
)

ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = Path(__file__).resolve().parent / "out" / "bad_months_study" / "rolling"

K_RESUME = 5
SLACK_K = 0.5
RESUME_RUN_RULE_Z = -1.0
BOOT_ALPHA = 0.10
N_BOOT = 1000
MIN_WINDOW_DAYS = 252  # ~1y minimo prima che il meccanismo si attivi

# Finestre mobili candidate (giorni di trading): 1, 1.5, 2, 2.5, 3 anni.
WINDOW_GRID = (252, 378, 504, 630, 756)
H_GRID = (4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0, 23.0, 26.0, 30.0)


def robust_median_scale(window: np.ndarray) -> tuple[float, float]:
    median = float(np.median(window))
    mad = float(np.median(np.abs(window - median)))
    return median, 1.4826 * mad


def rolling_zscore(x: pd.Series, window_days: int, min_window_days: int = MIN_WINDOW_DAYS) -> pd.Series:
    """z_t usando mediana/MAD della finestra [t-window_days, t) — esclude
    il giorno stesso, ex-ante puro."""
    values = x.to_numpy()
    n = len(values)
    z = np.full(n, np.nan)
    for i in range(n):
        start = max(0, i - window_days)
        window = values[start:i]
        if len(window) < min_window_days:
            continue
        median, scale = robust_median_scale(window)
        if scale <= 0 or not np.isfinite(scale):
            continue
        z[i] = (values[i] - median) / scale
    return pd.Series(z, index=x.index)


def cusum_halt_days_rolling(
    x: pd.Series,
    k: float,
    h: float,
    K: int,
    window_days: int,
    min_window_days: int = MIN_WINDOW_DAYS,
    boot_alpha: float = BOOT_ALPHA,
    n_boot: int = N_BOOT,
    seed: int = 42,
) -> pd.DataFrame:
    values = x.to_numpy()
    dates = x.index.to_numpy()
    n = len(values)
    rng = np.random.default_rng(seed)
    C = 0.0
    halted = False
    z_history: list[float] = []
    rows = []
    for i in range(n):
        start = max(0, i - window_days)
        window = values[start:i]
        if len(window) < min_window_days:
            z_history.append(np.nan)
            rows.append({"date": dates[i], "z": np.nan, "C": C, "halted": halted, "event": ""})
            continue
        median, scale = robust_median_scale(window)
        if scale <= 0 or not np.isfinite(scale):
            z_history.append(np.nan)
            rows.append({"date": dates[i], "z": np.nan, "C": C, "halted": halted, "event": ""})
            continue
        zt = (values[i] - median) / scale
        z_history.append(zt)

        if not halted:
            C = min(0.0, C + zt - k)
            trigger = C < -h
            rows.append({"date": dates[i], "z": zt, "C": C, "halted": False, "event": "halt_start" if trigger else ""})
            if trigger:
                halted = True
                C = 0.0
        else:
            window_z = (window - median) / scale
            recent = z_history[-K:]
            run_ok = len(recent) == K and all(np.isfinite(recent)) and all(v > RESUME_RUN_RULE_Z for v in recent)
            resume = False
            p_value = np.nan
            if run_ok and len(window_z) > 0:
                boot_means = rng.choice(window_z, size=(n_boot, K), replace=True).mean(axis=1)
                observed_mean = float(np.mean(recent))
                p_value = float((boot_means <= observed_mean).mean())
                resume = p_value > boot_alpha
            rows.append(
                {
                    "date": dates[i],
                    "z": zt,
                    "C": np.nan,
                    "halted": True,
                    "event": "resume" if resume else "",
                    "resume_pvalue": p_value,
                }
            )
            if resume:
                halted = False
                C = 0.0
    return pd.DataFrame(rows)


def grid_search_window_h(
    x_train: pd.Series,
    negative_months: pd.DataFrame,
    daily_returns: pd.Series,
    k: float = SLACK_K,
    K: int = K_RESUME,
    window_grid=WINDOW_GRID,
    h_grid=H_GRID,
) -> pd.DataFrame:
    """negative_months deve essere la tabella COMPLETA (tutti i segmenti);
    filtra internamente a segment='train'."""
    rows = []
    for window_days in window_grid:
        for h in h_grid:
            halt_df = cusum_halt_days_rolling(x_train, k, h, K, window_days)
            metrics = evaluate_rule(halt_df, daily_returns, negative_months, segment="train")
            metrics["window_days"] = window_days
            metrics["h"] = h
            rows.append(metrics)
    return pd.DataFrame(rows)


def portfolio_metrics_from_halts(halt_df: pd.DataFrame, daily_returns: pd.Series) -> dict:
    df = halt_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    ret = daily_returns.reindex(df.index).fillna(0.0)
    realized = ret.where(~df["halted"], 0.0)
    equity = (1.0 + realized).cumprod()
    dd = equity / equity.cummax() - 1.0
    sharpe = realized.mean() / realized.std(ddof=1) * np.sqrt(252) if realized.std(ddof=1) else np.nan
    return {
        "final_multiple": float(equity.iloc[-1]),
        "maxdd_pct": float(dd.min() * 100),
        "sharpe": float(sharpe),
        "total_halted_days": int(df["halted"].sum()),
        "n_days": len(df),
    }


def run(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    panel = load_daily_panel()
    ah_gain_mean, _ = daily_potential_ah_gain_series(panel)

    policy = "switch"
    daily_returns = load_daily_returns(policy)
    negative_months = load_negative_months(policy)

    ah_gain_full = ah_gain_mean.reindex(daily_returns.index).ffill()
    train_period = negative_months.loc[negative_months["segment"] == "train", "month_period"]
    x_train = ah_gain_full[ah_gain_full.index.to_period("M").isin(train_period)]

    grid = grid_search_window_h(x_train, negative_months, daily_returns)
    grid.to_csv(out_dir / "rolling_grid_search_train.csv", index=False)

    # Anche le metriche di portafoglio dirette (Sharpe/maxDD nativi-proxy),
    # non solo Youden — lezione della calibrazione precedente: Youden da
    # solo puo' selezionare una soglia troppo sensibile.
    port_rows = []
    for window_days in WINDOW_GRID:
        for h in H_GRID:
            halt_df = cusum_halt_days_rolling(x_train, SLACK_K, h, K_RESUME, window_days)
            pm = portfolio_metrics_from_halts(halt_df, daily_returns.reindex(x_train.index))
            pm["window_days"] = window_days
            pm["h"] = h
            port_rows.append(pm)
    port_df = pd.DataFrame(port_rows)
    port_df.to_csv(out_dir / "rolling_grid_search_train_portfolio.csv", index=False)

    print(f"wrote {out_dir}")
    print("Top 10 by Sharpe (train, proxy portfolio metrics):")
    print(port_df.sort_values("sharpe", ascending=False).head(10).to_string(index=False))
    print()
    print("Top 10 by Youden (train, month classification):")
    print(grid.sort_values("youden_index", ascending=False).head(10).to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fase 5 v2: CUSUM a baseline mobile OvernightAH")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args().out_dir)
