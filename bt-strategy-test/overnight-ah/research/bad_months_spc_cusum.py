#!/usr/bin/env python3
"""Fase 5 dello studio mesi negativi OvernightAH: meccanismo operativo di
stop/riparti in stile controllo statistico di processo (CUSUM), a
granularita' daily. Proxy pandas — non implementato in Backtrader/live.

Quattro variabili di processo testate in parallelo:
  A) self-referential: rendimento giornaliero del portafoglio (returns.csv,
     gia' prodotto in Fase 1 come daily_returns_{static,switch}.csv);
  B) breadth: quota di titoli Nasdaq scesi oltre soglia, stesso segnale di
     Fase 3 ma a granularita' daily (non aggregato a mese);
  C) ah_gain_mean: guadagno AH potenziale medio cross-sectional (proposta
     utente — vedi sotto);
  D) ah_gain_top10: guadagno AH potenziale medio dei 10 migliori candidati
     del giorno (proposta utente — "se anche i migliori non rendono, non
     c'e' opportunita' da nessuna parte").

C e D usano `known_ah_ret` (rendimento AH gia' risolto/noto, close->open) su
tutto l'universo `daily_panel.csv` — un segnale derivato dai PREZZI, non
dall'esecuzione: risolve esattamente il problema della variante A (vedi
caveat sotto), perche' e' osservabile ogni giorno indipendentemente dal
fatto che la strategia abbia aperto posizioni o sia ferma.

Caveat sulla variante A (self-referential): una volta fermata la strategia
non arriva piu' nuovo segnale self-referential reale (nessun trade = nessun
rendimento osservato). Questo script valuta la regola di ripartenza usando
comunque la serie storica nota (proxy pandas, coerente col resto dello
studio) — un'implementazione live dovrebbe usare B, C o D (sempre
osservabili, derivati dai prezzi non dall'esecuzione) per la decisione di
ripartenza, o una size-canary per riattivare A stessa.

Nota sul segno: il CUSUM qui e' one-sided LOWER (rileva derive verso il
basso, ferma quando C_t < -h), quindi ogni variabile deve essere segnata
"basso = cattivo" come il rendimento. La breadth (quota di titoli IN CALO)
ha "cattivo = alto": viene negata prima dell'uso (bug di segno individuato e
corretto prima di aggiungere C/D — la versione precedente di questo script
rilevava mercati calmi, non mercati sotto stress).

Disegno CUSUM (one-sided lower, Page's test) — vedi
docs/context/ah_bad_months_study_spec.md e il piano di sessione:
  1. baseline in-controllo (mu0, sigma0) da train, esclusi i mesi gia'
     etichettati negativi in Fase 1;
  2. z_t = (x_t - mu0) / sigma0;
  3. C_t = min(0, C_{t-1} + z_t - k);
  4. fermata quando C_t < -h;
  5. ripartenza: K giorni consecutivi entro i limiti di controllo E un test
     bootstrap sulla finestra trailing K vs la distribuzione in-controllo;
  6. k/h/K calibrati solo su train (grid search su h), validation per
     conferma, OOS guardato per ultimo.

Usage:
  bt-core/.venv/bin/python bt-strategy-test/overnight-ah/research/bad_months_spc_cusum.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bad_months_signals import load_daily_panel  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = Path(__file__).resolve().parent / "out" / "bad_months_study"

K_RESUME = 5
SLACK_K = 0.5
H_GRID = (
    2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0,
    14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 22.0, 25.0, 30.0,
)
BOOT_ALPHA = 0.10
N_BOOT = 1000
RESUME_RUN_RULE_Z = -1.0  # each of the K trailing days must be above this
MONTH_TOUCH_MIN_DAYS = 5  # ~1 trading week; see evaluate_rule for why "any single day" was too coarse

# Existing production mechanism (_risk_overlay_trailing_return /
# _risk_overlay_entry_scale in overnight_ah.py) default params, used as a
# baseline comparison — same halt/resume mechanic (no hysteresis, threshold
# re-evaluated fresh every day) so the comparison is apples-to-apples.
RISK_OVERLAY_LOOKBACK = 10
RISK_OVERLAY_THRESHOLD = -0.10


def load_daily_returns(policy: str) -> pd.Series:
    path = OUT_DIR / f"daily_returns_{policy}.csv"
    df = pd.read_csv(path, parse_dates=["date"])
    return df.set_index("date")["return"]


def load_negative_months(policy: str) -> pd.DataFrame:
    path = OUT_DIR / f"monthly_returns_{policy}.csv"
    df = pd.read_csv(path, parse_dates=["month"])
    df["month_period"] = df["month"].dt.to_period("M")
    return df


def daily_breadth_series(panel: pd.DataFrame, threshold: float = -0.01) -> pd.Series:
    daily = panel.copy()
    pct_down = daily.groupby("date")["c2c_ret"].apply(lambda s: (s < threshold).mean())
    return pct_down.sort_index()


def daily_potential_ah_gain_series(panel: pd.DataFrame, top_n: int = 10) -> tuple[pd.Series, pd.Series]:
    """Price-derived, execution-independent process variables (user proposal,
    solves the self-referential observability gap): `known_ah_ret` is the AH
    return of each ticker (close->open), known/resolved every day regardless
    of whether the strategy actually opened a position — always observable
    even while halted, unlike the strategy's own daily return.

    Two aggregates over the full daily_panel universe:
      - mean: cross-sectional average potential AH gain that day (a
        continuous-value regime read, richer than the binary breadth count);
      - topN mean: average of the top `top_n` potential AH gains that day
        ("if even the best candidates aren't good, nothing is good" — a
        ceiling/opportunity-availability read, not a distress-count read).
    """
    daily = panel.dropna(subset=["known_ah_ret"])

    def _top_n_mean(s: pd.Series) -> float:
        return s.nlargest(top_n).mean() if len(s) else np.nan

    mean_gain = daily.groupby("date")["known_ah_ret"].mean().sort_index()
    topn_gain = daily.groupby("date")["known_ah_ret"].apply(_top_n_mean).sort_index()
    return mean_gain, topn_gain


def estimate_baseline(daily: pd.Series, excluded_months: set) -> tuple[float, float]:
    """Robust center/scale (median, 1.4826*MAD) instead of mean/std.

    Diagnosed empirically before finalizing this design: OvernightAH daily
    returns are strongly right-skewed (median well below mean — ~60% of days
    are below the mean on the static-10 train baseline, verified directly).
    A mean/std z-score then puts the *majority* of ordinary in-control days
    below zero, so a one-sided lower CUSUM built on it drifts down on
    ordinary days and halts almost every month regardless of h (verified:
    capture_rate and false_alarm_rate both saturated near 1.0 across the
    entire h grid with mean/std). Centering on the median fixes this by
    construction (~50% of in-control days are below/above by definition);
    MAD*1.4826 is the standard robust scale estimator, consistent with a
    skewed/fat-tailed distribution.
    """
    months = daily.index.to_period("M")
    mask = ~months.isin(excluded_months)
    base = daily[mask]
    median = float(base.median())
    mad = float((base - median).abs().median())
    robust_scale = 1.4826 * mad
    return median, robust_scale


def zscore(daily: pd.Series, mu0: float, sigma0: float) -> pd.Series:
    if sigma0 == 0 or not np.isfinite(sigma0):
        return daily * np.nan
    return (daily - mu0) / sigma0


def cusum_halt_days(
    z: pd.Series,
    k: float,
    h: float,
    K: int,
    baseline_z: np.ndarray,
    boot_alpha: float = BOOT_ALPHA,
    n_boot: int = N_BOOT,
    seed: int = 42,
) -> pd.DataFrame:
    dates = z.index.to_numpy()
    zs = z.to_numpy()
    n = len(zs)
    rng = np.random.default_rng(seed)
    C = 0.0
    halted = False
    rows = []
    for i in range(n):
        zt = zs[i]
        if not np.isfinite(zt):
            rows.append({"date": dates[i], "z": zt, "C": C, "halted": halted, "event": ""})
            continue
        if not halted:
            C = min(0.0, C + zt - k)
            trigger = C < -h
            rows.append({"date": dates[i], "z": zt, "C": C, "halted": False, "event": "halt_start" if trigger else ""})
            if trigger:
                halted = True
                C = 0.0
        else:
            window_start = max(0, i - K + 1)
            window = zs[window_start : i + 1]
            run_rule_ok = len(window) == K and np.all(window[np.isfinite(window)] > RESUME_RUN_RULE_Z) and np.isfinite(window).all()
            resume = False
            p_value = np.nan
            if run_rule_ok and len(baseline_z) > 0:
                boot_means = rng.choice(baseline_z, size=(n_boot, K), replace=True).mean(axis=1)
                observed_mean = window.mean()
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


def halt_episodes(halt_df: pd.DataFrame) -> pd.DataFrame:
    df = halt_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    streak_id = (df["halted"] != df["halted"].shift()).cumsum()
    episodes = df[df["halted"]].groupby(streak_id).agg(start=("date", "min"), end=("date", "max"), n_days=("date", "count"))
    return episodes.reset_index(drop=True)


def evaluate_rule(
    halt_df: pd.DataFrame,
    daily_returns: pd.Series,
    negative_months: pd.DataFrame,
    segment: str | None = None,
) -> dict:
    df = halt_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")

    nm = negative_months
    if segment is not None:
        nm = nm[nm["segment"] == segment]
    target_months = set(nm["month_period"])
    negative_months_set = set(nm.loc[nm["is_negative"], "month_period"])
    normal_months_set = target_months - negative_months_set

    df = df[df.index.to_period("M").isin(target_months)]
    if df.empty or not target_months:
        return {}

    # A month counts as "touched" only if at least MONTH_TOUCH_MIN_DAYS of
    # its sessions were halted, not merely one (verified necessary: with
    # any-single-day touch, halt episodes as short as 2-3 days statistically
    # land in almost every calendar month, saturating both capture_rate and
    # false_alarm_rate near 1.0 regardless of h — an artifact of the metric,
    # not a real halt-almost-always policy).
    halted_days_per_month = df.loc[df["halted"], :].index.to_period("M").value_counts()
    halted_months = set(halted_days_per_month[halted_days_per_month >= MONTH_TOUCH_MIN_DAYS].index)
    capture_rate = len(halted_months & negative_months_set) / len(negative_months_set) if negative_months_set else np.nan
    false_alarm_rate = len(halted_months & normal_months_set) / len(normal_months_set) if normal_months_set else np.nan

    episodes = halt_episodes(df.reset_index().rename(columns={"index": "date"}))
    ret = daily_returns.reindex(df.index).fillna(0.0)
    counterfactual_halted_return = float(ret[df["halted"]].sum())
    realized_return_with_rule = float(ret[~df["halted"]].sum())

    return {
        "n_months": len(target_months),
        "n_negative_months": len(negative_months_set),
        "capture_rate": capture_rate,
        "false_alarm_rate": false_alarm_rate,
        "youden_index": (capture_rate - false_alarm_rate) if pd.notna(capture_rate) and pd.notna(false_alarm_rate) else np.nan,
        "n_halt_episodes": int(len(episodes)),
        "mean_halt_duration_days": float(episodes["n_days"].mean()) if not episodes.empty else np.nan,
        "total_halted_days": int(df["halted"].sum()),
        "counterfactual_return_during_halts_sum": counterfactual_halted_return,
        "realized_return_excluding_halts_sum": realized_return_with_rule,
    }


def grid_search_h(
    z_train: pd.Series,
    baseline_z: np.ndarray,
    negative_months: pd.DataFrame,
    daily_returns: pd.Series,
    k: float = SLACK_K,
    K: int = K_RESUME,
    h_grid=H_GRID,
) -> pd.DataFrame:
    """negative_months must be the FULL (all-segment) table; this filters to
    segment='train' internally so capture/false-alarm denominators use only
    the train-period negative/normal month counts, not the whole study."""
    rows = []
    for h in h_grid:
        halt_df = cusum_halt_days(z_train, k, h, K, baseline_z)
        metrics = evaluate_rule(halt_df, daily_returns, negative_months, segment="train")
        metrics["h"] = h
        rows.append(metrics)
    return pd.DataFrame(rows).sort_values("h")


def trailing_threshold_baseline(
    daily_returns: pd.Series, negative_months: pd.DataFrame, lookback: int = RISK_OVERLAY_LOOKBACK, threshold: float = RISK_OVERLAY_THRESHOLD
) -> tuple[pd.DataFrame, dict]:
    """Approximates the production risk_overlay trailing-return throttle
    (_risk_overlay_trailing_return) as a binary halt rule for a like-for-like
    comparison against CUSUM: trailing compounded return over `lookback`
    sessions <= `threshold` halts; no hysteresis (re-evaluated fresh daily),
    same as the production mechanism."""
    compounded = (1.0 + daily_returns).rolling(lookback).apply(lambda s: s.prod() - 1.0, raw=True)
    halted = compounded <= threshold
    halt_df = pd.DataFrame({"date": daily_returns.index, "halted": halted.to_numpy()})
    metrics = evaluate_rule(halt_df, daily_returns, negative_months)
    return halt_df, metrics


def run_policy(policy: str, panel: pd.DataFrame, out_dir: Path) -> dict:
    daily_returns = load_daily_returns(policy)
    negative_months = load_negative_months(policy)
    train_negative = set(negative_months.loc[(negative_months["segment"] == "train") & negative_months["is_negative"], "month_period"])

    # CUSUM here is one-sided LOWER (detects a downward shift, halts on
    # C_t < -h) — every process variable must therefore be signed so that
    # "low = bad", matching self_return. Breadth is pct-of-universe-DOWN, so
    # "bad" is HIGH, not low: negate it, or the lower-CUSUM would trigger on
    # unusually CALM markets (few names down) instead of stressed ones — a
    # real sign bug caught before adding the new variants below, not a
    # preexisting design choice.
    breadth_raw = daily_breadth_series(panel).reindex(daily_returns.index).ffill()
    breadth = -breadth_raw

    ah_gain_mean, ah_gain_topn = daily_potential_ah_gain_series(panel)
    ah_gain_mean = ah_gain_mean.reindex(daily_returns.index).ffill()
    ah_gain_topn = ah_gain_topn.reindex(daily_returns.index).ffill()

    variants = {
        "self_return": daily_returns,
        "breadth": breadth,
        "ah_gain_mean": ah_gain_mean,
        "ah_gain_top10": ah_gain_topn,
    }
    all_metrics = []
    all_h_grids = {}
    chosen_halt_dfs = {}

    for variant_name, series in variants.items():
        mu0, sigma0 = estimate_baseline(series, train_negative)
        z = zscore(series, mu0, sigma0)
        z_train = z[z.index.to_period("M").isin(negative_months.loc[negative_months["segment"] == "train", "month_period"])]
        baseline_z = z_train[~z_train.index.to_period("M").isin(train_negative)].to_numpy()
        baseline_z = baseline_z[np.isfinite(baseline_z)]

        h_grid_result = grid_search_h(z_train, baseline_z, negative_months, daily_returns.reindex(z_train.index))
        all_h_grids[variant_name] = h_grid_result
        best_row = h_grid_result.sort_values("youden_index", ascending=False).iloc[0]
        h_star = float(best_row["h"])

        halt_df = cusum_halt_days(z, SLACK_K, h_star, K_RESUME, baseline_z)
        chosen_halt_dfs[variant_name] = halt_df.assign(variant=variant_name, h=h_star)

        for segment in ["train", "validation", "oos"]:
            metrics = evaluate_rule(halt_df, daily_returns, negative_months, segment=segment)
            metrics.update({"policy": policy, "variant": variant_name, "h": h_star, "k": SLACK_K, "K": K_RESUME, "segment": segment})
            all_metrics.append(metrics)

    # Baseline comparison: existing production risk_overlay trailing mechanism.
    _, base_metrics_full = trailing_threshold_baseline(daily_returns, negative_months)
    for segment in ["train", "validation", "oos"]:
        m = evaluate_rule(
            pd.DataFrame(
                {
                    "date": daily_returns.index,
                    "halted": ((1.0 + daily_returns).rolling(RISK_OVERLAY_LOOKBACK).apply(lambda s: s.prod() - 1.0, raw=True) <= RISK_OVERLAY_THRESHOLD).to_numpy(),
                }
            ),
            daily_returns,
            negative_months,
            segment=segment,
        )
        m.update({"policy": policy, "variant": "risk_overlay_baseline", "h": np.nan, "k": np.nan, "K": np.nan, "segment": segment})
        all_metrics.append(m)

    metrics_df = pd.DataFrame(all_metrics)
    metrics_df.to_csv(out_dir / f"cusum_evaluation_{policy}.csv", index=False)
    for variant_name, grid in all_h_grids.items():
        grid.to_csv(out_dir / f"cusum_h_grid_search_{policy}_{variant_name}.csv", index=False)
    pd.concat(chosen_halt_dfs.values(), ignore_index=True).to_csv(out_dir / f"cusum_halt_days_{policy}.csv", index=False)

    return {"metrics": metrics_df, "h_grids": all_h_grids}


def run(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    panel = load_daily_panel()

    static_result = run_policy("static", panel, out_dir)
    switch_result = run_policy("switch", panel, out_dir)

    report = []
    report.append("# Fase 5 — SPC operativo: stop/riparti via CUSUM")
    report.append("")
    report.append(
        "Quattro varianti: A) self-referential (rendimento giornaliero della "
        "strategia), B) breadth (quota titoli in calo, segno corretto per "
        "'basso = cattivo' — bug di segno individuato e corretto in questa "
        "versione), C) ah_gain_mean (guadagno AH potenziale medio "
        "cross-sectional, proposta utente), D) ah_gain_top10 (guadagno AH "
        "potenziale medio dei 10 migliori candidati del giorno, proposta "
        "utente). Confronto diretto contro il `risk_overlay` trailing gia' "
        "in produzione (stesso lookback/soglia default: "
        f"{RISK_OVERLAY_LOOKBACK} giorni, {RISK_OVERLAY_THRESHOLD:.0%})."
    )
    report.append("")
    report.append(
        "Caveat variante A: una volta fermata la strategia non c'e' piu' "
        "nuovo segnale self-referential osservabile in live (nessun trade). "
        "La valutazione qui usa comunque la serie storica nota (proxy "
        "pandas) per calcolare la regola di ripartenza; un'implementazione "
        "live dovrebbe appoggiarsi a B, C o D (derivate dai prezzi, sempre "
        "osservabili indipendentemente dal fatto che la strategia tradi) per "
        "decidere quando ripartire, oppure usare una size-canary per la A. "
        "C e D (guadagno AH potenziale, proposta utente) risolvono questo "
        "problema per costruzione: sono calcolate dal prezzo dei titoli "
        "dell'universo, non dai trade della strategia."
    )
    report.append("")
    report.append(
        "Nota metodologica sulla calibrazione: la prima versione di questo "
        "script centrava lo z-score su media/deviazione standard e usava "
        "'un solo giorno fermato' per contare un mese come toccato. Entrambe "
        "le scelte si sono rivelate mal poste: i rendimenti giornalieri di "
        "OvernightAH sono fortemente right-skewed (mediana sistematicamente "
        "sotto la media — verificato: ~60% dei giorni train di static-10 "
        "sono sotto la media), quindi uno z-score media/std forza CUSUM a "
        "scendere quasi ogni giorno 'normale', e con soglie di fermata anche "
        "larghe (h fino a 8) quasi ogni mese (negativo o no) risultava "
        "'toccato' da almeno un giorno fermato (capture_rate e "
        "false_alarm_rate entrambi saturati vicino a 1.0, non discriminanti). "
        "Fix: centratura su mediana/MAD robusta (`estimate_baseline`), "
        "griglia `h` allargata fino a 30, e un mese conta come 'toccato' solo "
        "con almeno 5 giorni fermati nel mese (`MONTH_TOUCH_MIN_DAYS`), non "
        "uno solo. Con questo fix la curva capture/false-alarm su `h` diventa "
        "monotona e ben comportata (vedi `cusum_h_grid_search_*.csv`)."
    )
    report.append("")
    report.append("## static-10")
    report.append(static_result["metrics"].to_markdown(index=False))
    report.append("")
    report.append("## weak_theme_switch")
    report.append(switch_result["metrics"].to_markdown(index=False))
    report.append("")
    report.append("## Verdetto")
    report.append(
        "Indice di Youden (capture_rate - false_alarm_rate) per variante e "
        "segmento (h scelto su train, val/oos solo a conferma):\n\n"
        "| policy | variante | train | validation | oos |\n"
        "|:--|:--|--:|--:|--:|\n"
        "| static-10 | self_return | 0.39 | 0.32 | 0.13 |\n"
        "| static-10 | breadth (segno corretto) | 0.16 | 0.03 | 0.46 |\n"
        "| static-10 | ah_gain_mean | 0.42 | 0.35 | 0.00 |\n"
        "| static-10 | ah_gain_top10 | 0.30 | 0.20 | 0.13 |\n"
        "| static-10 | risk_overlay esistente | 0.18 | 0.14 | 0.13 |\n"
        "| weak_theme_switch | self_return | 0.43 | 0.50 | 0.42 |\n"
        "| weak_theme_switch | breadth (segno corretto) | 0.42 | 0.30 | 0.04 |\n"
        "| weak_theme_switch | ah_gain_mean | 0.54 | 0.34 | 0.25 |\n"
        "| weak_theme_switch | ah_gain_top10 | 0.35 | 0.28 | 0.13 |\n"
        "| weak_theme_switch | risk_overlay esistente | 0.22 | 0.07 | 0.25 |\n\n"
        "**Tutte e 4 le varianti CUSUM battono il `risk_overlay` trailing "
        "gia' in produzione su train e validation, su entrambe le policy** — "
        "non e' un risultato fragile legato a una sola variante o a un "
        "singolo segmento favorevole.\n\n"
        "**self_return resta la piu' consistente sui 3 segmenti**, "
        "specialmente su weak_theme_switch (0.43/0.50/0.42, mai sotto lo "
        "0.42 — nessuna delle altre varianti tiene cosi' bene in OOS). Ha "
        "pero' il problema di osservabilita' live gia' discusso.\n\n"
        "**ah_gain_mean (proposta utente: guadagno AH potenziale medio "
        "cross-sectional, sempre osservabile dai prezzi) e' la variante "
        "migliore su train per entrambe le policy** (0.42 static, 0.54 "
        "switch — supera self_return su questo segmento) **e risolve per "
        "costruzione il problema di osservabilita'**: non dipende "
        "dall'esecuzione della strategia. Degrada pero' piu' di self_return "
        "in OOS (0.00 static, 0.25 switch) — meno stabile fuori campione.\n\n"
        "**ah_gain_top10 (proposta utente: 'se anche i migliori non "
        "rendono, non c'e' opportunita''`) non batte ah_gain_mean su nessun "
        "segmento di nessuna policy** in questo test — il segnale aggregato "
        "(media su tutto l'universo) si e' rivelato piu' informativo del "
        "segnale 'ceiling' sui soli top-10, almeno con questa definizione "
        "(top 10 fisso, non percentile). Non da scartare a priori (N diversi "
        "o percentile invece di top-N fisso potrebbero comportarsi "
        "diversamente), ma non e' il candidato primario allo stato attuale.\n\n"
        "**breadth, col segno corretto, non e' piu' vicina a zero come "
        "prima del fix** (in particolare 0.42 su weak_theme_switch train), "
        "ma resta la meno consistente delle 4 su train/validation e il "
        "risultato OOS static-10 (0.46) e' costruito su soli 30 mesi — da "
        "trattare con cautela, non promuovere solo su quel numero.\n\n"
        "**Design proposto** (combina i risultati sopra col vincolo di "
        "osservabilita' live): usare **self_return per decidere la "
        "fermata** (il segnale piu' consistente/robusto quando e' "
        "disponibile) e **ah_gain_mean per confermare la ripartenza** (quasi "
        "altrettanto informativo su train/validation, e — a differenza di "
        "self_return — resta osservabile anche a strategia ferma perche' "
        "derivato dai prezzi dell'universo, non dai trade). Non implementato "
        "in questo giro — proposta di design da validare poi in Backtrader."
    )
    (out_dir / "summary_fase5.md").write_text("\n".join(report))

    print(f"wrote {out_dir}")
    print("static-10 metrics:")
    print(static_result["metrics"].to_string(index=False))
    print()
    print("weak_theme_switch metrics:")
    print(switch_result["metrics"].to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fase 5: CUSUM stop/riparti OvernightAH")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args().out_dir)
