# RTH EDA Notebook — Design Spec
Date: 2026-05-27

## Scopo
Notebook Jupyter per analisi esplorativa (EDA) del dataset RTH-only prodotto dalla pipeline `bt-strategy-test/RTH_analysis/`. Serve a capire le feature prima di Step 2 (selezione feature, mutual information).

## File prodotto
```
bt-strategy-test/RTH_analysis/04_eda.ipynb
```

## Dipendenze
- `notebook` (da installare nel venv: `pip install notebook`)
- `pandas`, `numpy`, `matplotlib`, `seaborn` (già disponibili)

## Input
`bt-strategy-test/RTH_analysis/out/rth_universe_map.parquet` — 142.999 righe, 101 colonne, 90 simboli, 2020–2026

## Struttura notebook

### Sezione 0 — Setup
- Import e caricamento dati
- Costanti: `WINDOWS = [5, 10, 20, 50, 63, 100]`, `FAMILIES` (dict famiglia → colonne)
- Soglie correlazione: `CORR_HIGH = 0.90`, `CORR_EXTREME = 0.95`
- Funzioni helper riutilizzabili (definite qui, usate nelle sezioni successive)

### Sezione 1 — Panoramica dataset
- Shape, simboli, date range, numero feature
- Bar chart: NaN ratio per colonna (80 feature, ordinate per % NaN)
- Tabella: min/max NaN ratio per famiglia

### Sezione 2 — Distribuzioni per famiglia
- 14 sottosezioni indipendenti (una per famiglia RTH)
- Funzione `plot_family(df, cols, title)` riutilizzabile
- Per ogni famiglia: histplot + boxplot (finestra N=20 dove applicabile)
- Tabella sotto ogni grafico: `mean / std / skew / kurtosis`
- Famiglie: mom, mean, ewm, drift_accel, std, mom_norm, eff, signed_eff, winrate, pressure, close_to_range, range_exp, volume, rs, overnight

### Sezione 3 — Stabilità temporale
- Feature principale per famiglia (N=20), media ± std per anno (2020–2026)
- Lineplot per ogni famiglia con banda di confidenza
- Heatmap: anno × famiglia → media normalizzata (z-score per riga)

### Sezione 4 — Correlazione pooled con outlier detection
- Matrice di correlazione 80×80 su tutti i simboli × date (pooled, dopo dropna)
- Seaborn clustermap con clustering gerarchico
- Tabella outlier: coppie con `|corr| > CORR_HIGH`, ordinate per |corr| decrescente
- Coppie con `|corr| > CORR_EXTREME` evidenziate in rosso

## Cosa NON è incluso
- Confronto RTH vs close-close
- Correlazione cross-symbol (tra simboli diversi)
- Selezione soglie operative (Step 2)
- Mutual information (Step 2)

## Note implementative
- Ogni sezione ha la sua cella di esecuzione indipendente (nessuna dipendenza nascosta tra sezioni, a parte il DataFrame caricato in Sezione 0)
- Grafici: dimensione fissa `figsize` per leggibilità, `dpi=100`
- Clustermap correlazione: `figsize=(20, 18)` per visualizzare tutte le label
- Notebook numerato coerentemente con gli script: `04_eda.ipynb`
