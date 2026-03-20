# Guida: Script 05-filtered_strategy_analysis.py

## Scopo

Applica il **filtro Triple Magnitudine** (migliore identificato nell'analisi) ai dati completi e calcola metriche dettagliate per valutare l'efficacia.

## Dove si Inserisce nel Workflow

```
01-fast_backtest.py          → Calcola slope, r2, disp, forward outcomes
02-feature_diagnostics.py    → Analizza correlazioni e slice
03-slice_analisys.py         → Analizza slice specifiche con filtri AND
04-entry_exit_analysis.py    → Analizza timing TP/SL (con filtri fissi)

→ 05-filtered_strategy_analysis.py  ← NUOVO! Test filtro triple magnitudine
```

## Input

**Dati richiesti**: File `*_feature_outcome_full.csv` (output di `01-fast_backtest.py`)

**Colonne necessarie**:
- `slope_5`, `slope_20`, `slope_60`
- `high_ret_max_5m`, `low_ret_min_5m`, `close_ret_5m` (e per altri orizzonti)
- `volZ` (opzionale, per filtro addizionale)
- `r2_60`, `disp_60`, `mse_60`, ecc. (opzionali, per fit_quality)

## Output

### File generati in `results/filtered_strategy/`:

1. **filter_metadata.csv**
   - Parametri del filtro applicato
   - Soglie calcolate (percentili)
   - N righe input/filtered
   - % righe mantenute

2. **metrics_filtered.csv**
   - Metriche per ogni orizzonte (5m, 10m, 15m)
   - P(hit TP), P(hit SL), P(hit neither)
   - Mean/median high, low, close

3. **metrics_baseline.csv**
   - Stesse metriche ma SENZA filtro (per confronto)

4. **Grafici PNG**:
   - `p_hit_tp_comparison.png` - Probabilità hit TP (filtered vs baseline)
   - `p_hit_sl_comparison.png` - Probabilità hit SL
   - `mean_high_comparison.png` - Mean high return
   - `lift_comparison.png` - Lift (ratio filtered/baseline)

## Utilizzo Base

### Esecuzione con parametri default (p75 su tutti gli slope):

```bash
cd bin/binomial_regression
source ../../backtrader/bin/activate
python 05-filtered_strategy_analysis.py
```

**Risultato**: Applica filtro `abs(slope_5/20/60) > p75`

---

## Opzioni Avanzate

### 1. Cambia Percentili (Selettività)

```bash
# Più selettivo (solo top 10%)
python 05-filtered_strategy_analysis.py \
    --slope5-percentile 90 \
    --slope20-percentile 90 \
    --slope60-percentile 90

# Meno selettivo (top 50%)
python 05-filtered_strategy_analysis.py \
    --slope5-percentile 50 \
    --slope20-percentile 50 \
    --slope60-percentile 50
```

**Trade-off**:
- p90 → Meno candele (più selettivo), ma win rate più alto
- p50 → Più candele (più opportunità), ma win rate più basso

---

### 2. Aggiungi Trend Alignment

```bash
python 05-filtered_strategy_analysis.py \
    --slope5-percentile 75 \
    --slope20-percentile 75 \
    --slope60-percentile 75 \
    --use-alignment
```

**Effetto**: Filtra solo candele dove `sign(slope_5) == sign(slope_20) == sign(slope_60)`

---

### 3. Aggiungi Filtro VolZ

```bash
python 05-filtered_strategy_analysis.py \
    --slope5-percentile 75 \
    --slope20-percentile 75 \
    --slope60-percentile 75 \
    --volz-min 0.85
```

**Effetto**: Aggiunge condizione `volZ > 0.85` (volume sopra media)

---

### 4. Aggiungi Filtro Fit Quality

```bash
python 05-filtered_strategy_analysis.py \
    --slope5-percentile 75 \
    --slope20-percentile 75 \
    --slope60-percentile 75 \
    --quality-min 0.5 \
    --quality-window 60
```

**Effetto**: Aggiunge condizione `fit_quality_60 >= 0.5` (top 50% fit quality)

---

### 5. Combinazione Completa (Massima Selettività)

```bash
python 05-filtered_strategy_analysis.py \
    --slope5-percentile 90 \
    --slope20-percentile 90 \
    --slope60-percentile 90 \
    --use-alignment \
    --volz-min 0.85 \
    --quality-min 0.7 \
    --quality-window 60 \
    --tp-pct 0.005 \
    --sl-pct 0.003
```

**Risultato**: Setup ULTRA selettivo (probabilmente < 1% delle candele)

---

### 6. Cambia TP/SL

```bash
# TP più vicino, SL più stretto
python 05-filtered_strategy_analysis.py \
    --tp-pct 0.003 \
    --sl-pct 0.002

# TP più lontano, SL più ampio
python 05-filtered_strategy_analysis.py \
    --tp-pct 0.01 \
    --sl-pct 0.005
```

---

### 7. Cambia Orizzonti di Analisi

```bash
# Solo orizzonti brevi
python 05-filtered_strategy_analysis.py \
    --horizons 3,5,10

# Include orizzonti lunghi
python 05-filtered_strategy_analysis.py \
    --horizons 5,10,15,30,60
```

---

## Interpretazione Output

### Esempio Output Log:

```
================================================================================
REPORT CONFRONTO
================================================================================

Filtro applicato: abs(slope_5)>p75 + abs(slope_20)>p75 + abs(slope_60)>p75

Righe:
  Input:    98370
  Filtered: 5933 (6.0%)

Metriche @ 5 minuti:
  P(hit TP):
    Baseline: 33.9%
    Filtered: 64.8%     ← 🎯 QUASI DOPPIO!
    Lift:     1.91x

  Mean High:
    Baseline: 0.108%
    Filtered: 0.258%    ← 🚀 PIÙ CHE DOPPIO!
    Lift:     2.38x

  P(hit SL):
    Baseline: 39.3%
    Filtered: 42.1%     ← Leggermente più alto (normale)
```

### Cosa Guardare:

✅ **Lift > 1.5x** → Filtro efficace
✅ **% kept 5-15%** → Buon bilanciamento selettività/opportunità
✅ **P(hit TP) > 50%** → Setup profittevole
⚠️ **% kept < 2%** → Troppo selettivo, poche opportunità
⚠️ **Lift < 1.2x** → Filtro debole, non vale il trade-off

---

## Grafici Generati

### 1. p_hit_tp_comparison.png
Mostra probabilità di raggiungere TP per ogni orizzonte (5m, 10m, 15m).
- **Linea grigia**: Baseline (nessun filtro)
- **Linea verde**: Filtered (con filtro applicato)

**Interpretazione**: La linea verde dovrebbe essere SOPRA quella grigia.

---

### 2. lift_comparison.png
Mostra il lift (ratio filtered/baseline) per:
- P(hit TP)
- Mean high

**Interpretazione**:
- Lift > 1.0 → Filtro migliora
- Lift = 1.0 → Filtro neutro
- Lift < 1.0 → Filtro peggiora (non usare!)

---

## Workflow Consigliato

### Step 1: Test Base (p75)
```bash
python 05-filtered_strategy_analysis.py
```
→ Controlla lift @ 5 minuti

### Step 2: Testa Selettività (p90 vs p75 vs p50)
```bash
# p90 (top 10%)
python 05-filtered_strategy_analysis.py \
    --slope60-percentile 90 \
    --output-dir results/filtered_p90

# p50 (top 50%)
python 05-filtered_strategy_analysis.py \
    --slope60-percentile 50 \
    --output-dir results/filtered_p50
```
→ Confronta trade-off opportunità vs qualità

### Step 3: Aggiungi VolZ se Serve
```bash
python 05-filtered_strategy_analysis.py \
    --volz-min 0.85 \
    --output-dir results/filtered_p75_volz
```
→ Verifica se migliora ulteriormente

### Step 4: Valuta Setup Finale
Scegli il setup con:
- **Lift >= 1.5x** (preferibilmente 1.8x+)
- **% kept >= 5%** (almeno qualche opportunità)
- **P(hit TP) >= 50%** (win rate accettabile)

---

## Confronto con 04-entry_exit_analysis.py

| Feature | 04-entry_exit_analysis.py | 05-filtered_strategy_analysis.py |
|---------|---------------------------|----------------------------------|
| Filtri applicati | Fissi (slope_min hardcoded) | Configurabili (percentili) |
| Calcola timing preciso | ✅ Sì (barre esatte a TP/SL) | ❌ No |
| Calcola metriche aggregate | Parziale | ✅ Completo |
| Confronto vs baseline | ❌ No | ✅ Sì |
| Grafici comparativi | Basici | ✅ Avanzati (lift) |
| Configurabilità filtri | Bassa | ✅ Alta |

**Quando usare 04**: Se vuoi timing PRECISO (quante barre a TP/SL)
**Quando usare 05**: Se vuoi TESTARE filtri diversi e confrontare

---

## Esempi di Test

### Test 1: Solo Magnitudine
```bash
python 05-filtered_strategy_analysis.py \
    --slope5-percentile 75 \
    --slope20-percentile 75 \
    --slope60-percentile 75 \
    --output-dir results/test_mag_only
```

### Test 2: Magnitudine + Alignment
```bash
python 05-filtered_strategy_analysis.py \
    --slope5-percentile 75 \
    --slope20-percentile 75 \
    --slope60-percentile 75 \
    --use-alignment \
    --output-dir results/test_mag_align
```

### Test 3: Magnitudine + VolZ
```bash
python 05-filtered_strategy_analysis.py \
    --slope5-percentile 75 \
    --slope20-percentile 75 \
    --slope60-percentile 75 \
    --volz-min 0.85 \
    --output-dir results/test_mag_volz
```

Poi confronta i risultati in:
- `results/test_mag_only/metrics_filtered.csv`
- `results/test_mag_align/metrics_filtered.csv`
- `results/test_mag_volz/metrics_filtered.csv`

---

## FAQ

**Q: Quale percentile usare?**
A: Parti da p75. Se hai poche opportunità, scendi a p50. Se hai troppe, sali a p90.

**Q: Vale la pena aggiungere alignment?**
A: Dai test precedenti, NO. Magnitudine da sola è più efficace.

**Q: E se volessi testare altri filtri?**
A: Modifica lo script o crea una versione custom. Il codice è modulare.

**Q: Come decido tra filtri diversi?**
A: Guarda il lift @ 5 minuti. Scegli quello con lift >= 1.5x e % kept >= 5%.

---

## Prossimi Passi

Dopo aver identificato il filtro ottimale con questo script:

1. **Implementa in backtest reale** (backtrader)
2. **Testa in paper trading**
3. **Valuta slippage e commissioni**
4. **Monitora in produzione**

Il filtro `abs(slope_5/20/60) > p75` è un ottimo punto di partenza! 🚀
