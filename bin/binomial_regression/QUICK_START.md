# Quick Start - Diagnostica Strategia Regressione

## Problema Identificato

Le tue analisi mostrano **correlazioni inverse anomale**:
- **slope positivo** → correlazione **NEGATIVA** con high_ret_max (-0.030)
- Le **slice con slope negativo** performano **MEGLIO**

Questo è un **red flag critico** che indica possibili bug o problemi concettuali.

## Soluzione: Test Suite Automatizzata

Ho creato una suite di test diagnostici per identificare e risolvere il problema.

## Esecuzione Rapida

```bash
cd bin/binomial_regression
./run_diagnostics.sh
```

Questo script eseguirà automaticamente:
1. ✅ Verifica calcolo slope (test su 20 candele random)
2. ✅ Verifica forward outcomes (test look-ahead bias)
3. ✅ Analisi timing hit TP/SL (quante posizioni si chiudono entro N minuti)

## Cosa Aspettarsi

### Se tutto OK:
```
✅ Slope verification: PASS
✅ Forward outcomes verification: PASS
✅ Analisi timing completata

METRICHE CHIAVE:
  Mediana barre a TP:          X.X
  P(no close entro 6m):        XX.X%
```

### Se ci sono bug:
```
❌ Slope verification: FAIL
   - Logic errors: 20
   AZIONE RICHIESTA: Controlla logs/verify_slope.log
   IMPORTANTE: RICHIEDERÀ RICALCOLO COMPLETO
```

## Interpretazione Risultati

### 1. Test Slope
- **PASS**: Il calcolo è corretto, il problema è altrove
- **FAIL**: Bug nel calcolo → Correggi `01-fast_backtest.py`

### 2. Test Forward Outcomes
- **PASS**: Nessun look-ahead bias
- **WARNING**: Possibile inclusione barra corrente
- **FAIL**: Bug nel calcolo → Correggi `01-fast_backtest.py`

### 3. Analisi Timing
- **P(no close entro 6m) < 30%**: Setup funzionante ✅
- **P(no close entro 6m) 30-50%**: Setup migliorabile ⚠️
- **P(no close entro 6m) > 50%**: Setup inefficace ❌

## Azioni in Base ai Risultati

### Scenario A: Bug trovato (slope o outcomes)
1. Leggi il log in `logs/verify_*.log`
2. Identifica la riga di codice problematica
3. Correggi in `01-fast_backtest.py`
4. Ricalcola tutto:
   ```bash
   python 01-fast_backtest.py
   python 02-feature_diagnostics.py
   python 03-slice_analisys.py
   ```
5. Riprova: `./run_diagnostics.sh`

### Scenario B: Nessun bug, ma troppo poche chiusure entro 6m
1. Riduci TP (da 0.5% a 0.3%) e SL (da 0.3% a 0.2%)
2. Aggiungi filtro su volatilità realizzata (vedi README_DIAGNOSTICS.md)
3. Restringi a slot orari ad alta volatilità (primi 30-60 min dopo open)
4. Aumenta `quality_min` da 0.5 a 0.7 (solo top 30% setup)

### Scenario C: Nessun bug, correlazioni ancora negative
**Ipotesi**: Su TF 1m il mercato è dominato da **mean reversion**, non trend following.

Opzioni:
1. **Inverti la strategia**: entra su slope **negativo** invece che positivo
2. **Cambia TF**: passa a 5m o 15m dove il trend è più persistente
3. **Cambia approccio**: usa mean reversion invece di trend following

## File Creati

### Script Diagnostici:
- `verify_slope_calculation.py` - Test calcolo slope
- `verify_forward_outcomes.py` - Test look-ahead bias
- `analyze_hit_timing.py` - Analisi tempi hit TP/SL
- `run_diagnostics.sh` - Script automatizzato completo

### Documentazione:
- `DIAGNOSTIC_PLAN.md` - Piano completo (6 fasi)
- `README_DIAGNOSTICS.md` - Guida dettagliata
- `QUICK_START.md` - Questo file

## Output Files

Dopo l'esecuzione di `run_diagnostics.sh`:

```
bin/binomial_regression/
├── logs/
│   ├── verify_slope.log         ← Test calcolo slope
│   ├── verify_outcomes.log      ← Test forward outcomes
│   └── hit_timing.log           ← Log analisi timing
└── results/
    └── hit_timing_analysis/
        ├── analysis_meta.csv              ← Parametri usati
        ├── hit_timing_row_level.csv       ← Timing per ogni entry
        ├── hit_timing_summary_by_horizon.csv  ← Metriche aggregate
        ├── hit_timing_global_stats.csv    ← Statistiche globali
        ├── hit_prob_by_horizon.png        ← Grafico probabilità
        ├── distribution_bars_to_tp.png    ← Distribuzione tempo TP
        ├── distribution_bars_to_sl.png    ← Distribuzione tempo SL
        └── distribution_first_hit_type.png ← Tipo primo hit
```

## Parametri Personalizzabili

Se vuoi modificare i parametri di test, edita `run_diagnostics.sh` alla sezione:

```bash
python analyze_hit_timing.py \
    --tp-pct 0.005 \          # Take profit %
    --sl-pct 0.003 \          # Stop loss %
    --horizons 3,5,6,10,15,30,60 \
    --slope5-min 0.0004 \     # Filtro slope_5
    --slope20-min 0.0002 \    # Filtro slope_20
    --slope60-min 0.00015 \   # Filtro slope_60
    --volz-min 0.85 \         # Filtro volZ
    --quality-min 0.5         # Filtro fit_quality
```

## Domande Frequenti

**Q: Quanto tempo richiede l'esecuzione?**
A: 2-5 minuti per tutti i test (dipende dal numero di asset)

**Q: Devo fermarmi se trovo un bug?**
A: SÌ. Lo script si ferma automaticamente se trova errori critici.

**Q: Posso eseguire solo un singolo test?**
A: Sì, esegui direttamente lo script Python:
```bash
python verify_slope_calculation.py
```

**Q: Cosa faccio se il problema persiste dopo i fix?**
A: Leggi `README_DIAGNOSTICS.md` sezione "Fase 2.2" e "Fase 2.3" per filtri addizionali.

## Supporto

Per dettagli tecnici completi, vedi:
- `DIAGNOSTIC_PLAN.md` - Piano strategico 6 fasi
- `README_DIAGNOSTICS.md` - Guida operativa completa

## Prossimi Sviluppi (TODO)

Gli script seguenti sono descritti in `DIAGNOSTIC_PLAN.md` ma NON ancora implementati:
- `add_volatility_filters.py` - Filtri su volatilità realizzata
- `analyze_time_context.py` - Analisi slot orari
- `optimize_quality_vol_filter.py` - Grid search combinazioni filtri

Implementali se necessario seguendo i template nei file di documentazione.

---

**IMPORTANTE**: Esegui SEMPRE i test di verifica (Fase 1) prima di procedere con ottimizzazioni.
Un bug nel calcolo invalida tutte le analisi successive.
