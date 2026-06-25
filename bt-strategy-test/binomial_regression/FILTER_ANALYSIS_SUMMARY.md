# Analisi Completa Filtri: Sintesi e Raccomandazioni

## Executive Summary

Abbiamo testato diversi filtri per selezionare le migliori opportunità di trading su timeframe 1m. I risultati mostrano che:

1. **Filtro Magnitudine** (abs di slope) è il più efficace
2. **Filtri Qualità** (disp/rmse/r2) sono complementari al filtro magnitudine
3. La combinazione ottimale dipende dal trade-off opportunità vs qualità

---

## Confronto Prestazioni

### 1. Baseline (Nessun Filtro)

```
Candele:      430,522 (100%)
P(hit TP):    4.3%
Mean high:    0.134%
```

**Caratteristiche:**
- Massime opportunità
- Win rate molto basso
- Non utilizzabile per trading reale

---

### 2. Filtro Triple Magnitudine (Script 05)

**Condizione:** `abs(slope_5) > p75 AND abs(slope_20) > p75 AND abs(slope_60) > p75`

```
Candele:      30,445 (7.1% del totale)
P(hit TP):    18.6%
Mean high:    0.318%
Lift P(TP):   4.30x
Lift High:    2.38x
```

**Caratteristiche:**
- ✅ Lift eccellente (4.3x)
- ✅ Buon bilanciamento opportunità/qualità
- ✅ Win rate accettabile (18.6%)
- ⚠️ Cattura ~40-50 opportunità al giorno (su dataset multi-asset)

**Interpretazione:** Seleziona candele con forte movimento su tutte le finestre temporali (volatilità persistente)

---

### 3. Filtri Qualità Standalone (Script 06, senza magnitudine)

#### 3a. Filtro Dispersione/RMSE

**Condizione:** `disp_60 > p75` (equivalente a `rmse_60 > p75`)

```
Candele:      107,586 (25.0% del totale)
P(hit TP):    11.9%
Mean high:    0.247%
Lift P(TP):   2.74x
Lift High:    1.85x
```

**Caratteristiche:**
- ✅ Molte opportunità (25% dei dati)
- ✅ Lift discreto (2.74x)
- ⚠️ Lift inferiore a magnitudine (2.74x vs 4.30x)

**Interpretazione:** Seleziona candele con alta dispersione residui = alta volatilità locale

#### 3b. Filtro R² BASSO (< p25)

```
Candele:      107,586 (25.0% del totale)
P(hit TP):    3.9%
Mean high:    0.126%
Lift P(TP):   0.89x ❌
Lift High:    0.94x ❌
```

**Caratteristiche:**
- ❌ PEGGIORA le prestazioni
- ❌ Non utilizzabile da solo

**Interpretazione:** Basso r2 può significare noise/rumore, non necessariamente volatilità utile

---

### 4. Filtri Combinati: Magnitudine + Qualità (Script 06, con --use-mag-filter)

#### 4a. Magnitudine + Dispersione

**Condizione:**
```
abs(slope_5/20/60) > p75  (filtro magnitudine)
AND
disp_60 > p75  (filtro qualità)
```

```
Candele:      7,611 (1.8% del totale, 25% del filtrato magnitudine)
P(hit TP):    33.8%
Mean high:    0.478%

Lift vs baseline:        7.86x
Lift vs mag-only:        1.81x
```

**Caratteristiche:**
- ✅ Win rate MOLTO alto (33.8%)
- ✅ Mean high eccellente (0.478%)
- ⚠️ Poche opportunità (1.8% dei dati)
- ⚠️ ~10-15 setup al giorno su dataset multi-asset

**Interpretazione:** Seleziona solo le opportunità con:
1. Forte trend multi-timeframe (magnitudine)
2. Alta volatilità locale (dispersione)

#### 4b. Magnitudine + Dispersione + R² basso

**Condizione:**
```
abs(slope_5/20/60) > p75
AND disp_60 > p75
AND r2_60 < p25
```

```
Candele:      3,415 (0.8% del totale)
P(hit TP):    33.4%
Mean high:    0.474%

Lift vs baseline:        7.76x
Lift vs mag-only:        1.79x
```

**Caratteristiche:**
- ✅ Win rate simile a mag+disp (33.4%)
- ⚠️ DIMEZZA le opportunità (da 7,611 a 3,415)
- ⚠️ Lift marginalmente inferiore (7.76x vs 7.86x)

**Interpretazione:** Aggiungere r2 è ridondante con disp e riduce troppo le opportunità

---

## Raccomandazioni per Setup Operativo

### Setup 1: BILANCIATO

**Filtro:** Triple Magnitudine (script 05)

```python
abs(slope_5) > p75 AND
abs(slope_20) > p75 AND
abs(slope_60) > p75
```

**Metriche reali @ 5 minuti:**
- ~40-50 setup/giorno (stimato)
- P(hit TP) = 18.6%
- P(hit SL) = 32.9%
- P(neither) = 48.5%
- TP = +0.5%, SL = -0.3%

**Expectancy per trade:**
- 0.186 × 0.5% - 0.329 × 0.3% + 0.485 × 0.008% = **-0.002%** ❌

**Pro:**
- Buon numero di opportunità (~40-50/giorno)
- Lift P(TP) eccellente (4.3x vs baseline)
- Semplice da implementare

**Contro:**
- ⚠️ **Expectancy NEGATIVA** (sistema perdente con TP/SL attuali)
- P(hit SL) troppo alto (32.9%) rispetto a P(hit TP) (18.6%)
- Ratio TP/SL = 0.57 (sfavorevole)

**Quando usare:**
- ❌ NON usare con TP=0.5% / SL=0.3%
- ✅ Aumentare TP a 0.8% o ridurre SL a 0.2%
- ✅ O usare come filtro preliminare per Setup 2

---

### Setup 2: ALTA QUALITÀ

**Filtro:** Magnitudine + Dispersione (script 06 con --use-mag-filter)

```python
abs(slope_5) > p75 AND
abs(slope_20) > p75 AND
abs(slope_60) > p75 AND
disp_60 > p75
```

**Pro:**
- Win rate alto (33.8%)
- Qualità setup eccellente
- Mean high molto alto (0.478%)

**Contro:**
- Poche opportunità (~10-15/giorno)

**Quando usare:**
- Trading discrezionale/semi-automatico
- Preferenza per qualità su quantità
- Capital limitato (poche posizioni simultanee)

---

### Setup 3: ULTRA SELETTIVO (Solo per casi speciali)

**Filtro:** Magnitudine + Dispersione + R² basso

```python
abs(slope_5) > p75 AND
abs(slope_20) > p75 AND
abs(slope_60) > p75 AND
disp_60 > p75 AND
r2_60 < p25
```

**Pro:**
- Win rate massimo (33.4%)
- Setup di altissima qualità

**Contro:**
- Pochissime opportunità (~5-7/giorno)
- Lift simile a Setup 2 ma con meno opportunità

**Quando usare:**
- Trading manuale sporadico
- Conferma extra per posizioni ad alto rischio
- Situazioni dove ogni trade deve contare

---

## Parametri di Ottimizzazione

### Percentili Magnitudine

Testati in script 05, i risultati mostrano:

| Percentile | % Candele | P(hit TP) | Lift |
|------------|-----------|-----------|------|
| p50        | 49.9%     | 14.5%     | 3.3x |
| p75        | 25.0%     | 18.6%     | 4.3x |
| p90        | 9.9%      | 23.1%     | 5.3x |

**Trade-off:**
- p50: Più opportunità, lift inferiore
- p75: **Bilanciamento ottimale** (raccomandato)
- p90: Win rate massimo, pochissime opportunità

### Percentili Qualità (Dispersione)

Quando combinati con magnitudine p75:

| Disp Percentile | % Candele (del mag) | P(hit TP) | Lift addizionale |
|-----------------|---------------------|-----------|------------------|
| p50             | ~50%                | ~25%      | ~1.3x            |
| p75             | ~25%                | ~34%      | ~1.8x            |
| p90             | ~10%                | ~40%      | ~2.1x            |

**Note:** Valori stimati, test completi non ancora eseguiti

---

## Implementazione in Backtrader

### Codice Setup 1 (Bilanciato - Magnitudine)

```python
class IntradayStrategy(bt.Strategy):
    params = dict(
        slope5_pct=75,   # Percentile slope_5
        slope20_pct=75,  # Percentile slope_20
        slope60_pct=75,  # Percentile slope_60
    )

    def __init__(self):
        # Calcola regressioni (indicatori custom)
        self.slope5 = RegressionSlope(self.data, period=5)
        self.slope20 = RegressionSlope(self.data, period=20)
        self.slope60 = RegressionSlope(self.data, period=60)

        # Calcola soglie dinamiche (rolling quantiles)
        self.threshold5 = bt.indicators.PercentRank(
            abs(self.slope5), period=1000, upperband=self.p.slope5_pct
        )
        self.threshold20 = bt.indicators.PercentRank(
            abs(self.slope20), period=1000, upperband=self.p.slope20_pct
        )
        self.threshold60 = bt.indicators.PercentRank(
            abs(self.slope60), period=1000, upperband=self.p.slope60_pct
        )

    def next(self):
        # Filtro magnitudine
        if (self.threshold5 > self.p.slope5_pct and
            self.threshold20 > self.p.slope20_pct and
            self.threshold60 > self.p.slope60_pct):

            # Determina direzione da slope_60
            if self.slope60[0] > 0:
                self.buy()  # Long
            else:
                self.sell()  # Short
```

### Codice Setup 2 (Alta Qualità - Magnitudine + Dispersione)

```python
class IntradayStrategyQuality(bt.Strategy):
    params = dict(
        slope5_pct=75,
        slope20_pct=75,
        slope60_pct=75,
        disp60_pct=75,  # Aggiunto filtro dispersione
    )

    def __init__(self):
        # Regressioni
        self.slope5 = RegressionSlope(self.data, period=5)
        self.slope20 = RegressionSlope(self.data, period=20)
        self.slope60 = RegressionSlope(self.data, period=60)

        # Dispersione (indicatore custom)
        self.disp60 = RegressionDispersion(self.data, period=60)

        # Soglie
        self.threshold5 = bt.indicators.PercentRank(
            abs(self.slope5), period=1000, upperband=self.p.slope5_pct
        )
        self.threshold20 = bt.indicators.PercentRank(
            abs(self.slope20), period=1000, upperband=self.p.slope20_pct
        )
        self.threshold60 = bt.indicators.PercentRank(
            abs(self.slope60), period=1000, upperband=self.p.slope60_pct
        )
        self.threshold_disp = bt.indicators.PercentRank(
            self.disp60, period=1000, upperband=self.p.disp60_pct
        )

    def next(self):
        # Filtro magnitudine + qualità
        if (self.threshold5 > self.p.slope5_pct and
            self.threshold20 > self.p.slope20_pct and
            self.threshold60 > self.p.slope60_pct and
            self.threshold_disp > self.p.disp60_pct):  # AGGIUNTO

            if self.slope60[0] > 0:
                self.buy()
            else:
                self.sell()
```

---

## Metriche Attese (Forward Testing)

### Setup 1 (Bilanciato)

**Assunzioni:**
- 40 setup/giorno
- P(hit TP) = 18.6%
- P(hit SL) = 37.1%
- TP = +0.5%, SL = -0.3%

**Metriche giornaliere stimate:**
- Trades win: ~7
- Trades loss: ~15
- Trades neither: ~18
- P&L atteso: 7×0.5% - 15×0.3% = -1.0% ❌

⚠️ **ATTENZIONE:** Con questo R:R (0.5/0.3 = 1.67) e win rate 18.6%, il sistema è PERDENTE!

**Soluzioni:**
1. Aumentare TP (es. 0.8%) o ridurre SL (es. 0.2%)
2. Usare Setup 2 (win rate più alto)
3. Aggiungere filtri temporali (evitare fasce orarie sfavorevoli)

### Setup 2 (Alta Qualità)

**Metriche reali @ 5 minuti:**
- 12 setup/giorno (stimato su dataset multi-asset)
- P(hit TP) = 33.8%
- P(hit SL) = 43.6%
- P(neither) = 22.6%
- TP = +0.5%, SL = -0.3%

**Expectancy per trade:**
- 0.338 × 0.5% - 0.436 × 0.3% + 0.226 × 0.009% = **+0.040%** ✅

**Metriche giornaliere stimate (12 trades):**
- Trades win (hit TP): ~4.1
- Trades loss (hit SL): ~5.2
- Trades neither: ~2.7
- P&L atteso giornaliero: 12 × 0.040% = **+0.48%**

**Caratteristiche:**
- ✅ Sistema profittevole (expectancy positiva)
- ⚠️ Edge molto piccolo (0.04% per trade, ~2-3 tick)
- ⚠️ Commissioni/slippage potrebbero annullare il vantaggio
- ⚠️ P(hit SL) > P(hit TP): sistema sfavorevole in termini di win rate, ma R:R compensa

**Ratio TP/SL:** 33.8% / 43.6% = 0.78 (migliore tra tutti i setup)

---

## Ottimizzazione TP/SL

### Problema Attuale

Con TP = 0.5% e SL = 0.3% (R:R = 1.67):
- **Setup 1:** Expectancy = -0.002% (PERDENTE)
- **Setup 2:** Expectancy = +0.040% (marginalmente profittevole)

Il problema è che **P(hit SL) > P(hit TP)** in entrambi i setup:
- Setup 1: 32.9% SL vs 18.6% TP → Ratio 0.57
- Setup 2: 43.6% SL vs 33.8% TP → Ratio 0.78

### Soluzioni Possibili

#### Opzione A: Aumentare TP (Target più lontano)

**TP = 0.8%, SL = 0.3%** (R:R = 2.67)

Assumendo P(hit TP) scenda proporzionalmente:
- Setup 1: P(TP) ≈ 12%, P(SL) ≈ 32%
  - Expectancy ≈ 0.12×0.8% - 0.32×0.3% = +0.000% (break-even)
- Setup 2: P(TP) ≈ 23%, P(SL) ≈ 44%
  - Expectancy ≈ 0.23×0.8% - 0.44×0.3% = +0.052% ✅

**Pro:** Migliora expectancy, R:R più favorevole
**Contro:** Win rate più basso, tempo holding più lungo

#### Opzione B: Ridurre SL (Stop più stretto)

**TP = 0.5%, SL = 0.2%** (R:R = 2.5)

Assumendo P(hit SL) scenda:
- Setup 1: P(TP) ≈ 18%, P(SL) ≈ 24%
  - Expectancy ≈ 0.18×0.5% - 0.24×0.2% = +0.042% ✅
- Setup 2: P(TP) ≈ 34%, P(SL) ≈ 33%
  - Expectancy ≈ 0.34×0.5% - 0.33×0.2% = +0.104% ✅✅

**Pro:** Migliora molto expectancy, stop risk ridotto
**Contro:** SL più stretto potrebbe essere toccato troppo facilmente

#### Opzione C: Asimmetrico (TP lontano, SL stretto)

**TP = 0.8%, SL = 0.2%** (R:R = 4.0)

Migliore dei due mondi:
- Setup 2: P(TP) ≈ 23%, P(SL) ≈ 33%
  - Expectancy ≈ 0.23×0.8% - 0.33×0.2% = +0.118% ✅✅✅

**Pro:** Expectancy ottima, R:R molto favorevole
**Contro:** Richiede test accurato per stimare P(hit TP/SL)

### Raccomandazione

**TESTARE Opzione B** (TP=0.5%, SL=0.2%) perché:
1. Minimo cambiamento rispetto a setup attuale
2. Migliora drasticamente expectancy
3. Stop loss più stretto = rischio ridotto
4. Setup 2 diventa chiaramente profittevole (+0.104% vs +0.040%)

**Come testare:**
```bash
# Riesegui analisi con SL = 0.2%
python analyze_combined_filter_full_metrics.py --sl-pct 0.002
```

---

## Prossimi Step

### 1. Verifica P(hit SL) per Setup 2

Riesegui script 06 con parametri:
```bash
python 06-quality_filter_analysis.py \
    --use-mag-filter \
    --tp-pct 0.005 \
    --sl-pct 0.003
```

Verifica se P(hit SL) è accettabile (~40% max)

### 2. Test Ottimizzazione TP/SL

Testa diverse combinazioni:
- TP 0.8%, SL 0.2% (R:R = 4.0)
- TP 0.6%, SL 0.25% (R:R = 2.4)
- TP 0.5%, SL 0.3% (R:R = 1.67, attuale)

### 3. Analisi Temporale

Verifica se ci sono fasce orarie migliori:
- Evitare primi 30 minuti (alta volatilità)
- Evitare ultimi 30 minuti (closing auction)
- Focus su 10:00-15:00?

### 4. Backtest Reale con Commissioni

Implementa in backtrader con:
- Commissioni realistiche
- Slippage
- Verifica drawdown

### 5. Paper Trading

Testa Setup 2 in paper trading per 2-4 settimane prima di andare live

---

## Conclusioni

**Setup Raccomandato: #2 (Magnitudine + Dispersione)**

Motivazioni:
1. Win rate ~34% permette profittabilità anche con R:R conservativo
2. Numero opportunità sufficiente (~12/giorno)
3. Qualità setup molto alta (mean high 0.478%)
4. Riduce rischio overtrading (vs Setup 1 con 40+ setup/giorno)

**Parametri finali:**
```python
slope5_pct = 75
slope20_pct = 75
slope60_pct = 75
disp60_pct = 75
TP = 0.005 (0.5%)
SL = 0.003 (0.3%)
```

**Prossimo step critico:** Verificare P(hit SL) per confermare profittabilità attesa.
