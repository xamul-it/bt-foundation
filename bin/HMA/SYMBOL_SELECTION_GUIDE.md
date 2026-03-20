# Symbol Selection - Guida al Filtro Sistematico

**Data**: 2026-02-13
**Scopo**: Creare un filtro automatico per selezionare simboli ad alta volatilità

---

## Problema

**Attuale**: Selezione manuale di 20 simboli "NASDAQ high-volatility"
- ❌ Soggettivo
- ❌ Non aggiornabile dinamicamente
- ❌ Non testabile/validabile

**Soluzione**: Filtro sistematico basato su metriche oggettive calcolate su periodo precedente

---

## Approccio

### 1. Analisi Storica (Training Period)

Calcoliamo caratteristiche dei simboli **PRIMA** del periodo di validazione:

```
Training: 2025-02-06 → 2025-05-05 (90 giorni)
Validation: 2025-05-06 → 2025-07-03 (41 giorni)
```

### 2. Metriche Calcolate

Per ogni simbolo, su lookback di **30, 60, 120 giorni**:

| Categoria | Metriche | Scopo |
|-----------|----------|-------|
| **Volatilità ATR** | • ATR medio<br>• ATR % medio<br>• ATR % mediano<br>• ATR % P75<br>• ATR % max | Misura volatilità assoluta e relativa |
| **Frequenza Alta Vol** | • % giorni ATR ≥ 0.7% | Quanti giorni ha volatilità sufficiente? |
| **Volatilità Returns** | • Std returns<br>• Std returns annualized | Volatilità classica |
| **Liquidità** | • Volume medio<br>• Dollar volume medio | Sufficiente liquidità per trading? |
| **Prezzo** | • Prezzo medio<br>• Std prezzo | Livello prezzo e stabilità |

### 3. Obiettivo

Trovare soglie che:
- ✅ Selezionano i 20 simboli Monte Carlo (recall alto)
- ✅ Escludono simboli non performanti (precision alto)
- ✅ Sono robuste nel tempo

---

## Come Usare

### Step 1: Eseguire Analisi

```bash
cd /home/htpc/backtrader/backtrader
source backtrader/bin/activate
python bin/HMA/analyze_symbol_characteristics.py
```

### Step 2: Risultati Generati

```
bin/HMA/symbol_analysis/
├── symbol_metrics.csv           # Metriche per ogni simbolo
├── threshold_analysis.csv       # Soglie ottimali testate
└── mc_vs_others_comparison.csv  # Confronto MC vs Altri
```

### Step 3: Analizzare Output

**symbol_metrics.csv**: Una riga per simbolo con tutte le metriche
```csv
symbol,atr_pct_mean_30d,atr_pct_mean_60d,pct_days_atr_high_30d,...,in_monte_carlo
TSLA,0.0235,0.0218,0.45,True
AAPL,0.0087,0.0091,0.12,True
...
```

**threshold_analysis.csv**: Soglie testate ordinate per F1 score
```csv
metric,threshold,recall,precision,f1,mc_pass,total_pass
atr_pct_mean_60d,0.0123,0.95,0.86,0.90,19,22
pct_days_atr_high_30d,0.25,0.90,0.82,0.86,18,22
...
```

**mc_vs_others_comparison.csv**: Differenze tra MC e altri simboli
```csv
metric,mc_mean,other_mean,ratio,diff_pct
atr_pct_mean_60d,0.0156,0.0089,1.75,75.3%
pct_days_atr_high_60d,0.28,0.11,2.55,154.5%
...
```

---

## Interpretazione Risultati

### Esempio Output Atteso:

```
OPTIMAL THRESHOLD ANALYSIS
================================================================================

Top Threshold Configurations by F1 Score:

metric                  threshold  mc_pass  mc_total  other_pass  recall  precision   f1
atr_pct_mean_60d        0.0123     19       20        3           0.95    0.86       0.90
pct_days_atr_high_60d   0.25       18       20        4           0.90    0.82       0.86
atr_pct_median_60d      0.0115     19       20        5           0.95    0.79       0.86
```

**Interpretazione**:
- **recall 0.95**: Il filtro cattura 19/20 (95%) dei simboli Monte Carlo ✅
- **precision 0.86**: Dei 22 simboli selezionati, 19 sono corretti (86%) ✅
- **F1 0.90**: Ottimo bilanciamento recall/precision ✅

### Filtro Raccomandato:

Basato su analisi, esempio:

```python
# Filtro Singola Metrica (semplice)
atr_pct_mean_60d >= 0.0123  # 1.23%

# Filtro Multi-Criteria (robusto)
atr_pct_mean_60d >= 0.0120 AND
pct_days_atr_high_60d >= 0.25 AND
dollar_volume_mean_60d >= 50_000_000
```

---

## Implementazione in Backtrader

### Opzione 1: JSON File Dinamico

Creare script che aggiorna automaticamente la lista simboli:

```python
# bin/update_hv_symbols.py
import pandas as pd
from datetime import datetime, timedelta

def update_hv_symbols():
    # Calcola metriche su ultimi 60 giorni
    end_date = datetime.now()
    start_date = end_date - timedelta(days=60)

    # Calcola per tutti i simboli
    metrics = calculate_all_metrics(start_date, end_date)

    # Applica filtro
    selected = metrics[
        (metrics['atr_pct_mean_60d'] >= 0.0123) &
        (metrics['pct_days_atr_high_60d'] >= 0.25)
    ]

    # Salva JSON
    symbols = selected['symbol'].tolist()
    with open('config/tickers/NASDAQ_HV_DYNAMIC.json', 'w') as f:
        json.dump(symbols, f)

    return symbols

# Eseguire settimanalmente o mensilmente
```

### Opzione 2: Filtro In-Strategy

Integrare il filtro direttamente nella strategia:

```python
class HMADynamic(IntradayStrategy):
    def __init__(self):
        super().__init__()

        # Calcola metriche pre-periodo per ogni simbolo
        self.symbol_metrics = {}
        for d in self.datas:
            metrics = self.calculate_pre_period_metrics(d)
            self.symbol_metrics[d] = metrics

        # Filtra simboli
        self.active_symbols = [
            d for d in self.datas
            if self.symbol_metrics[d]['atr_pct_mean_60d'] >= 0.0123
        ]

    def calculate_pre_period_metrics(self, data):
        # Calcola ATR medio su ultimi 60 giorni
        # (implementazione dettagliata)
        pass
```

---

## Vantaggi del Filtro Sistematico

### ✅ Oggettività
- Decisioni basate su dati, non opinioni
- Replicabile e testabile

### ✅ Adattabilità
- Aggiornabile automaticamente
- Si adatta a cambiamenti di mercato

### ✅ Backtestabile
- Può essere testato su periodi storici
- Verifica robustezza nel tempo

### ✅ Trasparenza
- Criteri chiari e misurabili
- Facilmente spiegabile

---

## Prossimi Passi

### 1. Eseguire Analisi Iniziale
```bash
python bin/HMA/analyze_symbol_characteristics.py
```

### 2. Verificare Risultati
- Controllare recall/precision
- Verificare soglie proposte
- Identificare metriche più predittive

### 3. Validare su Altri Periodi
Ripetere analisi su periodi diversi:
- Q1 2025: Training Nov-Jan, Test Feb-Mar
- Q4 2024: Training Jul-Sep, Test Oct-Dec
- Verificare se soglie rimangono stabili

### 4. Implementare Filtro
- Creare script di aggiornamento automatico
- Oppure integrare in strategia

### 5. Paper Trading
- Testare con filtro dinamico
- Confrontare vs selezione manuale

---

## Note Importanti

### Attenzione a:

1. **Survivorship Bias**
   - Simboli delisted non sono nei dati
   - Considerare solo simboli attivi

2. **Look-Ahead Bias**
   - Usare SOLO dati disponibili alla data di selezione
   - Non "spiare" nel futuro

3. **Overfitting**
   - Non ottimizzare troppo su singolo periodo
   - Verificare stabilità su multipli periodi

4. **Data Quality**
   - Verificare dati completi per periodo training
   - Escludere simboli con dati mancanti

### Best Practices:

- **Rivedere filtro mensilmente**: Mercato cambia, filtro deve adattarsi
- **Mantenere storico**: Salvare lista simboli selezionati per ogni periodo
- **Monitorare performance**: Verificare se simboli selezionati performano
- **A/B Testing**: Confrontare filtro automatico vs manuale

---

## Appendice: Metriche Dettagliate

### ATR % Mean
```python
atr_pct_mean = atr.mean() / close.mean()
```
**Interpretazione**: Volatilità media come % del prezzo
**Tipico High-Vol**: > 1.2%
**Tipico Low-Vol**: < 0.8%

### % Days ATR High
```python
pct_days_high = (atr_pct >= 0.007).sum() / len(atr_pct)
```
**Interpretazione**: Frazione giorni con ATR ≥ 0.7%
**Tipico High-Vol**: > 25%
**Tipico Low-Vol**: < 10%

### Annualized Volatility
```python
vol_ann = returns.std() * sqrt(252)
```
**Interpretazione**: Volatilità annualizzata (standard)
**Tipico High-Vol**: > 40%
**Tipico Low-Vol**: < 25%

---

**Autore**: Claude Code
**Data**: 2026-02-13
**Versione**: 1.0
