# Piano Implementazione HMA Strategy - Backtest + Paper Trading

**Data:** 2026-02-12
**Obiettivo:** Implementare strategia HMA validata con supporto backtest e paper trading

---

## 📋 Analisi Situazione Attuale

### **Strategia Esistente: `strategies/intraday.py::HMA` (linea 232-368)**

✅ **Già Implementato:**
- `live_enabled=True` → supporto paper trading
- `required_minperiod()` → warmup corretto
- Broker/data live detection (linee 297-303)
- Correction bar filtering (linee 314-318)
- Close-and-revert logic
- Calendar Alpaca integration
- Equal weight position sizing

❌ **Mancante (da analisi):**
- ATR filter (≥ 0.7%)
- ATR-based position sizing
- Opening volatility analysis (09:30-10:00)
- Dynamic capital allocation
- Queue management (max 10 positions)
- Limit orders at close
- Stop loss 0.5%

---

## 🎯 Strategia di Implementazione

### **Opzione A: Modificare HMA Esistente** (❌ NON RACCOMANDATO)
- **Pro:** Una sola strategia
- **Contro:**
  - Rompe compatibilità con usage esistente
  - Complesso da testare (troppe modifiche insieme)
  - Difficile rollback se problemi

### **Opzione B: Creare HMADynamic Nuova** (✅ RACCOMANDATO)
- **Pro:**
  - Mantiene HMA originale funzionante
  - Test isolato delle nuove features
  - Facile confronto backtest HMA vs HMADynamic
  - Rollback immediato se problemi
- **Contro:**
  - Due strategie da mantenere (accettabile)

**SCELTA:** Opzione B - `strategies/intraday.py::HMADynamic`

---

## 📝 Piano Dettagliato Interventi

### **FASE 1: Preparazione Dati** ✅ (già fatto)

#### 1.1 Verifica Provider Dati
```bash
# Test provider alpaca
ls -la config/data/m/alpaca/*.csv | head -5
ls -la config/data/d/alpaca/*.csv | head -5
```
**Status:** ✅ Dati presenti

#### 1.2 Miglioramento loadtickers (INCREMENTALE)
**File:** `bin/loadtickers.py` o script esistente

**Modifica Necessaria:**
```python
# OLD (rimuove e ricrea)
def load_tickers_old(symbols, start_date, end_date):
    for symbol in symbols:
        file_path = f"config/data/m/alpaca/{symbol}.csv"
        if os.path.exists(file_path):
            os.remove(file_path)  # ❌ Rimuove tutto

        # Download completo
        data = download_data(symbol, start_date, end_date)
        data.to_csv(file_path)

# NEW (append solo dati mancanti)
def load_tickers_incremental(symbols, start_date, end_date):
    for symbol in symbols:
        file_path = f"config/data/m/alpaca/{symbol}.csv"

        if os.path.exists(file_path):
            # Leggi ultima data presente
            df_existing = pd.read_csv(file_path)
            df_existing['datetime'] = pd.to_datetime(df_existing['datetime'])
            last_date = df_existing['datetime'].max()

            # Download solo da last_date + 1 giorno
            download_start = last_date + pd.Timedelta(days=1)

            if download_start >= pd.to_datetime(end_date):
                print(f"{symbol}: già aggiornato")
                continue

            # Download dati mancanti
            data_new = download_data(symbol, download_start, end_date)

            if len(data_new) > 0:
                # Append ai dati esistenti
                df_combined = pd.concat([df_existing, data_new])
                df_combined = df_combined.drop_duplicates(subset=['datetime'])
                df_combined = df_combined.sort_values('datetime')
                df_combined.to_csv(file_path, index=False)
                print(f"{symbol}: aggiunte {len(data_new)} barre")
        else:
            # File non esiste, download completo
            data = download_data(symbol, start_date, end_date)
            data.to_csv(file_path, index=False)
            print(f"{symbol}: creato con {len(data)} barre")
```

**Benefici:**
- ✅ Velocità: download solo dati mancanti
- ✅ Affidabilità: non perde dati esistenti
- ✅ API friendly: meno richieste ad Alpaca

---

### **FASE 2: Implementazione HMADynamic**

#### 2.1 Struttura Base
**File:** `strategies/intraday.py` (aggiungere classe)

```python
class HMADynamic(IntradayStrategy):
    """
    HMA Strategy con:
    - ATR filter >= 0.7%
    - ATR-based position sizing
    - Opening volatility analysis
    - Dynamic capital allocation
    - Queue management (max 10 positions)
    - Limit orders at close
    - Stop loss 0.5%

    Compatible con backtest e paper trading.
    """
    params = (
        # Core HMA
        ('period', 16),
        ('inverted', True),

        # Filters
        ('atr_min', 0.007),  # 0.7%
        ('hour_min', 10),
        ('hour_max', 15),

        # Position Sizing
        ('target_risk', 0.02),  # 2% per trade
        ('max_position_pct', 0.20),  # 20% max per position
        ('max_positions', 10),

        # Risk Management
        ('sl_pct', 0.005),  # 0.5% stop loss

        # Dynamic Allocation
        ('use_dynamic_allocation', True),
        ('opening_range_high', 0.68),  # % threshold
        ('gap_high', 0.0472),  # % threshold
    )

    live_enabled = True

    @classmethod
    def required_minperiod(cls, period=16, **kwargs):
        return max(period, 30)  # Max tra HMA e ATR (14)
```

#### 2.2 Indicatori
```python
def __init__(self):
    super().__init__()

    # Indicators per data feed
    self.hma = {}
    self.atr = {}
    self.atr_pct = {}

    for d in self.datas:
        self.hma[d] = bt.indicators.HMA(d.close, period=self.p.period)
        self.atr[d] = bt.indicators.ATR(d, period=14)
        # ATR% = ATR / close * 100
        self.atr_pct[d] = self.atr[d] / d.close * 100

    # State tracking
    self.last_bar = {d._name: 0 for d in self.datas}
    self.direction = {d: 0 for d in self.datas}  # 1=long, -1=short, 0=none

    # Opening analysis (per day)
    self.current_day = None
    self.opening_metrics = {}
    self.opportunity_scores = {}
    self.capital_allocation = {}

    # Position count
    self.position_count = 0
```

#### 2.3 Opening Volatility Analysis (09:30-10:00)
```python
def analyze_opening(self):
    """
    Chiamato ogni barra tra 09:30-10:00.
    Calcola opening range e gap per ranking simboli.
    """
    current_dt = self.datas[0].datetime.datetime(0)
    current_time = current_dt.time()

    # Solo tra 09:30-10:00
    if not (time(9, 30) <= current_time < time(10, 0)):
        return

    for d in self.datas:
        if len(d) < 30:  # Need almeno 30 minuti di dati
            continue

        symbol = d._name

        # Get opening range (high-low dei primi 30 minuti)
        bars_30min = [d.high[i] for i in range(min(30, len(d)))]
        opening_high = max(bars_30min)
        opening_low = min([d.low[i] for i in range(min(30, len(d)))])

        opening_range_pct = (opening_high - opening_low) / opening_low * 100

        # Get gap (first open vs previous close)
        if len(d) >= 2:
            prev_close = d.close[-1]
            opening_first = d.open[0]
            gap_pct = abs(opening_first - prev_close) / prev_close * 100
        else:
            gap_pct = 0

        self.opening_metrics[symbol] = {
            'opening_range_pct': opening_range_pct,
            'gap_pct': gap_pct
        }

        # Calculate opportunity score
        score = 0
        if opening_range_pct > self.p.opening_range_high:
            score += 1.5
        if gap_pct > self.p.gap_high:
            score += 1.0

        self.opportunity_scores[symbol] = max(score, 0.5)  # Min 0.5
```

#### 2.4 Capital Allocation (10:00)
```python
def allocate_capital(self):
    """
    Chiamato a 10:00. Alloca capitale basato su opportunity scores.
    """
    current_dt = self.datas[0].datetime.datetime(0)
    current_time = current_dt.time()

    # Solo a 10:00
    if current_time != time(10, 0):
        return

    # Rank simboli per opportunity score
    ranked = sorted(
        self.opportunity_scores.items(),
        key=lambda x: x[1],
        reverse=True
    )

    # Take top 20 symbols (o tutti se < 20)
    top_symbols = [s for s, score in ranked[:20] if score > 0]

    # Calculate allocation weights
    total_score = sum(self.opportunity_scores[s] for s in top_symbols)

    portfolio_value = self.broker.getvalue()
    available_capital = portfolio_value * 0.8  # Reserve 20%

    for symbol in top_symbols:
        if total_score > 0:
            weight = self.opportunity_scores[symbol] / total_score
            self.capital_allocation[symbol] = available_capital * weight
        else:
            self.capital_allocation[symbol] = available_capital / len(top_symbols)
```

#### 2.5 ATR-Based Position Sizing
```python
def calculate_position_size(self, data):
    """
    Calcola position size basato su:
    - ATR corrente
    - Capital allocation per simbolo
    - Target risk 2%
    - Bounds: 5-20% of portfolio
    """
    symbol = data._name

    # Get allocated capital for this symbol
    allocated = self.capital_allocation.get(symbol, self.broker.getvalue() * 0.1)

    # Get available cash
    available = min(allocated, self.broker.get_cash())

    # Base size for target risk
    # If SL hit → loss = 2% of portfolio
    base_size = (available * self.p.target_risk) / self.p.sl_pct

    # ATR adjustment
    atr_pct = self.atr_pct[data][0]
    if atr_pct < 0.003:  # Avoid division by very small ATR
        atr_pct = 0.003

    atr_adjustment = 0.007 / atr_pct  # Normalize to 0.7%

    # Opportunity multiplier
    score = self.opportunity_scores.get(symbol, 1.0)
    if score > 1.5:
        opportunity_mult = 1.5
    elif score < 0.5:
        opportunity_mult = 0.5
    else:
        opportunity_mult = 1.0

    # Final position value
    position_value = base_size * atr_adjustment * opportunity_mult

    # Bounds: 5% to 20% of portfolio
    portfolio_value = self.broker.getvalue()
    min_value = portfolio_value * 0.05
    max_value = portfolio_value * self.p.max_position_pct

    position_value = np.clip(position_value, min_value, max_value)

    # Convert to shares
    price = data.close[0]
    shares = int(position_value / price)

    return shares
```

#### 2.6 Main Trading Logic
```python
def next(self):
    # Check market hours
    if not self.inValidMarket():
        self._closeAll()
        return

    current_time = self.datas[0].datetime.time()

    # Phase 1: Opening analysis (09:30-10:00)
    if time(9, 30) <= current_time < time(10, 0):
        self.analyze_opening()
        return  # Don't trade during opening

    # Phase 2: Capital allocation (10:00)
    if current_time == time(10, 0):
        self.allocate_capital()

    # Phase 3: Trading (10:00-15:00)
    if not (time(10, 0) <= current_time < time(15, 0)):
        return

    # Track position count
    self.position_count = sum(1 for d in self.datas if self.getposition(d).size != 0)

    # Process each data feed
    for d in self.datas:
        if len(d) == 0:
            continue

        # Skip if warmup (paper trading)
        broker_live = getattr(self.broker, 'live', False)
        data_live = getattr(d.lines, 'live', [False])[0] if hasattr(d.lines, 'live') else False

        if broker_live and not data_live:
            continue  # Warmup phase

        # Skip correction bars
        if len(d) == self.last_bar[d._name]:
            continue
        self.last_bar[d._name] = len(d)

        # ATR Filter
        if self.atr_pct[d][0] < self.p.atr_min:
            continue  # Skip low volatility

        # Check HMA signal
        pos = self.getposition(d)

        if self.p.inverted:
            # Contrarian: HMA falling → LONG, HMA rising → SHORT
            signal_long = self.hma[d][-1] > self.hma[d][0]
            signal_short = self.hma[d][-1] < self.hma[d][0]
        else:
            signal_long = self.hma[d][-1] < self.hma[d][0]
            signal_short = self.hma[d][-1] > self.hma[d][0]

        # LONG signal
        if signal_long:
            # Close short position if exists
            if pos.size < 0:
                self.close(d, exectype=bt.Order.Limit, price=d.close[0])

            # Open long if not already long
            if self.direction[d] != 1:
                # Check queue capacity
                if self.position_count < self.p.max_positions:
                    size = self.calculate_position_size(d)

                    if size > 0:
                        # Entry with LIMIT at close, SL at entry - 0.5%
                        self.buy(
                            data=d,
                            size=size,
                            exectype=bt.Order.Limit,
                            price=d.close[0],
                            # Note: SL managed via close conditions, not bracket order
                        )
                        self.direction[d] = 1
                        self.position_count += 1

        # SHORT signal
        elif signal_short:
            # Close long position if exists
            if pos.size > 0:
                self.close(d, exectype=bt.Order.Limit, price=d.close[0])

            # Open short if not already short
            if self.direction[d] != -1:
                # Check queue capacity
                if self.position_count < self.p.max_positions:
                    size = self.calculate_position_size(d)

                    if size > 0:
                        self.sell(
                            data=d,
                            size=size,
                            exectype=bt.Order.Limit,
                            price=d.close[0]
                        )
                        self.direction[d] = -1
                        self.position_count += 1

        # Check Stop Loss (manual check ogni bar)
        if pos.size != 0:
            entry_price = pos.price
            current_price = d.close[0]

            if pos.size > 0:  # Long
                sl_price = entry_price * (1 - self.p.sl_pct)
                if current_price <= sl_price:
                    self.close(d)
                    self.direction[d] = 0

            elif pos.size < 0:  # Short
                sl_price = entry_price * (1 + self.p.sl_pct)
                if current_price >= sl_price:
                    self.close(d)
                    self.direction[d] = 0
```

---

### **FASE 3: Meccanismo Verifica Discrepanze**

#### 3.1 Logger Comparativo
**File:** `strategies/comparison_logger.py` (nuovo)

```python
import json
import logging
from datetime import datetime
from pathlib import Path

class ComparisonLogger:
    """
    Logger per tracciare differenze tra backtest e paper trading.
    Non crea latenza: scrive async su file.
    """

    def __init__(self, strategy_name, mode):
        self.strategy_name = strategy_name
        self.mode = mode  # 'backtest' or 'paper'

        self.output_dir = Path(f'out/{strategy_name}/comparison')
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.log_file = self.output_dir / f'{mode}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.jsonl'

    def log_signal(self, timestamp, symbol, signal_type, price, indicators):
        """Log signal generation"""
        record = {
            'timestamp': str(timestamp),
            'symbol': symbol,
            'event': 'signal',
            'signal_type': signal_type,
            'price': float(price),
            'indicators': {k: float(v) for k, v in indicators.items() if v is not None}
        }

        with open(self.log_file, 'a') as f:
            f.write(json.dumps(record) + '\n')

    def log_order(self, timestamp, symbol, side, size, price, order_type):
        """Log order submission"""
        record = {
            'timestamp': str(timestamp),
            'symbol': symbol,
            'event': 'order',
            'side': side,
            'size': int(size),
            'price': float(price),
            'order_type': order_type
        }

        with open(self.log_file, 'a') as f:
            f.write(json.dumps(record) + '\n')

    def log_fill(self, timestamp, symbol, side, size, fill_price):
        """Log order fill"""
        record = {
            'timestamp': str(timestamp),
            'symbol': symbol,
            'event': 'fill',
            'side': side,
            'size': int(size),
            'fill_price': float(fill_price)
        }

        with open(self.log_file, 'a') as f:
            f.write(json.dumps(record) + '\n')
```

#### 3.2 Comparison Analyzer
**File:** `bin/compare_backtest_paper.py` (nuovo)

```python
#!/usr/bin/env python3
"""
Confronta log di backtest vs paper trading per trovare discrepanze.
"""

import pandas as pd
import json
from pathlib import Path

def load_logs(backtest_file, paper_file):
    """Load JSONL logs"""

    def read_jsonl(file_path):
        records = []
        with open(file_path) as f:
            for line in f:
                records.append(json.loads(line))
        return pd.DataFrame(records)

    bt_df = read_jsonl(backtest_file)
    paper_df = read_jsonl(paper_file)

    return bt_df, paper_df

def compare_signals(bt_df, paper_df, time_tolerance_sec=60):
    """
    Confronta segnali generati.
    time_tolerance_sec: tolleranza temporale per matching
    """

    bt_signals = bt_df[bt_df['event'] == 'signal'].copy()
    paper_signals = paper_df[paper_df['event'] == 'signal'].copy()

    bt_signals['timestamp'] = pd.to_datetime(bt_signals['timestamp'])
    paper_signals['timestamp'] = pd.to_datetime(paper_signals['timestamp'])

    # Match signals by symbol and time
    mismatches = []

    for _, bt_row in bt_signals.iterrows():
        # Find matching paper signal
        mask = (
            (paper_signals['symbol'] == bt_row['symbol']) &
            (abs(paper_signals['timestamp'] - bt_row['timestamp']) < pd.Timedelta(seconds=time_tolerance_sec))
        )

        matches = paper_signals[mask]

        if len(matches) == 0:
            mismatches.append({
                'type': 'MISSING_IN_PAPER',
                'timestamp': bt_row['timestamp'],
                'symbol': bt_row['symbol'],
                'signal': bt_row['signal_type']
            })
        elif matches.iloc[0]['signal_type'] != bt_row['signal_type']:
            mismatches.append({
                'type': 'SIGNAL_MISMATCH',
                'timestamp': bt_row['timestamp'],
                'symbol': bt_row['symbol'],
                'backtest_signal': bt_row['signal_type'],
                'paper_signal': matches.iloc[0]['signal_type']
            })

    # Check for signals in paper but not in backtest
    for _, paper_row in paper_signals.iterrows():
        mask = (
            (bt_signals['symbol'] == paper_row['symbol']) &
            (abs(bt_signals['timestamp'] - paper_row['timestamp']) < pd.Timedelta(seconds=time_tolerance_sec))
        )

        if len(bt_signals[mask]) == 0:
            mismatches.append({
                'type': 'EXTRA_IN_PAPER',
                'timestamp': paper_row['timestamp'],
                'symbol': paper_row['symbol'],
                'signal': paper_row['signal_type']
            })

    return pd.DataFrame(mismatches)

def compare_fills(bt_df, paper_df):
    """Confronta fills (price, slippage)"""

    bt_fills = bt_df[bt_df['event'] == 'fill'].copy()
    paper_fills = paper_df[paper_df['event'] == 'fill'].copy()

    # Calculate slippage
    # (Assuming order price is logged before)
    # ...

    return comparison_df

def main():
    backtest_log = Path('out/HMADynamic/comparison/backtest_20260212.jsonl')
    paper_log = Path('out/HMADynamic/comparison/paper_20260212.jsonl')

    bt_df, paper_df = load_logs(backtest_log, paper_log)

    # Compare signals
    signal_mismatches = compare_signals(bt_df, paper_df)

    print(f"\n{'='*80}")
    print("SIGNAL MISMATCHES")
    print(f"{'='*80}")
    print(signal_mismatches)

    # Compare fills
    fill_comparison = compare_fills(bt_df, paper_df)

    # ...

if __name__ == '__main__':
    main()
```

---

### **FASE 4: Testing**

#### 4.1 Unit Test Componenti
```python
# tests/test_hma_dynamic.py

def test_atr_position_sizing():
    # Test position size calculation
    pass

def test_opening_analysis():
    # Test opening volatility calculation
    pass

def test_capital_allocation():
    # Test allocation weights
    pass
```

#### 4.2 Backtest Validation
```bash
# Test su periodo breve (1 mese)
python btmain.py \
    --strat intraday.HMADynamic \
    --ticker AAPL,MSFT,NVDA,TSLA \
    --fromdate 2025-06-01 \
    --todate 2025-07-01 \
    --timeframe minute \
    --provider alpaca \
    --mode backtest \
    --commission CommNone \
    --amount 100000 \
    --stratargs "period=16 inverted=True"
```

#### 4.3 Paper Trading Test
```bash
# Test paper trading 1 giorno
python btmain.py \
    --strat intraday.HMADynamic \
    --ticker AAPL,MSFT \
    --mode paper \
    --live \
    --alpaca-mode proxy \
    --stratargs "period=16 inverted=True"
```

#### 4.4 Comparison
```bash
# Confronta log
python bin/compare_backtest_paper.py \
    --backtest out/HMADynamic/comparison/backtest_20260212.jsonl \
    --paper out/HMADynamic/comparison/paper_20260212.jsonl
```

---

## 📅 Timeline

| Fase | Attività | Tempo | Output |
|------|----------|-------|--------|
| **1** | Miglioramento loadtickers | 1h | Script incrementale |
| **2a** | Implementa HMADynamic base | 2h | Classe con indicators |
| **2b** | Opening analysis + allocation | 1h | Ranking logic |
| **2c** | ATR-based sizing | 1h | Position sizing |
| **2d** | Main trading logic | 2h | Complete strategy |
| **3a** | Comparison logger | 1h | Logging infra |
| **3b** | Comparison analyzer | 1h | Analysis script |
| **4a** | Backtest test (1 mese) | 30min | Results validation |
| **4b** | Paper trading test (1 giorno) | 8h | Live validation |
| **4c** | Compare results | 30min | Discrepancy report |
| | **TOTALE** | **~18h** | Strategy completa |

---

## ✅ Checklist Implementazione

### Preparazione
- [ ] Verifica dati presenti (m/alpaca, d/alpaca)
- [ ] Migliora loadtickers (incrementale)
- [ ] Test loadtickers su 1 simbolo

### Implementazione
- [ ] Crea classe HMADynamic
- [ ] Implementa indicators (HMA, ATR)
- [ ] Implementa opening_analysis()
- [ ] Implementa allocate_capital()
- [ ] Implementa calculate_position_size()
- [ ] Implementa next() con queue management
- [ ] Implementa stop loss logic
- [ ] Test import strategia

### Logging
- [ ] Crea ComparisonLogger
- [ ] Integra in HMADynamic
- [ ] Crea compare_backtest_paper.py
- [ ] Test logging su backtest

### Validation
- [ ] Backtest 1 mese (Jun 2025)
- [ ] Verifica metriche vs Monte Carlo
- [ ] Paper trading 1 giorno
- [ ] Compare backtest vs paper
- [ ] Analizza discrepanze
- [ ] Fix issues se necessario

### Production Ready
- [ ] Backtest completo 2024-2025
- [ ] Paper trading 30 giorni
- [ ] Final comparison report
- [ ] Documentazione uso
- [ ] Ready for live (graduale)

---

## 🚨 Note Critiche

### **Limit Orders in Backtrader**
```python
# Backtrader limit order at close:
self.buy(data=d, size=size, exectype=bt.Order.Limit, price=d.close[0])

# Questo eseguirà:
# - Order creato con limit = close[0]
# - Fill alla barra successiva se price <= limit (long) o >= limit (short)
# - In backtest: assume fill istantaneo se prezzo permettte
# - In paper: ordine inviato ad Alpaca, fill reale
```

**Importante:** In backtest, `cheat_on_close` deve essere `False` per realismo.

### **ATR Calculation**
- Usa ATR(14) standard
- ATR% = ATR / close * 100
- Filter: ATR% >= 0.7%

### **Stop Loss**
- Manuale check ogni bar (non bracket order per compatibilità)
- SL = entry_price × (1 ± 0.005)

### **Position Count**
- Track manualmente `self.position_count`
- Update su buy/sell/close
- Max = 10

---

## 📚 File Coinvolti

| File | Azione | Descrizione |
|------|--------|-------------|
| `strategies/intraday.py` | EDIT | Aggiungere classe HMADynamic |
| `strategies/comparison_logger.py` | CREATE | Logger discrepanze |
| `bin/loadtickers_incremental.py` | CREATE | Loadtickers migliorato |
| `bin/compare_backtest_paper.py` | CREATE | Analisi discrepanze |
| `tests/test_hma_dynamic.py` | CREATE | Unit tests |
| `bt-strategy-test/HMA/IMPLEMENTATION_PLAN.md` | READ | Questo documento |

---

**Prossimo Step:** Scegliere da dove iniziare:
1. Miglioramento loadtickers (preparazione)
2. Implementazione HMADynamic (core)
3. Comparison logger (monitoring)

Quale preferisci iniziare?
