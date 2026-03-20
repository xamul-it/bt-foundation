# HMA Strategy - Complete Implementation Guide

**Date:** 2026-02-12
**Status:** Ready for Backtrader Implementation

---

## 📋 Executive Summary

Abbiamo completato l'analisi completa della strategia HMA intraday e ora siamo pronti per l'implementazione in Backtrader. Questo documento riassume tutto ciò che è stato fatto e fornisce la roadmap per il backtest finale.

---

## ✅ Analisi Completate

### 1. **HMA Strategy Validation** ✅
- **File:** `bin/HMA/COMPLETE_ANALYSIS_SUMMARY.md`
- **Risultati:**
  - HMA period=16, inverted=True
  - ATR ≥ 0.7% filter
  - Time window: 10:00-15:00 EST
  - Net expectancy: **0.189%** per trade (limit orders)
  - Win rate: 34.6%

### 2. **Limit Order Optimization** ✅
- **File:** `bin/HMA/LIMIT_ORDER_ANALYSIS_SUMMARY.md`
- **Risultati:**
  - Limit at close: **0.189%** expectancy
  - Market orders: 0.154% expectancy
  - **Miglioramento: +23%** vs market orders
  - Fill rate: 100%

### 3. **Monte Carlo con Slippage** ✅
- **File:** `bin/HMA/08-monte_carlo_with_slippage.py`
- **Risultati (conservative 0.05% slippage):**
  - Mean annual return: **16.10%**
  - 90% confidence: 7.78% - 25.45%
  - Probability profit: 100%

### 4. **Position Sizing Analysis** ✅
- **File:** `bin/HMA/POSITION_SIZING_AND_RISK_MANAGEMENT.md`
- **Risultati:**
  - ATR-based sizing is optimal
  - Formula: `position_size = (capital × risk%) / SL% × (0.7 / ATR%)`
  - Target risk: 2% per trade
  - Max position: 20% of capital

### 5. **Opening Volatility Analysis** ✅
- **File:** `bin/HMA/10-opening_volatility_analysis.py`
- **Risultati:**
  - **Opening range è il miglior predittore** (0.691 correlation)
  - Gap > 1% → 4× più setups
  - Opening range > 1% → 9× più setups
  - ATR at 10:00 → 0.616 correlation

### 6. **Realistic Portfolio Monte Carlo** ⏳
- **File:** `bin/HMA/11-monte_carlo_realistic_portfolio.py`
- **Status:** Running (100 simulations × 40 days × 28 symbols)
- **Features:**
  - Simula tutto il workflow giornaliero
  - Opening volatility ranking
  - Queue management (max 10 positions)
  - ATR-based sizing
  - Capital allocation dinamica

---

## 🎯 Strategia Finale Validata

### **Core Parameters**
```python
# HMA
period = 16
inverted = True
sl_pct = 0.005  # 0.5%

# Filters
atr_min = 0.007  # 0.7%
hour_min = 10
hour_max = 15

# Position Sizing
target_risk_per_trade = 0.02  # 2% of capital
max_position_size = 0.20  # 20% of capital
max_concurrent_positions = 10

# Execution
order_type = 'LIMIT'
limit_price = 'signal_bar_close'
slippage_assumption = 0.0005  # 0.05%
```

### **Dynamic Allocation Rules**
```python
# HIGH OPPORTUNITY (1.5× capital allocation)
if opening_range_pct > 0.68 OR gap_pct > 4.72:
    multiplier = 1.5

# LOW OPPORTUNITY (0.5× capital allocation or skip)
elif opening_range_pct < 0.37 AND gap_pct < 2.36:
    multiplier = 0.5

# NORMAL (1.0× capital allocation)
else:
    multiplier = 1.0
```

### **ATR-Based Position Sizing**
```python
def calculate_position_size(capital, atr_pct, allocation_multiplier):
    # Base size for 2% risk
    base_size = (capital * 0.02) / 0.005  # $40k for $100k capital

    # Adjust for ATR (normalize to 0.7%)
    atr_adjustment = 0.7 / max(atr_pct, 0.3)

    # Apply allocation multiplier
    position_size = base_size * atr_adjustment * allocation_multiplier

    # Bounds: 5% to 20% of capital
    return np.clip(position_size, capital * 0.05, capital * 0.20)
```

---

## 📊 Performance Attese (Con Slippage 0.05%)

### **Metriche Annuali (Proiettate)**
| Metrica | Valore | Range (90% CI) |
|---------|--------|----------------|
| **Return annuale** | **16.10%** | 7.78% - 25.45% |
| Max drawdown medio | -2% | -1% a -4% |
| Sharpe ratio | 3.5-4.0 | 3.0 - 5.4 |
| Trades/anno | ~1,100 | - |
| Trades/giorno | ~4.4 | - |
| Win rate | 34.6% | - |
| Avg win | 1.476% | - |
| Avg loss | -0.496% | - |

### **Capital Projection ($100k Initial)**
| Period | Capital Medio | Range (90% CI) |
|--------|---------------|----------------|
| 1 mese | $101,340 | $100,650 - $102,120 |
| 6 mesi | $108,350 | $104,680 - $113,600 |
| **1 anno** | **$116,100** | **$107,780 - $125,450** |

---

## 🔄 Workflow Giornaliero Completo

### **09:30 - Market Open**
```python
# Load all symbols
for symbol in watchlist:
    load_data(symbol)
    previous_close[symbol] = get_previous_close(symbol)
```

### **09:30-10:00 - Opening Analysis Phase**
```python
for symbol in watchlist:
    # Calculate opening metrics
    opening_data = get_bars(symbol, '09:30', '10:00')

    opening_range = (high - low) / low * 100
    gap = abs(open - prev_close) / prev_close * 100
    volume_ratio = opening_volume / (avg_daily_volume / 13)

    # Calculate opportunity score
    score = 0
    if opening_range > 0.68:
        score += 1.5
    if gap > 4.72:
        score += 1.0

    opportunity_scores[symbol] = score
```

### **10:00 - Capital Allocation**
```python
# Rank symbols by opportunity
symbols_ranked = sorted(watchlist, key=lambda s: opportunity_scores[s], reverse=True)

# Allocate capital proportionally to top symbols
total_score = sum(opportunity_scores[s] for s in symbols_ranked[:20])

for symbol in symbols_ranked[:20]:  # Top 20 symbols
    weight = opportunity_scores[symbol] / total_score
    capital_allocation[symbol] = total_capital * weight * 0.8  # Reserve 20%

    # Calculate ATR at 10:00
    atr_1000[symbol] = calculate_atr(get_bars_up_to('10:00'))
```

### **10:00-15:00 - Active Trading**
```python
for bar in minute_bars:  # Each minute
    for symbol in watchlist:
        # Check HMA signal
        if hma_signal_fires(symbol):
            # Verify ATR filter
            if current_atr(symbol) < 0.007:
                continue  # Skip low volatility

            # Check queue capacity
            if len(active_positions) >= MAX_POSITIONS:
                # Queue full: prioritize by opportunity score
                if should_replace_weakest_position(symbol):
                    close_position(weakest_symbol)
                else:
                    continue  # Skip this signal

            # Calculate position size
            allocated = capital_allocation[symbol]
            opportunity_mult = get_opportunity_multiplier(symbol)

            position_size = calculate_position_size(
                capital=min(allocated, available_capital),
                atr_pct=current_atr(symbol),
                allocation_multiplier=opportunity_mult
            )

            # Enter with LIMIT order at close
            enter_trade(
                symbol=symbol,
                side='long' or 'short',
                size=position_size,
                order_type='LIMIT',
                limit_price=current_bar['close'],
                sl_pct=0.005
            )
```

### **15:00-16:00 - Position Management**
```python
# Start closing positions on opposite signals
for position in active_positions:
    if opposite_signal(position.symbol):
        close_position(position, limit_price=current_bar['close'])

# At 15:59: Force close all remaining positions
if time == '15:59':
    for position in active_positions:
        close_position(position, market_order=True)
```

---

## 🔧 Implementazione Backtrader

### **Struttura Strategia**

```python
class HMADynamicStrategy(MultiTickerStrategy):
    params = (
        ('period', 16),
        ('inverted', True),
        ('sl_pct', 0.005),
        ('atr_min', 0.007),
        ('hour_min', 10),
        ('hour_max', 15),
        ('max_positions', 10),
        ('target_risk', 0.02),
        ('max_position_pct', 0.20),
    )

    def __init__(self):
        # Indicators for each data feed
        self.hma = {}
        self.atr_pct = {}

        for d in self.datas:
            self.hma[d] = HMA(d.close, period=self.p.period)
            self.atr_pct[d] = ATR_PCT(d, period=14)

        # Daily data for each symbol (for opening analysis)
        self.daily_data = {}

        # State tracking
        self.opening_scores = {}
        self.capital_allocation = {}
        self.position_count = 0

    def prenext(self):
        # Called before strategy is ready (not enough bars)
        pass

    def nextstart(self):
        # Called once when strategy first has enough bars
        self.next()

    def next(self):
        current_time = self.datas[0].datetime.time()

        # Phase 1: Opening Analysis (09:30-10:00)
        if time(9, 30) <= current_time < time(10, 0):
            self.analyze_opening()

        # Phase 2: Capital Allocation (10:00)
        elif current_time == time(10, 0):
            self.allocate_capital()

        # Phase 3: Trading (10:00-15:00)
        elif time(10, 0) <= current_time < time(15, 0):
            self.process_signals()

        # Phase 4: Close positions (15:00-16:00)
        elif time(15, 0) <= current_time < time(16, 0):
            self.close_all_positions()

    def analyze_opening(self):
        """Calculate opening metrics for all symbols"""
        for d in self.datas:
            symbol = d._name

            # Get opening range
            if len(d) >= 30:  # 30 minutes
                bars_30min = d.get(size=30)
                opening_high = max(bar.high for bar in bars_30min)
                opening_low = min(bar.low for bar in bars_30min)
                opening_range = (opening_high - opening_low) / opening_low * 100

                # Get gap
                prev_close = self.get_previous_close(symbol)
                gap = abs(d.open[0] - prev_close) / prev_close * 100 if prev_close else 0

                # Calculate opportunity score
                score = 0
                if opening_range > 0.68:
                    score += 1.5
                if gap > 4.72:
                    score += 1.0

                self.opening_scores[symbol] = score

    def allocate_capital(self):
        """Allocate capital based on opening scores"""
        # Rank symbols
        ranked = sorted(self.opening_scores.items(), key=lambda x: x[1], reverse=True)
        top_symbols = [s for s, score in ranked[:20] if score > 0]

        # Calculate allocation
        total_score = sum(self.opening_scores[s] for s in top_symbols)

        for symbol in top_symbols:
            if total_score > 0:
                weight = self.opening_scores[symbol] / total_score
                self.capital_allocation[symbol] = self.broker.get_value() * weight * 0.8

    def process_signals(self):
        """Process HMA signals and manage positions"""
        for d in self.datas:
            symbol = d._name

            # Check ATR filter
            if self.atr_pct[d][0] < self.p.atr_min:
                continue

            # Check HMA signal
            if self.p.inverted:
                signal_long = self.hma[d][-1] > self.hma[d][-2] and self.hma[d][0] < self.hma[d][-1]
                signal_short = self.hma[d][-1] < self.hma[d][-2] and self.hma[d][0] > self.hma[d][-1]
            else:
                signal_long = self.hma[d][-1] < self.hma[d][-2] and self.hma[d][0] > self.hma[d][-1]
                signal_short = self.hma[d][-1] > self.hma[d][-2] and self.hma[d][0] < self.hma[d][-1]

            # Close opposite position
            pos = self.getposition(d)
            if pos.size > 0 and signal_short:
                self.close(data=d, exectype=bt.Order.Limit, price=d.close[0])
            elif pos.size < 0 and signal_long:
                self.close(data=d, exectype=bt.Order.Limit, price=d.close[0])

            # Open new position
            if signal_long and pos.size == 0:
                if self.position_count < self.p.max_positions:
                    size = self.calculate_position_size(d, symbol)
                    if size > 0:
                        self.buy(data=d, size=size, exectype=bt.Order.Limit,
                                price=d.close[0], exectype=bt.Order.StopLimit,
                                plimit=d.close[0], stopprice=d.close[0] * (1 - self.p.sl_pct))

            elif signal_short and pos.size == 0:
                if self.position_count < self.p.max_positions:
                    size = self.calculate_position_size(d, symbol)
                    if size > 0:
                        self.sell(data=d, size=size, exectype=bt.Order.Limit,
                                 price=d.close[0], exectype=bt.Order.StopLimit,
                                 plimit=d.close[0], stopprice=d.close[0] * (1 + self.p.sl_pct))

    def calculate_position_size(self, data, symbol):
        """Calculate ATR-based position size"""
        # Get allocated capital
        allocated = self.capital_allocation.get(symbol, self.broker.get_value() * 0.1)
        available = min(allocated, self.broker.get_cash())

        # Base size for target risk
        base_size = (available * self.p.target_risk) / self.p.sl_pct

        # ATR adjustment
        atr_pct = self.atr_pct[data][0]
        atr_adj = 0.7 / max(atr_pct, 0.3)

        # Opportunity multiplier
        opening_score = self.opening_scores.get(symbol, 0)
        if opening_score > 1.5:
            opportunity_mult = 1.5
        elif opening_score < 0.5:
            opportunity_mult = 0.5
        else:
            opportunity_mult = 1.0

        position_value = base_size * atr_adj * opportunity_mult

        # Bounds
        max_value = available * self.p.max_position_pct
        position_value = min(position_value, max_value)

        # Convert to shares
        shares = int(position_value / data.close[0])

        return shares

    def close_all_positions(self):
        """Force close all positions before market close"""
        for d in self.datas:
            pos = self.getposition(d)
            if pos.size != 0:
                self.close(data=d)  # Market order at close
```

---

## 📝 Next Steps

### **1. Completare Monte Carlo Realistico** ⏳
- **Status:** Running (~80 minuti)
- **Output:** `bin/HMA/monte_carlo_portfolio/`
- **Metriche attese:** Return distribution con allocazione dinamica

### **2. Implementare Strategia in Backtrader**
```bash
# Create strategy file
vim strategies/hma_dynamic.py

# Copy implementation from this guide
# Add to strategies/__init__.py
```

### **3. Run Backtest Completo**
```bash
python btmain.py \
    --strat hma_dynamic.HMADynamicStrategy \
    --ticker AAPL,MSFT,NVDA,TSLA,META,AMZN,GOOGL \
    --fromdate 2024-01-01 \
    --todate 2025-12-31 \
    --timeframe minute \
    --provider alpaca \
    --mode backtest \
    --commission CommNone \
    --amount 100000 \
    --stratargs "period=16 inverted=True sl_pct=0.005"
```

### **4. Analizzare Risultati**
- Confronta con Monte Carlo
- Verifica slippage assunto
- Check position sizing effectiveness
- Validate opening volatility allocation

### **5. Paper Trading (se backtest OK)**
```bash
python btmain.py \
    --strat hma_dynamic.HMADynamicStrategy \
    --mode paper \
    --live \
    --alpaca-mode proxy
```

### **6. Live Trading (se paper trading OK)**
- Start con capitale ridotto ($10-25k)
- Max 5 positions (invece di 10)
- Monitor per 30 giorni
- Scale gradually

---

## 📚 File di Riferimento

| File | Descrizione |
|------|-------------|
| `COMPLETE_ANALYSIS_SUMMARY.md` | Journey completo strategia HMA |
| `LIMIT_ORDER_ANALYSIS_SUMMARY.md` | Analisi ordini limite |
| `POSITION_SIZING_AND_RISK_MANAGEMENT.md` | Position sizing ATR-based |
| `COMPLETE_IMPLEMENTATION_GUIDE.md` | Questo documento |
| `01-hma_backtest.py` | Script iniziale backtest |
| `06-hma_limit_order_optimization.py` | Test ordini limite |
| `07-monte_carlo_simulation.py` | Monte Carlo base |
| `08-monte_carlo_with_slippage.py` | Monte Carlo con slippage |
| `09-position_sizing_analysis.py` | Analisi position sizing |
| `10-opening_volatility_analysis.py` | Analisi volatilità apertura |
| `11-monte_carlo_realistic_portfolio.py` | Monte Carlo realistico |

---

## ✅ Checklist Pre-Backtest

- [x] Strategia validata (HMA 16, inverted, ATR 0.7%)
- [x] Limit orders analysis (0.189% expectancy)
- [x] Slippage assumptions (0.05% conservative)
- [x] Position sizing definito (ATR-based)
- [x] Opening volatility analysis (0.691 correlation)
- [x] Dynamic allocation rules (HIGH/LOW opportunity)
- [x] Queue management (max 10 positions)
- [ ] Monte Carlo realistico (in corso)
- [ ] Implementazione Backtrader
- [ ] Backtest 2024-2025
- [ ] Paper trading 30 giorni
- [ ] Live trading graduale

---

**Conclusione:** Tutti i componenti sono stati analizzati e validati. Siamo pronti per l'implementazione in Backtrader una volta completato il Monte Carlo realistico.
