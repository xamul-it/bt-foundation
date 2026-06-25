# HMADynamic Backtest - Issues Identified

## Test Run Summary

**Date:** 2026-02-12
**Period:** 2025-05-01 to 2025-05-31 (1 month)
**Symbols:** AAPL, MSFT
**Data:** Minute bars (Alpaca)

## Results

- **Total Trades:** 1 (❌ Expected: 60-90)
- **Return:** 0.18% (❌ Expected: ~1-2%)
- **SQN:** 0
- **Sharpe:** None

## Critical Issues Found

### ❌ Issue 1: Opening Analysis NON Funziona

**Evidence:**
```
WARNING: AAPL No opening metrics (late start?), using default allocation (1.0x)
WARNING: MSFT No opening metrics (late start?), using default allocation (1.0x)
```

**Repeated every day** - Opening analysis NEVER executes successfully.

**Possible Causes:**
1. Time filter check non funziona correttamente
2. `get_current_time_decimal()` returns wrong values
3. Data timestamps non sono in EST
4. Opening bars (09:30-10:00) non presenti nei dati

**Impact:** CRITICO - Allocation sempre default (1.0×), no diversificazione

---

### ❌ Issue 2: Pochissimi Trade

**Evidence:**
- Solo 1 trade in 1 mese
- Nessun "LONG:" o "SHORT:" log (except 1)

**Possible Causes:**
1. ATR filter troppo stretto (0.7% minimum)
2. HMA signals non generati
3. Time filter blocca entries
4. Position sizing calculation fails

**Impact:** CRITICO - Strategia non opera

---

### ❌ Issue 3: Capital Allocation Ripetuta

**Evidence:**
```
Capital allocated for 2 symbols (repeated many times per day)
```

**Cause:** `allocation_done` flag gets reset but allocation happens multiple times

**Impact:** MINORE - Performance overhead

---

## Debugging Steps Needed

### 1. Verify Data Timestamps

**Check se dati hanno bars 09:30-10:00:**
```bash
head -100 config/data/m/alpaca/AAPL.csv | grep "09:3" | head -10
head -100 config/data/m/alpaca/AAPL.csv | grep "10:0" | head -10
```

**Check timezone:**
```python
df = pd.read_csv('config/data/m/alpaca/AAPL.csv')
print(df['timestamp'].head(50))
# Should be in EST/EDT
```

---

### 2. Add Debug Logging

**In `analyze_opening()`:**
```python
def analyze_opening(self, d):
    time_decimal = self.get_current_time_decimal(d)

    # DEBUG
    logger.info(f"analyze_opening called: {d._name}, time={time_decimal:.2f}, "
                f"datetime={d.datetime.datetime()}")

    if self.p.opening_start <= time_decimal < self.p.opening_end:
        logger.info(f"{d._name} IN opening period, collecting bar")
        # ... collect bar
```

**In `next()`:**
```python
time_decimal = self.get_current_time_decimal(d)
logger.debug(f"{d._name} next() time={time_decimal:.2f}")

# Before HMA signal check
if len(hma) >= 2:
    atr_pct = atr[0] / d.close[0]
    logger.debug(f"{d._name} HMA={hma[0]:.2f}, ATR%={atr_pct*100:.2f}%, "
                 f"filter={atr_pct >= self.p.atr_min}")
```

---

### 3. Check get_current_time_decimal()

**Verify correct calculation:**
```python
def get_current_time_decimal(self, d):
    dt = d.datetime.datetime()
    hour = dt.hour
    minute = dt.minute

    # DEBUG
    time_dec = hour + minute / 60.0
    logger.debug(f"{d._name} time_decimal: {dt} → {time_dec:.2f}")

    return time_dec
```

**Possible issue:** Data is in UTC, not EST!
- UTC 14:30 → EST 09:30 (need conversion)

---

### 4. Verify ATR Filter

**Check if ATR values reasonable:**
```python
# In next(), before ATR filter
if atr[0] > 0:
    atr_pct = atr[0] / d.close[0]
    logger.info(f"{d._name} ATR={atr[0]:.2f}, price={d.close[0]:.2f}, "
                f"ATR%={atr_pct*100:.2f}%, min={self.p.atr_min*100:.2f}%")

    if atr_pct < self.p.atr_min:
        logger.info(f"{d._name} SKIP: ATR too low")
```

**Test with lower ATR threshold:**
```bash
--stratargs "atr_min=0.003"  # 0.3% instead of 0.7%
```

---

## Quick Fixes to Test

### Fix 1: Lower ATR Threshold

```bash
backtrader/bin/python btmain.py \
    --strat intraday_hma_dynamic.HMADynamic \
    --stratargs "atr_min=0.003" \
    --ticker AAPL,MSFT \
    --fromdate 2025-05-01 --todate 2025-05-31 \
    --timeframe minutes --provider alpaca \
    --benchmark ^GSPC
```

**Expected:** More trades

---

### Fix 2: Add Debug Logging

**Modify strategy to log:**
- Time decimal for every bar
- Opening analysis attempts
- ATR values
- HMA signal checks

**Re-run with --debug:**
```bash
backtrader/bin/python btmain.py \
    --strat intraday_hma_dynamic.HMADynamic \
    --ticker AAPL \
    --fromdate 2025-05-01 --todate 2025-05-02 \  # 1 day only
    --timeframe minutes --provider alpaca \
    --benchmark ^GSPC \
    --debug
```

**Expected:** Clear logs showing what's happening

---

### Fix 3: Check Timezone

**Alpaca data should be EST, verify:**
```python
# Test script
import pandas as pd
df = pd.read_csv('config/data/m/alpaca/AAPL.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])
print("First 50 rows:")
print(df.head(50)[['timestamp', 'open', 'high', 'low', 'close']])
print("\nOpening period (09:30-10:00):")
opening = df[(df['timestamp'].dt.hour == 9) & (df['timestamp'].dt.minute >= 30)]
opening = opening.append(df[(df['timestamp'].dt.hour == 10) & (df['timestamp'].dt.minute == 0)])
print(opening[['timestamp', 'open', 'high', 'low', 'close']])
```

---

## Root Cause Hypothesis

### Most Likely: Timezone Issue

**Hypothesis:**
- Alpaca data in **UTC**
- Strategy expects **EST**
- 09:30 EST = 14:30 UTC
- Strategy looking for 09:30 UTC (wrong time!)

**Solution:**
- Convert timestamps to EST in data loading
- Or adjust time filters to UTC
- Or use timezone-aware datetime in get_current_time_decimal()

---

### Second Likely: ATR Filter Too Strict

**Hypothesis:**
- 0.7% ATR is high threshold
- Most bars filtered out
- Very few setups pass

**Solution:**
- Lower to 0.3-0.5%
- Or make it a percentile-based filter

---

## Next Steps

1. ✅ **Quick test with lower ATR** (5 min)
   ```bash
   --stratargs "atr_min=0.003"
   ```

2. 🔜 **Add debug logging** (15 min)
   - Log time_decimal
   - Log ATR values
   - Log opening analysis attempts

3. 🔜 **Verify data timezone** (10 min)
   - Check first 100 rows of AAPL.csv
   - Verify 09:30-10:00 bars exist

4. 🔜 **Fix timezone if needed** (30 min)
   - Convert UTC → EST in get_current_time_decimal()
   - Or adjust time filters

5. 🔜 **Re-test** (10 min)
   - Should generate 60-90 trades
   - Should have opening metrics

---

## Monte Carlo Status

**Run 90/100** - Finishing soon

Will provide comparison when backtest is fixed.

---

## Summary

🚨 **Strategy NOT working in backtest**
- Opening analysis fails (timezone?)
- Very few trades (ATR filter?)
- Need debugging to identify root cause

**Priority:** Fix timezone issue first, then re-test.
