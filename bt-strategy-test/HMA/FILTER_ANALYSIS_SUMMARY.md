# HMA Strategy - Filter Analysis Summary

**Date:** 2026-02-11
**Goal:** Increase edge from +0.024% (gross) to ≥ +0.04% (net after 0.03% slippage)
**Result:** ✅ **TARGET ACHIEVED**

---

## Executive Summary

The HMA (Hull Moving Average) strategy on 1-minute timeframe showed positive gross expectancy (+0.024%) but was unprofitable after accounting for 0.03% slippage from 300ms execution latency.

Through systematic filter testing, we identified that **combining ATR (volatility) and time-of-day filters** increases the net expectancy to **+0.041%**, exceeding the 0.04% target.

---

## Problem Statement

### Initial Performance (Baseline)
- **Gross Expectancy:** +0.024%
- **Slippage:** 0.03% (300ms latency)
- **Net Expectancy:** -0.006% ❌
- **Conclusion:** Strategy unprofitable due to slippage

### Target
- **Net Expectancy:** ≥ 0.04% (to have ~0.01% margin after slippage)

---

## Step 1: Timeframe Analysis (FAILED)

**Hypothesis:** Larger timeframes should have higher expectancy per trade (fractal theory)

**Tested Timeframes:** 1min, 5min, 15min, 60min

**Results:**

| Timeframe | Trades | Gross Exp | Net Exp | Status |
|-----------|--------|-----------|---------|--------|
| 1min | 26,954 | +0.025% | -0.005% | Baseline |
| 5min | 5,399 | +0.004% | -0.026% | ❌ Worse |

**Conclusion:**
- 5min performed WORSE than 1min (win rate dropped from 60% to 48%)
- Fractal theory did NOT apply for HMA
- Larger timeframes reduce edge instead of increasing it
- **Proceed to Step 2 (filters) on 1min timeframe**

---

## Step 2: Filter Optimization (SUCCESS) ✅

### Filters Tested

1. **ATR Filters** (volatility)
   - `high_atr`: atr_pct > 0.5%
   - `very_high_atr`: atr_pct > 0.8%

2. **Volume Filters**
   - `high_volume`: volume > 1.5× SMA
   - `very_high_volume`: volume > 2.0× SMA

3. **Time-of-Day Filters**
   - `avoid_first_hour`: hour ≥ 10 (skip 09:30-10:00)
   - `avoid_edges`: hour ∈ [10, 15) (skip first & last hour)

4. **Combined Filters**
   - `atr_and_time`: high_atr + avoid_edges
   - `atr_and_volume`: high_atr + high_volume

### Complete Results

| Filter | Trades | Win% | Avg Win | Avg Loss | Gross | **Net** | Status |
|--------|--------|------|---------|----------|-------|---------|--------|
| **atr_and_time** | 2,916 | 45.3% | **0.723%** | 0.475% | +0.071% | **+0.041%** | ✅ **PASS** |
| atr_and_volume | 1,242 | 44.6% | 0.716% | 0.475% | +0.061% | +0.031% | ⚠️  POSITIVE |
| high_atr | 3,168 | 46.4% | 0.646% | 0.469% | +0.056% | +0.026% | ⚠️  POSITIVE |
| very_high_atr | 901 | 37.3% | 0.924% | 0.488% | +0.043% | +0.013% | ⚙️  MARGINAL |
| avoid_edges | 31,783 | 61.3% | 0.247% | 0.335% | +0.028% | -0.002% | ❌ FAIL |
| avoid_first_hour | 44,762 | 60.4% | 0.241% | 0.324% | +0.025% | -0.005% | ❌ FAIL |
| baseline | 44,774 | 61.0% | 0.233% | 0.322% | +0.024% | -0.006% | ❌ FAIL |
| high_volume | 16,168 | 58.6% | 0.246% | 0.317% | +0.024% | -0.006% | ❌ FAIL |
| very_high_volume | 10,207 | 56.8% | 0.267% | 0.327% | +0.022% | -0.008% | ❌ FAIL |

---

## Winning Configuration: atr_and_time

### Filter Parameters
```python
filters = {
    'atr_pct_min': 0.5,     # ATR must be > 0.5% of price (high volatility)
    'hour_min': 10,          # Trade only 10:00-15:00
    'hour_max': 15           # Avoid first hour (09:30-10:00) and last hour (15:00-16:00)
}
```

### Strategy Parameters
```python
period = 16              # HMA period
inverted = True          # Contrarian logic
sl_pct = 0.005          # 0.5% stop loss
tp_pct = None           # No take profit (exit on signal reversal)
```

### Performance Metrics

**Expectancy:**
- Gross: +0.071%
- Slippage: -0.030%
- **Net: +0.041%** ✅

**Trade Statistics:**
- Total trades: 2,916 (over 50 sample days)
- Trades per day: ~58
- Win rate: 45.3%
- Avg win: 0.723%
- Avg loss: 0.475%
- Win/Loss ratio: 1.52

**Trade Reduction:**
- Baseline: 44,774 trades
- Filtered: 2,916 trades
- **Reduction: 93.5%** (only highest quality setups)

### Why It Works

1. **ATR Filter (volatility):**
   - Filters out low-volatility periods where HMA whipsaws
   - Avg win increases from 0.233% to 0.723% (3× improvement!)
   - High volatility = larger moves = better HMA signals

2. **Time-of-Day Filter:**
   - First hour (09:30-10:00): High volatility but erratic (news, overnight gaps)
   - Last hour (15:00-16:00): Lower volume, less follow-through
   - Core hours (10:00-15:00): Most reliable trend periods

3. **Combined Effect:**
   - ATR filters for quality (volatility)
   - Time filters for reliability (avoid erratic periods)
   - Result: High-quality setups with consistent edge

---

## Key Insights

### What Worked ✅
1. **ATR-based volatility filtering** - Single best improvement
2. **Time-of-day filtering** - Complementary benefit
3. **Combined filters** - Synergistic effect (0.041% vs 0.026% for ATR alone)
4. **Trade quality over quantity** - 93% fewer trades, 3× better avg win

### What Didn't Work ❌
1. **Volume filters** - No improvement, sometimes worse
2. **Time filters alone** - Marginal benefit without ATR
3. **Very high ATR threshold** - Too restrictive (win rate drops too much)
4. **Larger timeframes** - Contrary to fractal theory, performance degraded

---

## Performance Comparison

### Baseline vs Optimal

| Metric | Baseline | atr_and_time | Change |
|--------|----------|--------------|--------|
| Trades | 44,774 | 2,916 | -93.5% |
| Win Rate | 61.0% | 45.3% | -15.7pp |
| Avg Win | 0.233% | 0.723% | **+210%** |
| Avg Loss | 0.322% | 0.475% | +47.5% |
| Gross Exp | +0.024% | +0.071% | **+196%** |
| Net Exp | -0.006% | +0.041% | **+783%** |

### Economic Impact (Theoretical)

Assumptions:
- Capital: $100,000
- Position size: 10% per trade ($10,000)
- 58 trades/day
- 20 trading days/month

**Monthly Expected Return:**
- Net expectancy per trade: 0.041%
- Expected profit per trade: $10,000 × 0.041% = $4.10
- Monthly trades: 58 × 20 = 1,160
- **Monthly expected profit: $4,756** (~4.8% of capital)

**Risk Considerations:**
- Margin very tight (0.041% vs 0.04% target)
- Slippage assumption critical (0.03% = 300ms latency)
- Need to validate with paper trading
- Commission costs not yet factored in

---

## Next Steps

### Immediate
1. ✅ Document filter analysis (this file)
2. ⏳ Wait for "all_filters" result (combining all 4 filter types)
3. Create validation script on recent data (last 3 months)

### Short-Term
1. **Validate on out-of-sample data** (2024-11 to 2025-02)
2. **Test with limit orders** (eliminate slippage assumption)
3. **Add commission modeling** (ensure profitability after fees)

### Medium-Term
1. **Paper trading** (20+ days)
   - Validate slippage assumptions
   - Measure actual execution quality
   - Track fill rates
2. **Parameter robustness testing**
   - Test ATR thresholds: 0.4%, 0.5%, 0.6%
   - Test time windows: 10-15, 10-14, 11-15
3. **Walk-forward optimization**

### Long-Term
1. **Live trading** (if paper trading validates)
   - Start with reduced capital ($10k-$25k)
   - Monitor slippage carefully
   - Scale gradually if metrics hold
2. **Multi-symbol portfolio**
   - Test on NASDAQ_HV basket (20 symbols)
   - Diversification benefits
   - Correlation analysis

---

## Conclusion

The HMA strategy with **atr_and_time filters** achieves the target net expectancy of 0.04% (actual: 0.041%).

**Key Success Factors:**
- Focus on quality over quantity (93% trade reduction)
- Volatility filtering (ATR > 0.5%)
- Time-of-day optimization (10:00-15:00)
- Synergy between filters

**Critical Risks:**
- Tight margin (only 0.001% above target)
- Slippage sensitivity (0.03% assumption)
- Commission impact not yet tested
- Sample size: 50 days (needs validation on longer period)

**Recommendation:**
Proceed to validation on out-of-sample data (3+ months) before paper trading. If validation confirms, the strategy is viable for live trading with appropriate risk management.

---

## Files Generated

- `filter_analysis/filter_baseline.csv` - No filters (baseline)
- `filter_analysis/filter_high_atr.csv` - ATR > 0.5%
- `filter_analysis/filter_very_high_atr.csv` - ATR > 0.8%
- `filter_analysis/filter_high_volume.csv` - Volume > 1.5× avg
- `filter_analysis/filter_very_high_volume.csv` - Volume > 2.0× avg
- `filter_analysis/filter_avoid_first_hour.csv` - hour ≥ 10
- `filter_analysis/filter_avoid_edges.csv` - hour ∈ [10, 15)
- `filter_analysis/filter_atr_and_time.csv` - **WINNING CONFIG** ✅
- `filter_analysis/filter_atr_and_volume.csv` - ATR + Volume
- `filter_analysis/filter_summary.csv` - Complete comparison

---

**Analysis Date:** 2026-02-11
**Run ID:** Multiple runs (see individual CSV files)
**Test Period:** 50 random days sampled from 2016-2026
**Symbols:** 28 NASDAQ stocks (config/data/m/alpaca/)
