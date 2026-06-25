# HMA Strategy - Validation Methodology

## Out-of-Sample Testing Approach

### Goal
Validate that the HMA strategy with optimal filters (atr_and_time) maintains positive expectancy on completely unseen data.

---

## Data Splits

### Training Data (Initial Optimization)
- **Period:** Random sampling from entire dataset (2016-2026)
- **Sample Size:** 50 days per symbol
- **Symbols:** 28 NASDAQ stocks (high volatility)
- **Purpose:** Find optimal filter configuration
- **Result:** atr_and_time filter with net expectancy +0.041%

### Validation Data (Out-of-Sample)
- **Period:** 2025-05-06 to 2025-07-03 (41 consecutive trading days)
- **Sample Method:** ALL days (not random) - complete coverage
- **Symbols:** Same 28 NASDAQ stocks
- **Purpose:** Validate strategy on recent, unseen market conditions
- **Why "Very Out":**
  - Training used random 50 days from 10-year period
  - Validation uses specific consecutive 2-month period
  - Zero overlap guaranteed
  - Tests recent market conditions (most relevant for live trading)

---

## Why This Validation is Strong

### 1. Temporal Out-of-Sample
- Training: Sparse random days over 10 years
- Validation: Dense consecutive days from recent period
- No temporal leakage

### 2. Market Regime Testing
- Recent period (May-Jul 2025) likely has different:
  - Volatility regime
  - Correlation structure
  - Market microstructure
- If strategy works here, it's robust to regime changes

### 3. Consecutive Days
- Random sampling can hide:
  - Streak risks (consecutive losses)
  - Regime persistence
  - Market adaptation
- Consecutive testing exposes these risks

### 4. Adequate Sample Size
- 41 days × 28 symbols × ~58 trades/day/symbol
- Expected: ~65,000+ individual trade opportunities
- Statistically significant for expectancy estimation

---

## Validation Metrics

### Primary Metric
**Net Expectancy** - Must remain ≥ +0.01% (conservative threshold)
- Training result: +0.041%
- Acceptable degradation: down to +0.01% still viable
- Failure threshold: < 0.0%

### Secondary Metrics
1. **Win Rate** - Should remain 40-50%
2. **Avg Win vs Avg Loss** - Ratio should stay > 1.5
3. **Trade Count** - Should be ~2,000-3,000 (41 days × ~58/day)
4. **Sharpe Ratio** - If calculable on daily returns

### Success Criteria
- ✅ **PASS:** Net expectancy ≥ +0.02% (50% of training)
- ⚠️  **MARGINAL:** Net expectancy +0.01% to +0.02%
- ❌ **FAIL:** Net expectancy < +0.01%

---

## Additional Validation Tests

### 1. ATR Threshold Sensitivity
**Test:** Vary ATR threshold (0.3%, 0.4%, 0.5%, 0.6%, 0.7%, 0.8%)
**Purpose:**
- Check if 0.5% is truly optimal
- Assess strategy robustness to parameter changes
- Find if there's additional edge with different threshold

**Expected Outcome:**
- Optimal threshold should be near 0.5% (±0.1%)
- Performance should degrade gracefully away from optimum
- No sudden cliffs (indicates overfitting)

### 2. Symbol Diversification
**Test:** Performance across individual symbols
**Purpose:**
- Identify if edge is concentrated in few symbols
- Check for symbol-specific overfitting
- Assess portfolio diversification benefits

**Red Flags:**
- >50% of profit from single symbol
- High variance in per-symbol expectancy
- Some symbols with large negative expectancy

### 3. Time-of-Day Analysis
**Test:** Performance by hour (10:00, 11:00, ..., 14:00)
**Purpose:**
- Validate time filter (10:00-15:00) is optimal
- Check if edge is concentrated in specific hours
- Identify potential for further time optimization

---

## Interpretation Guidelines

### If Validation Passes (≥ +0.02%)
✅ **Proceed to paper trading**
- Strategy robust and validated
- Implement with real-time execution
- Monitor for 20+ days before live capital

### If Validation Marginal (+0.01% to +0.02%)
⚠️  **Conditional proceed**
- Strategy may be viable but tight
- Require longer paper trading (30+ days)
- Consider additional optimizations:
  - Limit orders to reduce slippage
  - Parameter fine-tuning
  - Additional filters

### If Validation Fails (< +0.01%)
❌ **Do not trade**
- Strategy likely overfit to training data
- Market regime may have changed
- Options:
  - Re-optimize on more recent data
  - Try different indicators
  - Abandon HMA approach

---

## Known Risks & Limitations

### 1. Slippage Assumption (0.03%)
- Based on 300ms execution latency
- NOT validated with real fills
- Could be optimistic
- **Mitigation:** Paper trading will reveal true slippage

### 2. Commission Costs
- Not yet included in expectancy calculation
- Typical cost: $0.001-$0.002 per share
- At ~58 trades/day, could reduce edge by ~0.01%
- **Mitigation:** Add commission modeling in next step

### 3. Sample Period Length
- 41 days is good but not extensive
- Longer validation (3-6 months) would be stronger
- May miss rare events (crashes, squeezes)
- **Mitigation:** Walk-forward testing over multiple periods

### 4. Market Microstructure Changes
- Strategy developed on 2025 data
- Market microstructure can change (spreads, liquidity, rebates)
- Historical validation may not predict future
- **Mitigation:** Regular revalidation (monthly)

### 5. Capacity Constraints
- Not yet tested with realistic position sizes
- Large orders could move market (slippage)
- High-frequency trading (58/day/symbol) requires good execution
- **Mitigation:** Start with small capital, scale gradually

---

## Next Steps After Validation

### If Validation Successful

1. **Parameter Sensitivity Analysis**
   - Test robustness to small parameter changes
   - Ensure no sharp performance cliffs
   - Document acceptable parameter ranges

2. **Add Commission Modeling**
   - Include realistic commission structure
   - Recompute net expectancy
   - Ensure still profitable after all costs

3. **Walk-Forward Analysis** (optional but recommended)
   - Test on multiple non-overlapping periods
   - Check consistency across time
   - Identify if performance is deteriorating

4. **Paper Trading Setup**
   - Implement with Alpaca paper account
   - Monitor for 20-30 days
   - Track:
     - Actual slippage vs assumed
     - Fill rates
     - Real-time execution quality
     - Daily P&L vs expected

5. **Live Trading (if paper validates)**
   - Start with minimal capital ($5k-$10k)
   - Single symbol initially
   - Scale up gradually if metrics hold
   - Implement risk management:
     - Max daily loss: -2%
     - Max position size: 10% of capital
     - Max concurrent positions: 10

---

## Documentation

### Files Generated
- `validation_out_of_sample/atr_0.5.csv` - Trade-level validation results
- `validation_out_of_sample/atr_threshold_summary.csv` - Validation summary
- `atr_optimization/atr_*.csv` - Threshold sensitivity results
- `atr_optimization/atr_threshold_summary.csv` - Threshold comparison

### Reports to Generate
1. Validation summary (expectancy, win rate, trades)
2. Per-symbol breakdown
3. Time-of-day analysis
4. Comparison vs training results
5. Risk metrics (max drawdown, consecutive losses)

---

**Validation Date:** 2026-02-11
**Strategy:** HMA (period=16, inverted=True) + atr_and_time filters
**Training Result:** +0.041% net expectancy
**Validation Target:** ≥ +0.02% net expectancy (50% degradation acceptable)
