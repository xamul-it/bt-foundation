# Opening Range Breakout (ORB) Strategy

## Strategy Overview

Opening Range Breakout è una strategia intraday classica che sfrutta il breakout dell'opening range.

### Logic

1. **Opening Range (OR)**: High/Low dei primi N minuti (default: 30 min, 9:30-10:00)
2. **Breakout**: Dopo l'OR period, aspetta il breakout:
   - **Long**: Se close > OR_high
   - **Short**: Se close < OR_low
3. **Take Profit**: Entry + (multiplier × OR_size)
4. **Stop Loss**: Lato opposto dell'OR (OR_low per long, OR_high per short)
5. **Una posizione al giorno**
6. **Close a EOD**: Se non ha toccato TP/SL entro le 16:00

### Vantaggi

✅ **Logica chiara e oggettiva**
- OR è definito in modo preciso
- TP/SL basati su OR size (no ottimizzazione arbitraria)

✅ **Funziona in trending markets**
- Cattura movimenti direzionali post-apertura
- Breakout confermato da volume istituzionale

✅ **No problema "ottimistico vs realistico"**
- Entry e exit sono chiari
- TP/SL verificabili tick-by-tick

✅ **Testabile su lunghi periodi**
- 10-20 anni di dati disponibili
- Pattern consolidato e noto

### Svantaggi

❌ **Falsi breakout**
- In range-bound markets, molti whipsaw
- Soluzione: filtri (volume, min OR size)

❌ **Richiede volatilità**
- Se OR troppo stretto → TP troppo vicino
- Soluzione: min OR size filter

❌ **Solo long bias in bull markets**
- Short può essere meno profittevole
- Soluzione: test separati long/short

## Parameters

### OR Duration
- **15 min**: Più reattivo, OR più stretto
- **30 min** (default): Bilanciato
- **60 min**: OR più robusto, meno falsi breakout

### TP Multiplier
- **1x**: Conservativo, più hit
- **2x** (default): Bilanciato
- **3x**: Aggressivo, meno hit ma maggior reward

### Min OR Size Filter
- **0%** (default): No filter
- **0.2%**: Evita giorni flat
- **0.5%**: Solo giorni volatili

## Usage

### Basic Test

```bash
cd bt-strategy-test/ORB
source ../../bt-core/.venv/bin/activate

# Test single symbol
python 01-orb_backtest.py --symbol AAPL

# Test all symbols
python 01-orb_backtest.py

# Analyze results
python 02-analyze_orb.py
```

### Parameter Testing

```bash
# Test 15-minute OR
python 01-orb_backtest.py --or-minutes 15 --output orb_15min.csv

# Test 3x TP multiplier
python 01-orb_backtest.py --tp-multiplier 3.0 --output orb_tp3x.csv

# Test with min OR filter
python 01-orb_backtest.py --min-or-pct 0.002 --output orb_minOR.csv
```

### Grid Search

```bash
# Test multiple parameter combinations
for OR in 15 30 60; do
  for TP in 1.0 1.5 2.0 3.0; do
    python 01-orb_backtest.py \
      --or-minutes $OR \
      --tp-multiplier $TP \
      --output orb_OR${OR}_TP${TP}.csv
  done
done

# Analyze all results
for f in orb_OR*.csv; do
  echo "=== $f ==="
  python 02-analyze_orb.py --file $f | grep "Expectancy:"
done
```

## Expected Performance

### Hypothesis
- **Expectancy**: +0.1% - +0.3% per trade
- **Win rate**: 40-60% (TP hit)
- **Win/Loss ratio**: 2:1 - 3:1 (TP multiplier dependent)
- **Trades/day/symbol**: 0-1 (not every day has breakout)

### Best Case Scenarios
- **Trending days**: Post-earnings, news, market trend
- **High volatility**: OR > 0.5%
- **Strong momentum**: Quick breakout with volume

### Worst Case Scenarios
- **Range-bound days**: Whipsaw, falsi breakout
- **Low volatility**: OR < 0.2%, TP troppo vicino
- **Choppy markets**: Multiple reversals

## Next Steps

1. **Backtest su tutti i simboli** (28 assets)
2. **Parameter optimization** (OR duration, TP multiplier)
3. **Add filters**:
   - Volume confirmation
   - ATR-based OR size filter
   - Time-of-day filters (evita ultimo ora?)
4. **Walk-forward testing** su finestre temporali
5. **Paper trading** se expectancy > 0.1%

## Files

- `01-orb_backtest.py`: Main backtest script
- `02-analyze_orb.py`: Results analysis
- `orb_results.csv`: Output file (append mode)
- `README.md`: This file
