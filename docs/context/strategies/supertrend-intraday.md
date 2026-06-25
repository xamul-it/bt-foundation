# SuperTrend Intraday

## Purpose

Document the tested long-only SuperTrend intraday variant on 3-5 minute bars,
then evaluate RTH feature filters against a buy-open/sell-close benchmark.

## Strategy Rules

Implementation: `bt-core/strategies/intraday.py`, class
`SuperTrendIntraday`.

Finalization requirement: final candidates must be run as Backtrader strategies
through `bt-core/btmain.py`; standalone research scripts are not sufficient to
close the study.

Current core behavior:

- Long only for this tested variant; this is not a general strategy-analysis
  rule.
- Entry by limit order when close is above SuperTrend.
- No fixed take profit.
- Exit through dynamic SuperTrend lower band stop.
- EOD flatten 15 minutes before close.
- No new entries in the final 30 minutes.
- Trading starts 30 minutes after market open.

Important correction from the study: fixed TP was removed because it cut winners
while SuperTrend was still favorable. Exits should remain tied to SuperTrend
lines or EOD flattening.

## Data

- Provider: Alpaca SIP.
- Base bars: minute.
- Strategy timeframe: 5 minutes for the main study.
- Universe: `config-common/tickers/rth_stable_candidates_10.json`.
- Market data root: `data/` (symlink to `config-common/data/`).
- Minute Alpaca data location: `data/m/alpaca/`.
- RTH features: `bt-strategy-test/RTH_analysis/out/rth_features.parquet`.
- Main event dataset:
  `bt-strategy-test/supertrend_intraday/out/supertrend_meta_filter/supertrend_rth_event_dataset_2023_2026.parquet`.

Data note:

- Raw market data is split by provider under `data/`, including Alpaca and
  Yahoo folders.
- If required tickers are missing locally, load/backfill them with
  `bt-core/load_tickers.py`.
- 2023 minute data was backfilled with `bt-core/load_tickers.py`.
- `rth_universe_map.parquet` had unreliable `trading_date` mapping because of
  incomplete Alpaca calendar cache. The SuperTrend meta-filter script recomputes
  `trading_date` from the feature stream.

## Scripts

- `bt-strategy-test/supertrend_intraday/04_supertrend_meta_filter_study.py`
- `bt-strategy-test/supertrend_intraday/05_supertrend_period_benchmark.py`
- `bt-strategy-test/supertrend_intraday/06_supertrend_combo_filter_study.py`

Shared RTH feature builders:

- `bt-strategy-test/RTH_analysis/01_build_primitives.py`
- `bt-strategy-test/RTH_analysis/02_build_features.py`
- `bt-strategy-test/RTH_analysis/03_build_universe_map.py`

The generated feature artifacts live under `bt-strategy-test/RTH_analysis/out`.

## Main Parameter Baseline

Main tested baseline:

```text
timeframe = 5m
period = 5
multiplier = 2.0
```

Baseline means `st_no_filter`: SuperTrend strategy with no RTH filter.

## Benchmark

Primary benchmark: buy-open/sell-close over the same tradable daily universe.

Buy-and-hold is not used as the primary benchmark for this strategy.

## Key Results

Raw SuperTrend trade edge before BO-SC adjustment:

| Period | Trades | Edge/trade | Strategy sum |
|---|---:|---:|---:|
| 2023 | 7197 | +0.665 bps | +4789 bps |
| 2024 | 6358 | +0.235 bps | +1492 bps |
| 2025 | 6104 | +0.979 bps | +5978 bps |
| 2026 YTD | 2668 | -0.712 bps | -1900 bps |

Versus BO-SC:

| Period | Filter | Alpha vs BO-SC | Trade goodness | Stability |
|---|---|---:|---:|---:|
| 2024 | `st_no_filter` | +8817 bps | 5.93 | 0.0909 |
| 2024 | `rth_winrate63_bottom20` | +8459 bps | 4.50 | 0.0695 |
| 2026 YTD | `rth_logvol_z_20_top50` | +586 bps | 40.19 | 0.1527 |
| 2026 YTD | `st_no_filter` | -5293 bps | -19.19 | 0.0346 |
| 2023-2026 | `st_no_filter` | -9701 bps | 12.17 | 0.0871 |
| 2023-2026 | `rth_logvol_z_20_top50` | -7797 bps | 14.41 | 0.1113 |

Interpretation:

- SuperTrend has positive raw trade edge in several years, but raw trade edge is
  not sufficient.
- Against BO-SC, full-period alpha is negative.
- 2024 is favorable for SuperTrend relative to BO-SC.
- 2023 and 2025 are difficult because BO-SC captures strong intraday market
  drift.
- 2026 improves materially with volume filters, especially `rth_logvol_z_20`.

## Filter Findings

Tested filter families:

- Distance from SuperTrend.
- Momentum.
- Consecutive close above SuperTrend.
- Breakout.
- EMA slope and above EMA.
- VWAP.
- RTH volatility, range expansion, rvol, downside/upside, winrate.
- Statistical quantile filters.
- Pairwise feature filters.
- Tabular classifiers/regressors.
- Direction plus strength combinations.

Best current finding:

- Volume is the most useful filter family so far.
- `rth_logvol_z_20 top50` is the best simple 2026 YTD candidate.
- Direction plus strength combinations often increase edge/trade but reduce
  trade count too much and hurt alpha versus BO-SC.

## Current Assessment

Grade: `C`.

Reason:

- Useful volume signal exists.
- 2026 YTD alpha can turn positive with volume filters.
- Full-period alpha versus BO-SC remains negative.
- Execution stress remains fragile; `alpha_net_5bps` is often negative.

## Next Tests

- Treat volume as primary regime filter.
- Use direction as a soft score or light threshold, not a hard AND.
- Add intraday entry-state features: SuperTrend distance, SuperTrend slope,
  ATR/volatility, time-of-day, volume ratio, breakout/consecutive bars, VWAP/EMA
  distance.
- Consider pullback-to-SuperTrend limit entries instead of buying when already
  above SuperTrend.
- Re-rank all candidates with the three reusable indicators, not raw trade edge.
