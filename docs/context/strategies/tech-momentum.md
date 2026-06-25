# Tech Momentum

## Purpose

Study the QuantConnect draft in `bt-strategy-test/techmomentum/main.py` and
test whether a momentum rotation over a broader Nasdaq universe is more robust
than the hand-picked original universe.

## Source Strategy

Original draft: `bt-strategy-test/techmomentum/main.py`

Original behavior:

- Universe: `AMD`, `TSLA`, `AMZN`, `AAPL`, `SPXL`.
- Data: daily bars.
- Date range in source draft: 2014-01-01 to 2020-09-11.
- Momentum score: `ret_21 + ret_63 + ret_126`.
- Rebalance: weekly by default.
- Portfolio: 100% in the single highest-score asset.
- Direction: long-only for the tested variants.

Important interpretation: despite the class name, the draft is relative
momentum, not full dual momentum. It does not move to cash when all scores are
negative.

## Research Questions

- Does the signal still work outside the hand-picked original universe?
- Does top-1 concentration create avoidable instability?
- Do Nasdaq leaders naturally rise to the top of the ranking?
- Are there unstable high-momentum names that damage the strategy?
- Does weekly rebalancing add value versus monthly rebalancing?
- How does capacity change portfolio construction when the position becomes too
  large for a single asset?

## Data

- Provider for research harness: Yahoo daily.
- Data root: `data/d/yahoo/`.
- Nasdaq universe file: `config-common/tickers/NASDAQ_100_US.json`.
- Universe file contains 91 tickers; 90 were available locally.
- Missing Nasdaq ticker during the study: `ANSS`.
- Backfilled during study: `QQQ`, `SPXL` with `bt-core/load_tickers.py`.
- Study range: 2014-01-01 to 2026-06-18.

Backfill command used:

```bash
cd bt-core
.venv/bin/python load_tickers.py --provider yahoo --ticker QQQ,SPXL --fromdate 2000-01-01 --timeframe d --data ../data
```

## Research Harness

Script:

- `bt-strategy-test/techmomentum/analyze_tech_momentum.py`

Output folders:

- Weekly research outputs: `bt-strategy-test/techmomentum/out_weekly/`.
- Monthly research outputs: `bt-strategy-test/techmomentum/out_monthly/`.

Execution model:

- Score uses prior adjusted close to avoid lookahead.
- Execution approximation uses adjusted open-to-open returns.
- Research harness applies 5 bps cost per 1.0 turnover.

Main command:

```bash
bt-core/.venv/bin/python bt-strategy-test/techmomentum/analyze_tech_momentum.py \
  --start 2014-01-01 \
  --end 2026-06-18 \
  --frequency monthly \
  --cost-bps 5
```

## Benchmarks

- Primary benchmark for this daily rotation study: `QQQ`.
- Supporting benchmarks: `SPY`, equal-weight local Nasdaq universe.

Unlike intraday studies, buy-open/sell-close is not the main benchmark here
because this is a multi-day daily-bar rotation strategy.

## Key Research Results

Monthly rebalance, 2014-01-01 to 2026-06-18:

| Variant | Total return | CAGR | Ann. vol | Sharpe 0rf | Max drawdown | Alpha net 5bps vs QQQ |
|---|---:|---:|---:|---:|---:|---:|
| Original top1 | 288.19x | 57.62% | 58.24% | 1.073 | -70.81% | +36899 bps |
| Original no ETF top1 | 178.86x | 51.72% | 58.52% | 1.005 | -71.57% | +32345 bps |
| Nasdaq top3 | 71.52x | 41.05% | 37.69% | 1.104 | -47.75% | +10959 bps |
| Nasdaq top3 stable filter | 55.19x | 38.19% | 36.05% | 1.081 | -47.67% | +7648 bps |
| Nasdaq top5 | 38.26x | 34.27% | 32.11% | 1.081 | -40.49% | +2372 bps |
| Nasdaq top1 | 9.85x | 21.10% | 58.74% | 0.620 | -82.83% | +4457 bps |
| Equal-weight Nasdaq | 9.90x | 21.14% | 19.24% | 1.096 | -30.55% | -14593 bps |
| QQQ | 8.30x | 19.61% | 21.28% | 0.950 | -36.69% | n/a |

Weekly rebalance, same range:

| Variant | Total return | CAGR | Ann. vol | Sharpe 0rf | Max drawdown | Alpha net 5bps vs QQQ |
|---|---:|---:|---:|---:|---:|---:|
| Original top1 | 126.09x | 47.55% | 56.95% | 0.968 | -70.48% | +27751 bps |
| Nasdaq top3 | 45.88x | 36.20% | 36.68% | 1.028 | -57.04% | +6088 bps |
| Nasdaq top5 | 37.27x | 34.00% | 31.12% | 1.099 | -47.03% | +1719 bps |
| Nasdaq top1 | 13.56x | 23.99% | 58.46% | 0.660 | -72.81% | +7174 bps |
| QQQ | 8.30x | 19.61% | 21.28% | 0.950 | -36.69% | n/a |

## Period Stability

Monthly `nasdaq_top3`:

| Period | Total return | CAGR | Max drawdown | Alpha net 5bps vs QQQ |
|---|---:|---:|---:|---:|
| 2014-01-01 to 2020-09-11 | 12.66x | 47.81% | -36.07% | +8575 bps |
| 2021-01-01 to 2022-12-31 | 0.05x | 2.64% | -47.75% | +169 bps |
| 2023-01-01 to 2026-06-18 | 2.23x | 40.39% | -47.67% | -727 bps |
| Full period | 71.52x | 41.05% | -47.75% | +10959 bps |

Interpretation:

- Top1 is not the recommended portfolio construction. It has very high
  concentration risk and extreme drawdowns.
- Top3 monthly is the best current balance of return, diversification, and
  stability.
- Top5 reduces drawdown further but gives up too much return in the current
  test.
- The stable filter helps drawdown only modestly and reduces return.
- Monthly rebalance is better than weekly in this study.

## April 2026 Spike Sensitivity

The final sample includes a visible performance spike starting in April 2026.
For the Backtrader acceptance run, the April 2026 basket was `AMAT`, `FANG`,
`LRCX`.

Backtrader `monthly top3 reserve10` sensitivity:

| Sample end | Final equity multiple | CAGR | Ann. vol | Sharpe 0rf | Max drawdown |
|---|---:|---:|---:|---:|---:|
| 2026-03-31 | 19.84x | 27.65% | 30.58% | 0.953 | -45.35% |
| 2026-04-30 | 28.07x | 31.08% | 30.80% | 1.035 | -45.35% |
| 2026-05-29 | 32.61x | 32.44% | 31.09% | 1.061 | -45.35% |
| 2026-06-18 | 33.31x | 32.50% | 31.28% | 1.058 | -45.35% |

Strict March output folder:

- `bt-core/out/tech_momentum/TechMomentum/techmomentum_monthly_top3_reserve10_to_20260331_strict/`

April 2026 contributed a large part of the late-sample acceleration: the
Backtrader strategy gained `+41.46%` during April versus `+15.08%` for `QQQ`.
Cutting the sample at 2026-03-31 reduces the Backtrader CAGR from `32.50%` to
`27.65%` and Sharpe from `1.058` to `0.953`, while max drawdown is unchanged.

Interpretation: removing the April spike weakens the headline result, but does
not invalidate the signal. The strategy still materially beats `QQQ` through
March 2026, but the post-March move is a meaningful part of the recent
outperformance and should be treated as a late-sample event sensitivity.

## Selection Behavior

Most frequently selected names in monthly Nasdaq top3:

| Ticker | Rebalance count |
|---|---:|
| TSLA | 26 |
| AXON | 25 |
| PLTR | 23 |
| TTD | 22 |
| PDD | 21 |
| DXCM | 19 |
| FANG | 18 |
| NFLX | 17 |
| AMAT | 13 |
| META | 13 |

This confirms that the broader Nasdaq ranking naturally selects high-momentum
tech/growth names, but it also selects volatile names such as `PLTR`, `PDD`,
`TTD`, and `DXCM`. The portfolio therefore needs diversification or liquidity
rules; top1 alone is too brittle.

## Capacity Notes

Capacity-aware variants were tested with a 1% ADV20 cap:

- At 10M notional, capacity-aware monthly behavior is close to concentrated
  top momentum and remains fully invested.
- At 100M notional, the strategy starts spreading across roughly 7-8 names and
  holds about 30% cash on average under the 1% ADV cap.
- At 1B notional, the strategy is mostly capacity constrained and not useful in
  the current form.

Conclusion: top1 is a useful signal baseline, but not a scalable portfolio
construction rule. Top3 is the minimum practical construction for this study;
larger capital requires capacity-aware top-N allocation.

## Backtrader Finalization

Backtrader implementation:

- `bt-core/strategies/tech_momentum.py`
- Class: `TechMomentum`

Final clean run:

```bash
cd bt-core
.venv/bin/python btmain.py \
  --strat tech_momentum.TechMomentum \
  --ticker NASDAQ_100_US.json \
  --fromdate 2014-01-01 \
  --todate 2026-06-18 \
  --timeframe daily \
  --provider yahoo \
  --data ../data \
  --cash 100000 \
  --commission none \
  --mode backtest \
  --id techmomentum_monthly_top3_reserve10 \
  --stratargs "selnum=3 rebalance='monthly' amount=-1 reserve=0.10 absolute_momentum=False"
```

Backtrader output:

- Output folder:
  `bt-core/out/tech_momentum/TechMomentum/techmomentum_monthly_top3_reserve10/`.
- `results.json`: final portfolio value `3330757` from initial `100000`.
- TimeReturn: `32.30757`.
- Computed final equity multiple from `returns.csv`: `33.3076x`.
- CAGR from `returns.csv`: `32.50%`.
- Annual volatility from `returns.csv`: `31.28%`.
- Sharpe 0rf from `returns.csv`: `1.058`.
- Max drawdown from `returns.csv`: `-45.35%`.
- Order status check: `658 Completed`, `0 Margin`.

Backtrader comparison versus QQQ daily returns:

- Alpha sum: `+15452 bps`.
- Alpha mean day: `+4.93 bps`.
- Alpha positive days: `50.51%`.
- Daily alpha stress net 2bps: `+9188 bps`.
- Daily alpha stress net 5bps: `-208 bps`.
- Daily alpha stress net 10bps: `-15868 bps`.

Note: the Backtrader run uses real engine order handling and raw OHLC execution,
so it is the acceptance run. The pandas harness is the research ranking harness
and uses adjusted open-to-open returns with turnover-cost modeling.

## Execution Mode Study: C2C, AH, RTH

This is not a separate strategy. It is the same `techmomentum` strategy studied
under different execution modes:

- C2C: the pure/original interpretation, because the rotation opens and closes
  at market close and therefore holds close-to-close.
- AH / overnight: enter at close, exit at next open.
- RTH: enter at open, exit at same-day close.

Stable taxonomy for this substudy:

```text
base_strategy: techmomentum
  variant_type: execution_mode
  execution_mode: c2c | ah | rth
  rebalance: daily | weekly | monthly
  construction: top1 | top3 | top5 | top3_abs | top3_abs_vol_adj
  cost_bps_side: 1 | 5 | ...
```

Workspace:

- Research script: `bt-strategy-test/techmomentum/session_modes/analyze_execution_modes.py`.
- Research outputs: `bt-strategy-test/techmomentum/session_modes/out_cost1/`
  and `bt-strategy-test/techmomentum/session_modes/out_cost5/`.
- Superseded early AH exploration:
  `bt-strategy-test/techmomentum/ah_exploratory/`.

Research commands:

```bash
bt-core/.venv/bin/python bt-strategy-test/techmomentum/session_modes/analyze_execution_modes.py \
  --cost-bps-side 1 \
  --out bt-strategy-test/techmomentum/session_modes/out_cost1

bt-core/.venv/bin/python bt-strategy-test/techmomentum/session_modes/analyze_execution_modes.py \
  --cost-bps-side 5 \
  --out bt-strategy-test/techmomentum/session_modes/out_cost5
```

Execution alignment:

- AH signal at close `t` uses known AH returns through `open[t] / close[t-1]`.
  Trade is `close[t] -> open[t+1]`.
- RTH signal before open `t` uses RTH returns through `t-1`. Trade is
  `open[t] -> close[t]`.
- C2C reference signal at close `t` uses close-to-close returns through `t`.
  Trade is `close[t] -> close[t+1]`.

For AH and RTH, weekly/monthly rebalance means the shortlist is refreshed on
that schedule, but the strategy still enters and exits every session.

Full-sample research results at 1 bps per side:

| Mode variant | Total return | CAGR | Ann. vol | Sharpe 0rf | Max drawdown | Alpha net 5bps vs QQQ |
|---|---:|---:|---:|---:|---:|---:|
| C2C monthly top3 | 23.76x | 29.39% | 35.02% | 0.912 | -55.16% | -814 bps |
| C2C monthly top5 | 19.51x | 27.45% | 30.19% | 0.956 | -41.13% | -4693 bps |
| AH monthly top1 | 16.61x | 25.90% | 32.27% | 0.876 | -43.18% | -5508 bps |
| AH weekly top5 | 10.01x | 21.24% | 18.47% | 1.138 | -29.86% | -14520 bps |
| QQQ buy-and-hold | 8.37x | 19.68% | 21.39% | 0.949 | -35.12% | n/a |
| RTH monthly top3 abs vol-adj | 5.42x | 16.11% | 19.69% | 0.859 | -26.69% | -19579 bps |
| RTH weekly top5 | 2.35x | 10.19% | 22.69% | 0.542 | -36.70% | -25301 bps |
| QQQ AH-only | 1.45x | 7.47% | 13.18% | 0.614 | -30.83% | -30576 bps |
| QQQ RTH-only | 0.09x | 0.67% | 16.92% | 0.124 | -28.53% | -38247 bps |

Cost stress at 5 bps per side:

| Mode variant | Total return | CAGR | Sharpe 0rf | Max drawdown |
|---|---:|---:|---:|---:|
| QQQ buy-and-hold | 8.37x | 19.68% | 0.949 | -35.12% |
| C2C monthly top3 | 1.02x | 5.81% | 0.337 | -74.17% |
| AH monthly top1 | 0.44x | 2.96% | 0.251 | -72.38% |
| AH weekly top5 | -0.10x | -0.85% | 0.046 | -68.42% |
| RTH monthly top3 abs vol-adj | -0.48x | -5.05% | -0.165 | -58.88% |

Interpretation: AH and RTH are materially disjoint, but both are very sensitive
to transaction costs because they trade two sides every active session. AH is
interesting at low costs and confirms a distinct overnight effect; RTH is
notable in recent regimes but does not beat `QQQ` full-sample. C2C remains the
strongest TechMomentum research signal.

Period stability at 1 bps per side:

| Period | Best AH | Best RTH | Best C2C | QQQ buy-and-hold |
|---|---:|---:|---:|---:|
| 2014-2020 | AH weekly top1, 52.63% CAGR | RTH monthly top3 vol-adj, 13.74% CAGR | C2C monthly top5, 33.16% CAGR | 19.60% CAGR |
| 2021-2022 | AH monthly top1, 27.63% CAGR | RTH monthly top3 vol-adj, 26.81% CAGR | C2C monthly top3 vol-adj, 4.52% CAGR | -7.37% CAGR |
| 2023-2026 | AH weekly top5, 5.65% CAGR | RTH monthly top1, 56.96% CAGR | C2C weekly top1, 82.02% CAGR | 35.20% CAGR |

Backtrader monthly mode checks were run with monthly universe files generated
from the research decisions:

- `bt-strategy-test/techmomentum/session_modes/monthly_universe_ah_monthly_top1.csv`
- `bt-strategy-test/techmomentum/session_modes/monthly_universe_rth_monthly_top3_abs_vol_adj.csv`

AH Backtrader command:

```bash
cd bt-core
.venv/bin/python btmain.py \
  --strat overnight_ah.OvernightAH \
  --ticker NASDAQ_100_US.json \
  --fromdate 2014-01-01 \
  --todate 2026-06-18 \
  --timeframe daily \
  --provider yahoo \
  --data ../data \
  --cash 100000 \
  --commission none \
  --mode backtest \
  --id techmomentum_ah_monthly_top1 \
  --stratargs "monthly_universe_file='../bt-strategy-test/techmomentum/session_modes/monthly_universe_ah_monthly_top1.csv' max_concurrent=1 max_exposure=1.0 size_by_max_concurrent=True min_intraday_vol=0.0 max_intraday_vol=10.0 ah_lag1_threshold=0.0 min_adv=0 auction=True earnings_skip=False"
```

AH Backtrader output:

- Folder: `bt-core/out/overnight_ah/OvernightAH/techmomentum_ah_monthly_top1/`.
- Final equity multiple from `returns.csv`: `32.77x`.
- CAGR from `returns.csv`: `32.39%`.
- Annual volatility: `32.26%`.
- Sharpe 0rf: `1.031`.
- Max drawdown: `-39.96%`.
- Trades: `3132`.

RTH Backtrader command:

```bash
cd bt-core
.venv/bin/python btmain.py \
  --strat rth_open_close.RTHOpenClose \
  --ticker NASDAQ_100_US.json \
  --fromdate 2014-01-01 \
  --todate 2026-06-18 \
  --timeframe daily \
  --provider yahoo \
  --data ../data \
  --cash 100000 \
  --commission none \
  --mode backtest \
  --id techmomentum_rth_monthly_top3_abs_vol_adj \
  --stratargs "monthly_universe_file='../bt-strategy-test/techmomentum/session_modes/monthly_universe_rth_monthly_top3_abs_vol_adj.csv' max_concurrent=3 max_exposure=0.95 size_by_max_concurrent=True amount=-1 rank_by='monthly_order' min_price=0 min_adv=0 reserve=0"
```

RTH Backtrader output:

- Folder:
  `bt-core/out/rth_open_close/RTHOpenClose/techmomentum_rth_monthly_top3_abs_vol_adj/`.
- Final equity multiple from `returns.csv`: `7.94x`.
- CAGR from `returns.csv`: `18.13%`.
- Annual volatility: `18.65%`.
- Sharpe 0rf: `0.987`.
- Max drawdown: `-23.17%`.
- Trades: `9362`.

Decision: do not promote AH or RTH as standalone production configurations.
Keep them as execution-mode dimensions of TechMomentum and potentially reuse
their behavior as a regime/filter layer around the stronger C2C candidate.

## Assessment

Synthetic assessment: `B-`.

Rationale:

- The signal is real enough to beat `QQQ` in the full period and survive the
  2021-2022 rate-shock regime in the monthly top3 test.
- The original top1 universe is too concentrated and partly contaminated by
  hand-picked names plus `SPXL`.
- Top1 over the Nasdaq universe is not robust: it has large drawdowns and
  unstable leadership.
- Monthly top3 is the current best candidate, but drawdown remains high and
  execution/capacity rules matter.
- The final Backtrader run is clean with a 10% reserve and no margin rejections.

Current recommendation:

- Continue with monthly Nasdaq top3 as the main candidate.
- Keep 10% reserve in Backtrader unless a two-step rebalance implementation is
  added.
- Before production, add realistic commissions/slippage, review tax treatment of
  any ETF variants, and test live/paper execution constraints.
