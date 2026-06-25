# Strategy Analysis Methodology

This document defines reusable strategy-analysis rules. It should be read before
testing any new intraday strategy.

## Folder Convention

- In-progress strategy research, simulations, draft code, and working material
  live under `bt-strategy-test/`.
- Strategy-specific research scripts live under `bt-strategy-test/{strategy_name}/` unless they are production helpers, which live under `bin/`.
- Shared feature builders live in a shared folder, for example
  `bt-strategy-test/RTH_analysis/`.
- Strategy summaries live in `docs/context/strategy-analysis.md`.
- Technical strategy notes live in `docs/context/strategies/{strategy_name}.md`.

## Python Environment

Use the shared virtualenv at `bt-core/.venv/` for strategy analysis and backtest
commands.

Preferred command forms:

```bash
bt-core/.venv/bin/python path/to/script.py
bt-core/.venv/bin/pip ...
```

If activating a shell, use:

```bash
source bt-core/.venv/bin/activate
```

## Backtrader Finalization

Exploratory notebooks or standalone pandas scripts can be used for research,
filter discovery, and fast iteration. A strategy study is not finalized until it
has a Backtrader strategy implementation and the final candidate has been run
through `bt-core/btmain.py`.

Use `btmain.py` from inside `bt-core/`, with the shared virtualenv:

```bash
cd bt-core
.venv/bin/python btmain.py --strat path_or_module.StrategyClass ...
```

The final report or strategy context must record the `btmain.py` command, data
range, provider, universe, strategy parameters, benchmark, and output paths.

## Required Research Questions

Each strategy study must explicitly answer the core design questions tested,
instead of treating them as defaults. At minimum, record:

- Trade direction: long-only, short-only, long/short, or conditional direction.
- Entry timing and order type.
- Exit logic, including stop, take-profit, signal exit, and EOD flattening.
- Universe selection and ranking/filter logic.
- Target deployment regime and whether results are stable outside it.

Long-only is not a general rule. It is one possible tested configuration and
must be justified by results for the specific strategy.

## Data Convention

Record the following for every study:

- Data provider and feed.
- Universe file.
- Timeframe and compression.
- Date range.
- In-sample, validation, and OOS ranges.
- Whether missing data was backfilled.
- Backtrader strategy implementation used for finalization.
- Final `bt-core/btmain.py` command.
- Output paths for generated datasets and reports.

For the current intraday studies:

- Provider: Alpaca SIP.
- Base bars: minute.
- Backtrader can resample to strategy timeframe.
- Common universe: `config-common/tickers/rth_stable_candidates_10.json`.
- Market data root: `data/`, which points to `config-common/data/`.
- Data is split by provider, including Alpaca and Yahoo:
  - Daily Alpaca data: `data/d/alpaca/`.
  - Daily Yahoo data: `data/d/yahoo/`.
  - Minute Alpaca data: `data/m/alpaca/`.
- If required tickers are not present locally, use `bt-core/load_tickers.py` to
  load/backfill them before running the study.

## Benchmark

For intraday strategies, the primary benchmark is buy-open/sell-close.

For each symbol-day:

```text
bo_sc_bps(symbol, date) =
    (RTH close / RTH open - 1) * 10000
```

For each trading day:

```text
benchmark_day_sum_bps =
    sum bo_sc_bps(symbol, date) over the tradable universe
```

Buy-and-hold is not the main benchmark for intraday strategies. It may be
reported as context, but it should not drive strategy acceptance.

## Indicator 1: Alpha Versus Benchmark

For every strategy/filter and trading day:

```text
strategy_day_sum_bps =
    sum pnl_bps for all strategy trades on that day

alpha_day_bps =
    strategy_day_sum_bps - benchmark_day_sum_bps
```

If the strategy/filter does not trade that day, use `strategy_day_sum_bps = 0`.
This keeps the opportunity cost of BO-SC visible and avoids rewarding filters
that disappear on difficult days.

Period-level indicator:

```text
indicator_1_alpha_vs_benchmark_bps =
    sum alpha_day_bps over the period
```

Also report:

- `alpha_mean_day_bps`
- `alpha_positive_days_pct`
- `alpha_net_2bps`
- `alpha_net_5bps`
- `alpha_net_10bps`

## Indicator 2: Trade Goodness

This compares strategy quality across different trade counts.

```text
indicator_2_trade_goodness =
    edge_bps_per_trade * avg_trades_per_day
```

This is the expected daily production from the strategy's own trades before
benchmark subtraction.

Always report its components:

- `edge_bps_per_trade`
- `avg_trades_per_day`
- `trade_count`

Reason: `100` trades at `10 bps` and `10` trades at `100 bps` can have similar
gross production, but the execution risk is very different.

## Indicator 3: Intrinsic Stability / Risk

This measures the internal stability of strategy behavior.

Daily edge:

```text
strategy_edge_day_bps =
    strategy_day_sum_bps / trade_count_day
```

Coefficient of variation:

```text
edge_cv =
    std(strategy_edge_day_bps) / abs(mean(strategy_edge_day_bps))

trades_cv =
    std(trade_count_day) / mean(trade_count_day)
```

Risk penalty:

```text
indicator_3_intrinsic_risk_penalty =
    edge_cv + trades_cv
```

Stability index:

```text
indicator_3_intrinsic_stability =
    1 / (1 + edge_cv + trades_cv)
```

Higher stability is better. Higher risk penalty is worse.

## Execution Robustness

For every strategy/filter, report:

```text
break_even_cost_bps_per_trade =
    alpha_sum_bps / trade_count
```

Interpretation: average round-trip bps per trade that can be lost to slippage,
spread, and execution before alpha becomes zero.

Also stress:

```text
alpha_net_2bps  = alpha_sum_bps - 2  * trade_count
alpha_net_5bps  = alpha_sum_bps - 5  * trade_count
alpha_net_10bps = alpha_sum_bps - 10 * trade_count
```

## Recommended Acceptance Logic

A strategy is interesting only if:

- Indicator 1 is positive in OOS or in the target deployment regime.
- Indicator 2 is positive and large enough to justify execution risk.
- Indicator 3 is not dominated by extreme edge/trade-count instability.
- `alpha_net_5bps` is positive or the strategy has a strong reason to expect
  lower execution cost.

Suggested qualitative grades:

- `A`: alpha positive and robust after execution stress.
- `B`: alpha positive but still sensitive to execution or regime.
- `C`: signal exists but does not yet beat benchmark robustly.
- `D`: does not beat BO-SC.
- `X`: test contaminated, incomplete, or not comparable.

## Commit Discipline

When a study produces meaningful new results, proceed with a git commit before
starting materially different research. Meaningful results include:

- A new strategy dataset.
- A completed OOS benchmark comparison.
- A new filter/ranking study that changes interpretation.
- A methodology change affecting reported metrics.

Before committing:

- Check `git status`.
- Include only files related to the study.
- Do not revert unrelated user or project changes.
- Mention data ranges, benchmark, and main conclusion in the commit message.
