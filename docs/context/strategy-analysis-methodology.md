# Strategy Analysis Methodology

This document defines reusable strategy-analysis rules. It should be read before
testing any new intraday, overnight, or session-specific strategy.

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

For strategies that may be paper/live deployed, also record:

- Exact paper/live script or run-config used.
- Broker account/profile targeted, without secrets.
- Entry order type and scheduled time.
- Exit order type and scheduled time.
- Whether paper/live differs from the backtest execution assumption.

## Required Research Questions

Each strategy study must explicitly answer the core design questions tested,
instead of treating them as defaults. At minimum, record:

- Trade direction: long-only, short-only, long/short, or conditional direction.
- Entry timing and order type.
- Exit logic, including stop, take-profit, signal exit, and EOD flattening.
- Universe selection and ranking/filter logic.
- Target deployment regime and whether results are stable outside it.
- Data adjustment policy: raw OHLCV, adjusted OHLCV, or provider-specific daily
  bars.
- Corporate-event policy: splits, dividends, earnings, delistings, symbol
  changes, and whether those events are modeled in both backtest and paper/live.

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

For overnight/AH strategies, the primary benchmark must match the traded
session leg:

```text
close_to_next_open_bps(symbol, date) =
    (next RTH open / current RTH close - 1) * 10000
```

If the live/paper implementation enters before close instead of at the closing
auction, record it as an execution variant:

```text
preclose_to_next_open_bps(symbol, date, entry_time) =
    (next RTH open / entry_price_at_time - 1) * 10000
```

Do not compare these variants as if they were identical. A daily backtest with
`auction=True` usually approximates close-to-next-open. A paper/live run with
`auction=False` and cron before the close is a different execution leg and must
be labeled separately.

For daily rotation or multi-session strategies, report each session leg that
matters to the hypothesis, for example C2C, AH-only, RTH-only, and total C2C.

## Feed And Data Invariance

Before comparing two runs, verify that only the intended variable changed.

Do:

- Pin provider, feed, universe, date range, timeframe, commission model,
  slippage/cost model, and strategy params in the report.
- Use the same signal feed in backtest and paper/live when the goal is
  operational validation.
- Treat Yahoo, Yahoo adjusted, and Alpaca SIP as different datasets, not
  interchangeable sources.
- For threshold filters, report borderline cases when provider differences can
  flip pass/fail decisions.
- Include required gating symbols, such as `SPY`, in the ticker universe if the
  strategy needs their data; explicitly note when such symbols are not tradable.
- Use `trade_start_date` or an equivalent warmup control when indicators require
  history before the evaluation window.

Do not:

- Change provider and strategy logic in the same comparison.
- Add commission/slippage penalties to only one side of a comparison.
- Enable a paper/live filter that was not present in the backtest and still
  call the result the same strategy.
- Let missing gate data silently change behavior; document whether missing data
  blocks trading, bypasses the gate, or falls back to another universe.

## Corporate Events And Adjusted Data

Raw OHLCV can create artificial discontinuities around splits and dividends.
Adjusted OHLCV can make historical indicator behavior more realistic, but it may
not match executable live prices.

Rules:

- For split-sensitive strategies, avoid raw historical OHLCV unless split
  handling is explicitly tested.
- For dividend-sensitive overnight strategies, decide whether dividend drops are
  part of the economic return. If the strategy holds through ex-date, raw price
  drops may overstate loss unless dividend cash is modeled.
- If adjusted data is used, create an adjusted dataset with adjusted open, high,
  low, close, and volume policy documented. Do not mix adjusted close with raw
  open/high/low in the same return calculation.
- Paper/live cannot trade adjusted prices. Any adjusted-data backtest must be
  paired with a raw/live execution interpretation.
- Corporate-event filters, such as earnings skips, must be backtested with a
  historical event calendar before being promoted to paper/live. Otherwise they
  create an untested strategy variant.

## Paper/Live Promotion Guardrails

Paper/live should test the same hypothesis as the backtest unless the difference
is intentional and named.

Do:

- Keep a stable production checkout and a separate development checkout.
- Record which cron slot or script points to which checkout.
- Use run-configs or wrapper scripts that make provider, universe, mode, and
  strategy params explicit.
- Log no-candidate/no-order reasons in paper/live.
- Check open orders and positions before manual reruns.
- Use read-only broker checks before any manual paper/live run.

Do not:

- Rerun an entry script after market hours unless you know whether it can create
  accepted GTC orders.
- Assume `paper` is risk-free; bad GTC/OPG/CLS orchestration can invalidate the
  next test.
- Point live/paper at research code without first deciding whether the change is
  a promotion, a paper-only experiment, or a throwaway test.
- Store API keys, account secrets, or live credentials in strategy bundles.

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

## Promotion Guardrails

Do not promote a strategy from a single favorable market window. A candidate
must pass the target deployment window and the relevant stress windows before it
is treated as field-test ready.

For session strategies, report at least:

- pre-2022 or early-history validation when local data exists;
- 2020 stress behavior when applicable;
- 2022 bear/regime-change behavior;
- recent favorable-window behavior;
- continuous full-window behavior over the same inputs.

If a filter is designed to repair one stress period, validate it on the
continuous full window as well. A filter that fixes 2022 but leaves 2020-2021
broken is not a general regime solution.

For multi-bucket or portfolio-combined strategies, report bucket-level
attribution before promotion:

- trades;
- edge/trade;
- win-rate;
- PnL contribution;
- worst year;
- worst symbols or concentration.

Treat daily OHLC or pandas proxy studies as hypothesis generation unless the
same logic has been rerun through the intended Backtrader execution path. The
final promotion evidence must use the same provider/feed, universe, slippage,
sizing, signal files, and runtime gates expected in paper/live.

When a competing production or paper candidate exists, check total profit and
capital multiple in addition to edge/trade. A sparse strategy with excellent
average trade quality may still fail the portfolio objective if it cannot
produce enough stable total return.

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
