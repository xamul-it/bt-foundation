# Strategy Analysis Index

This file is the entry point for strategy research context. A new agent should
read this file first, then `strategy-analysis-methodology.md`, then the
strategy-specific file under `docs/context/strategies/`.

## Reusable Method

Read: `docs/context/strategy-analysis-methodology.md`

Core convention:

- Use the shared Python virtualenv at `bt-core/.venv/` for analysis and
  backtest commands.
- Final strategy studies must create a Backtrader strategy implementation and
  run it through `bt-core/btmain.py`.
- Intraday strategies are compared against buy-open/sell-close, not buy-and-hold.
- Primary performance is alpha versus the buy-open/sell-close benchmark.
- Strategy comparison uses three indicators: alpha versus benchmark, trade
  goodness, and intrinsic stability/risk.
- When a study produces meaningful new results, proceed with a git commit before
  starting a new research branch or a materially different strategy.

## Strategy Summary

| Strategy | Status | Data | Benchmark | Best signal so far | Synthetic assessment |
|---|---|---|---|---|---|
| SuperTrend intraday (tested long-only) | Studied, not production-ready | Alpaca SIP minute, `rth_stable_candidates_10.json`, 2023-2026 YTD | Buy-open/sell-close symbol-day basket | Volume regime: `rth_logvol_z_20 top50`; volume filters improve 2026 but remain fragile after costs | C: useful signal, but full-period alpha is negative versus BO-SC |
| Tech Momentum daily rotation | Studied, Backtrader-finalized candidate with execution-mode substudies | Yahoo daily, local `NASDAQ_100_US.json`, 2014-2026 YTD | `QQQ`, plus equal-weight Nasdaq, `SPY`, and mode benchmarks `QQQ` AH-only/RTH-only | Monthly Nasdaq top3, 21/63/126 momentum, 10% reserve in Backtrader; additional C2C/AH/RTH execution-mode tests remain part of this same strategy | B-: C2C signal is strongest; AH/RTH confirm session disjunction but are cost/regime-sensitive |

## Strategy Files

- `docs/context/strategies/supertrend-intraday.md`
- `docs/context/strategies/tech-momentum.md`

## Script Locations

- Work-in-progress strategy research area: `bt-strategy-test/`
- SuperTrend-specific scripts: `bt-strategy-test/supertrend_intraday/`
- Tech Momentum research scripts: `bt-strategy-test/techmomentum/`
- Tech Momentum execution-mode scripts: `bt-strategy-test/techmomentum/session_modes/`
- Superseded AH-only exploratory scripts: `bt-strategy-test/techmomentum/ah_exploratory/`
- Reusable RTH feature builders: `bt-strategy-test/RTH_analysis/`
- Python virtualenv: `bt-core/.venv/`
- Market data root: `data/` (symlink to `config-common/data/`)
- Missing ticker loader: `bt-core/load_tickers.py`

## Current Decisions

- Do not use buy-and-hold as the main benchmark for intraday strategies.
- Use buy-open/sell-close as the market drift benchmark.
- Do not assume long-only as a general rule; trade direction is a research
  question to answer per strategy.
- Use `bt-core/.venv/` as the Python environment for strategy analysis work.
- Use market data from `data/`, split by provider such as `data/d/alpaca/`,
  `data/d/yahoo/`, and `data/m/alpaca/`.
- If required tickers are missing locally, load/backfill them with
  `bt-core/load_tickers.py`.
- A study is not considered finalized until the signal has a Backtrader strategy
  implementation and has been run through `bt-core/btmain.py`.
- Use `bt-strategy-test/` as the dedicated workspace for simulations, draft
  code, experiments, and other in-progress strategy material.
- For future strategies, keep strategy-specific scripts under
  `bt-strategy-test/{strategy_name}/`.
- Keep shared methodology in `docs/context/strategy-analysis-methodology.md`.
- Keep results and interpretation in strategy-specific context files.
