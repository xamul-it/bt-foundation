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
- Overnight/AH strategies are compared against the session leg they trade,
  typically close-to-next-open or the explicitly modeled entry/exit variant.
- Primary performance is alpha versus the correct session benchmark.
- Strategy comparison uses three indicators: alpha versus benchmark, trade
  goodness, and intrinsic stability/risk.
- When a study produces meaningful new results, proceed with a git commit before
  starting a new research branch or a materially different strategy.

## Strategy Summary

| Strategy | Status | Data | Benchmark | Best signal so far | Synthetic assessment |
|---|---|---|---|---|---|
| SuperTrend intraday (tested long-only) | Studied, not production-ready | Alpaca SIP minute, `rth_stable_candidates_10.json`, 2023-2026 YTD | Buy-open/sell-close symbol-day basket | Volume regime: `rth_logvol_z_20 top50`; volume filters improve 2026 but remain fragile after costs | C: useful signal, but full-period alpha is negative versus BO-SC |
| RTH open/close dynamic selection | Under reassessment, not field-test ready | Yahoo adjusted daily for signal generation; Alpaca SIP minute for execution validation; Nasdaq/RTH working universes | Buy-open/sell-close session benchmark and same-period `OvernightAH` replacement target | `RTHMinuteMultiBucketExecution` q4 looked strong on 2023-2026 but weak on continuous 2020-2025; raw bucket remains research material, sortino bucket is currently suspect | C: real regime-dependent signal, but q4 multi-bucket is not robust enough for promotion |
| Tech Momentum daily rotation | Studied, Backtrader-finalized candidate with execution-mode substudies | Yahoo daily, local `NASDAQ_100_US.json`, 2014-2026 YTD | `QQQ`, plus equal-weight Nasdaq, `SPY`, and mode benchmarks `QQQ` AH-only/RTH-only | Monthly Nasdaq top3, 21/63/126 momentum, 10% reserve in Backtrader; additional C2C/AH/RTH execution-mode tests remain part of this same strategy | B-: C2C signal is strongest; AH/RTH confirm session disjunction but are cost/regime-sensitive |
| Overnight AH | Paper-operational candidate with dynamic monthly universe; live/stable split in progress | Yahoo daily for current paper signal, `yahoo_adj_research_universe.json` including `SPY`; Alpaca paper/live for execution | Close-to-next-open for daily backtest; paper `auc` currently tests market/GTC pre-close entry, so execution variant must be tracked separately | `weak_theme_switch`: dynamic 85% base + 15% semis corr12 top50 when semis regime is positive, otherwise static fallback; SPY 3m drawdown gate active when `SPY` is loaded | B: strong historical edge, but feed choice, entry timing, SPY gate, and execution slippage must stay aligned between backtest and paper/live |

## Strategy Files

- `docs/context/strategies/supertrend-intraday.md`
- `docs/context/strategies/tech-momentum.md`
- `docs/context/ah_context.md`
- `docs/context/alpaca_paper_live_overnight_ah.md`
- `bt-strategy-test/rth_open_close/README.md`
- `bt-strategy-test/rth_open_close/rth_dynamic_selection_notes.md`
- `bt-strategy-test/rth_open_close/rth_final_q4_field_runbook.md`

## Lessons Learned Register

- RTH q4 multi-bucket: do not promote a candidate from a strong recent window
  alone. The 2023-2026 Backtrader run was strong, but continuous 2020-2025
  validation exposed weak 2020-2021 behavior and large drawdown.
- Regime filters must be tested continuously, not only on the stress period
  they were designed to repair. The semis126 filter improved 2022 but did not
  address 2020-2021 losses.
- Daily proxy studies are hypothesis generators. Promotion requires
  Backtrader minute execution over the full intended deployment window, with
  the same universe, signal files, slippage, sizing, and regime gates.
- Multi-bucket candidates must include bucket attribution before promotion. In
  the RTH q4 reassessment, `raw_full_prerange009` remained positive while
  `sortino_tp4_tight` degraded total performance.
- The total-profit target must be checked alongside edge/trade and win-rate.
  High bps/trade on sparse trades is not enough to replace a higher-capacity
  strategy such as `OvernightAH`.
- Keep one source of truth for runtime parameters. Use `btmain` strategy params
  or run-config JSON for strategy parameters; do not duplicate live behavior in
  bucket JSON files.
- Gating symbols and tradable universes must be explicit. If a regime filter
  uses symbols such as `NVDA`, `AMD`, `AVGO`, `AMAT`, `ASML`, and `MU`, their
  data source and availability must be checked even if they are not directly
  traded.

## Script Locations

- Work-in-progress strategy research area: `bt-strategy-test/`
- SuperTrend-specific scripts: `bt-strategy-test/supertrend_intraday/`
- Tech Momentum research scripts: `bt-strategy-test/techmomentum/`
- Tech Momentum execution-mode scripts: `bt-strategy-test/techmomentum/session_modes/`
- OvernightAH operational/research bundle: `bt-strategy-test/overnight-ah/`
- RTH open/close research bundle: `bt-strategy-test/rth_open_close/`
- Superseded AH-only exploratory scripts: `bt-strategy-test/techmomentum/ah_exploratory/`
- Reusable RTH feature builders: `bt-strategy-test/RTH_analysis/`
- Python virtualenv: `bt-core/.venv/`
- Market data root: `data/` (symlink to `config-common/data/`)
- Missing ticker loader: `bt-core/load_tickers.py`

## Current Decisions

- Do not use buy-and-hold as the main benchmark for intraday strategies.
- Use buy-open/sell-close as the market drift benchmark.
- For overnight/AH strategies, do not use BO-SC as the main benchmark unless
  the strategy trades the RTH open-to-close leg. Use close-to-next-open or the
  explicitly modeled execution leg.
- Do not compare an auction-close backtest with a market-pre-close paper run
  without labeling it as an execution variant.
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
