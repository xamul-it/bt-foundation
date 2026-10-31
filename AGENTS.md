# Repository Guidelines

## Project Structure & Module Organization

This repository is a multi-service trading workspace:

- `bt-core/`: Backtrader engine, brokers, indicators, production strategies, and most Python tests.
- `bt-api/`: Flask API, scheduler services, authentication, and API tests.
- `bt-dash/`: Vue 3/Quasar dashboard; application code is under `src/`, PWA assets under `src-pwa/`, and JavaScript tests under `tests/`.
- `bt-gateway/`: Alpaca/ZMQ streaming and replay services.
- `bt-agent/`: research and strategy-prototyping utilities.
- `config-common/`: shared ticker lists, schedules, benchmarks, and market data. Treat runtime data as operational state.
- `bt-docker/`, `bin/`, and `scripts/`: containers and operational tooling.

Place tests beside the owning service in its `tests/` directory. Keep experimental studies in `research/` or `bt-strategy-test/`, not production modules.

## Build, Test, and Development Commands

Use each service's existing virtual environment:

```bash
bt-core/.venv/bin/pytest -q bt-core/tests
bt-api/.venv/bin/pytest -q bt-api/tests
cd bt-dash && node --test tests/*.test.cjs
cd bt-dash && npx quasar dev
cd bt-dash && npx quasar build -m pwa
```

The production dashboard is served from `bt-dash/dist/pwa`; an SPA-only build does not update production. Install frontend dependencies with `npm install` inside `bt-dash/`.

## Coding Style & Naming Conventions

Use four spaces in Python and two spaces in Vue/JavaScript. Follow existing patterns: `snake_case` for Python functions and modules, `PascalCase` for Vue components and strategy classes, and `camelCase` for JavaScript methods and state. Prefer small service functions, explicit error handling, and repository-relative configuration. No repository-wide formatter is enforced; keep diffs focused and run `git diff --check`.

## Testing Guidelines

Python tests use `pytest` and follow `test_*.py`; dashboard tests use Node's built-in test runner and `*.test.cjs`. Add regression coverage for fixes, especially broker lifecycle, order handling, scheduler state, API validation, and chart/data transformations. Run the narrow affected suite first, then the full service suite.

## Commit & Pull Request Guidelines

Recent history favors concise Conventional Commit subjects such as `feat(backtesting): ...`, `fix: ...`, and `docs: ...`. Keep commits scoped to one concern. Pull requests should explain behavior changes, list verification commands, link relevant issues, and include screenshots for dashboard changes. Call out migrations, scheduling changes, or effects on live/paper trading.

## Security & Configuration

Never commit credentials, broker keys, cookies, or private `.env` files. Preserve user-owned files under `config-common/`, `out/`, and `logs/`. Treat live-order changes as high risk and verify them with replay or backtest coverage before deployment.
