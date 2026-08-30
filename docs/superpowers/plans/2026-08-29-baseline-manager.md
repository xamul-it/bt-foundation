# Baseline Manager Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an operator generate, store, list, select and delete named statistical baselines (long backtest windows) for a scheduled cron profile, and check whether the recent backtest window is still statistically consistent with a chosen baseline (regime-shift detection).

**Architecture:** New dedicated table `profile_baselines` (decision D2b) parallel to `strategy_footprints` and `stat_baselines`. A new `bt-core/profile_baseline.py` module holds the pure helpers and the backtest-orchestration functions (mirrors `bin/watchtower_compute_footprint_drift.py`). `bt-core/watchtower_runtime.py` gets four thin SQL CRUD methods. `bt-api/app/watchtower.py` gets five endpoints plus an in-memory background-job wrapper (mirrors the existing `_start_baseline_job`). `bt-dash` gets a new "Gestione baseline" page plus a baseline selector + drift panel on the existing Configurazione page. Reuses the statistical engine as-is (`baseline_metrics_from_trades`, `evaluate_outcomes`, `monte_carlo_subset_test`).

**Tech Stack:** Python 3, psycopg (v3), Flask blueprint, Postgres (`bt_live_events` on `127.0.0.1:5433`), Vue 3 + Quasar 2 (`bt-dash`), `btmain.py` backtest engine.

**Spec:** `docs/superpowers/specs/2026-08-29-baseline-manager-design.md`

## Global Constraints

- **Cardinal constraint (unchanged):** `profile` is always free-form text, never a hardcoded set. Every endpoint and query must work for any profile name, including one with no rows yet (returns `[]`/empty, never an error).
- **Decisions locked:** D1a — version = STRATARGS historicised at `as_of_date` via `repo.resolve_params_as_of`, backtest runs against the *current* code checkout (no git worktree). D2b — new table `profile_baselines` (NOT `scheduled_baselines`; baselines are on-demand, not scheduled). D3 — recent window default **10 trading days**, param `recent_window_days`, allowed range 3–30.
- **Do not touch:** `watchtower_compute_footprint_drift.py`, `strategy_footprints`, `watchtower_replay_reconcile.py`, `stat_baselines`, the intraday `Watchtower.vue` page.
- **UI actions that write to the DB and launch backtests are in scope** for this feature. There is no project-wide "no UI writes" rule (confirmed with the user this session); the "Gestione baseline" page legitimately triggers `btmain.py` runs and inserts `profile_baselines` rows.
- **bt-api has no test suite / app factory.** `repo` is a module singleton (`from app.service.main_service import repo`). Endpoint verification is by `curl` against `bt-api@dev`; all testable logic lives in `bt-core` and is covered by `unittest` there.
- **bt-dash has no test runner** (`npm test` is a stub). Frontend verification = `@vue/compiler-sfc` compile-check + `npx quasar build -m pwa` succeeds. Build with `export PATH="/home/htpc/.nvm/versions/node/v20.20.0/bin:$PATH"`.
- Test style in `bt-core/tests/`: `unittest.TestCase`, subclass `WatchtowerRepository` as `StubRepo` overriding DB methods. Run: `bt-core/.venv/bin/python -m pytest tests/<file> -v` (pytest picks up unittest classes).
- Commit after every task. Branch off `main` first (`git checkout -b feat/baseline-manager`), never commit straight to `main`.

---

### Task 1: `profile_baselines` table

**Files:**
- Modify: `bt-core/config/sql/live_events_postgres.sql` (append at end of file)

**Interfaces:**
- Produces: table `profile_baselines` with columns used by Task 2's SQL.

- [ ] **Step 1: Append the DDL block**

Add to the end of `bt-core/config/sql/live_events_postgres.sql`:

```sql

-- Baseline Manager (docs/superpowers/specs/2026-08-29-baseline-manager-design.md).
--
-- On-demand, operator-managed statistical baselines for a scheduled cron
-- profile. Deliberately NOT stat_baselines (single-row-per-params, shared
-- with the intraday baseline endpoints) and NOT strategy_footprints
-- (period enum locked to pre/post_activation, one row per (profile,period),
-- auto-computed by watchtower_compute_footprint_drift.py). Here we need
-- several NAMED baselines per profile, each pinned to an explicit
-- backtest window + params-as-of date. code_commit stays NULL for now
-- (decision D1a: params-as-of only, current code checkout).
CREATE TABLE IF NOT EXISTS profile_baselines (
    id BIGSERIAL PRIMARY KEY,
    profile TEXT NOT NULL,
    strategy TEXT NOT NULL,
    label TEXT NOT NULL,
    params JSONB NOT NULL DEFAULT '{}'::jsonb,
    params_hash TEXT NOT NULL,
    as_of_date DATE,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    code_commit TEXT,
    ticker TEXT,
    provider TEXT,
    sample_size BIGINT NOT NULL DEFAULT 0,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    pnl_values JSONB NOT NULL DEFAULT '[]'::jsonb,
    run_id TEXT,
    source_meta JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_profile_baselines_window_order
        CHECK (window_end >= window_start),
    UNIQUE (profile, label)
);

CREATE INDEX IF NOT EXISTS idx_profile_baselines_lookup
    ON profile_baselines (profile, created_at DESC);
```

- [ ] **Step 2: Apply it to the running DB**

Run:
```bash
cd /home/htpc/backtrader
set -a; source env/bt-live-events; set +a
bt-core/.venv/bin/python -c "
import os, psycopg
from pathlib import Path
sql = Path('bt-core/config/sql/live_events_postgres.sql').read_text()
with psycopg.connect(os.environ['LIVE_EVENTS_DB_DSN']) as c:
    c.execute(sql); c.commit()
print('schema applied')
"
```
Expected: `schema applied` (the file is fully idempotent — `CREATE TABLE IF NOT EXISTS` throughout).

- [ ] **Step 3: Verify the table exists**

Run:
```bash
set -a; source env/bt-live-events; set +a
bt-core/.venv/bin/python -c "
import os, psycopg
with psycopg.connect(os.environ['LIVE_EVENTS_DB_DSN']) as c, c.cursor() as cur:
    cur.execute(\"select column_name from information_schema.columns where table_name='profile_baselines' order by ordinal_position\")
    print([r[0] for r in cur.fetchall()])
"
```
Expected: `['id', 'profile', 'strategy', 'label', 'params', 'params_hash', 'as_of_date', 'window_start', 'window_end', 'code_commit', 'ticker', 'provider', 'sample_size', 'metrics', 'pnl_values', 'run_id', 'source_meta', 'created_at']`

- [ ] **Step 4: Commit**

```bash
git add bt-core/config/sql/live_events_postgres.sql
git commit -m "feat(watchtower): add profile_baselines table for baseline manager"
```

---

### Task 2: Repo CRUD methods for `profile_baselines`

**Files:**
- Modify: `bt-core/watchtower_runtime.py` (add methods to `WatchtowerRepository`, next to `list_strategy_footprints`)
- Test: `bt-core/tests/test_profile_baselines.py` (new)

**Interfaces:**
- Consumes: `profile_baselines` table (Task 1); module helpers `_cursor_rows`, `_cursor_row`, `stable_json`, `self.connect()`, `self._normalize_dates`, `strategy_params_hash`, `strategy_fingerprint` (all already in `watchtower_runtime.py`).
- Produces:
  - `WatchtowerRepository.insert_profile_baseline(profile: str, strategy: str, label: str, params: dict, window_start: date, window_end: date, metrics: dict, pnl_values: list[float], sample_size: int, as_of_date: date | None = None, ticker: str | None = None, provider: str | None = None, run_id: str | None = None, code_commit: str | None = None, source_meta: dict | None = None) -> dict` — returns `{"id": int, "profile": str, "label": str}`. Raises `ValueError("duplicate_baseline_label:<profile>/<label>")` on unique violation.
  - `WatchtowerRepository.list_profile_baselines(profile: str | None = None) -> list[dict]` — full rows, dates ISO-stringified, newest first.
  - `WatchtowerRepository.get_profile_baseline(baseline_id: int) -> dict | None`
  - `WatchtowerRepository.delete_profile_baseline(baseline_id: int) -> bool` — True if a row was deleted.

- [ ] **Step 1: Write the failing test**

Create `bt-core/tests/test_profile_baselines.py`:

```python
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from watchtower_runtime import WatchtowerRepository


class ProfileBaselineCrudTests(unittest.TestCase):
    """Round-trips against the real bt_live_events DB. Skipped when the DB
    DSN is not configured (mirrors how the rest of watchtower behaves)."""

    LABEL = "pytest-crud-fixture"

    def setUp(self):
        self.repo = WatchtowerRepository()
        if not self.repo.available():
            self.skipTest("LIVE_EVENTS_DB_DSN not configured")
        self._cleanup()

    def tearDown(self):
        if getattr(self, "repo", None) and self.repo.available():
            self._cleanup()

    def _cleanup(self):
        for row in self.repo.list_profile_baselines("pytest-profile"):
            self.repo.delete_profile_baseline(row["id"])

    def test_insert_list_get_delete_roundtrip(self):
        created = self.repo.insert_profile_baseline(
            profile="pytest-profile",
            strategy="overnight_ah.OvernightAH",
            label=self.LABEL,
            params={"period": 8},
            window_start=date(2015, 1, 1),
            window_end=date(2026, 1, 1),
            metrics={"mean": 0.1, "sample_size": 3},
            pnl_values=[0.1, -0.2, 0.3],
            sample_size=3,
            as_of_date=date(2026, 1, 1),
            ticker="allmib.json",
            provider="alpaca",
            run_id="pytest-run",
        )
        self.assertIn("id", created)

        rows = self.repo.list_profile_baselines("pytest-profile")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["label"], self.LABEL)
        self.assertEqual(rows[0]["window_start"], "2015-01-01")
        self.assertEqual(rows[0]["pnl_values"], [0.1, -0.2, 0.3])

        one = self.repo.get_profile_baseline(created["id"])
        self.assertEqual(one["strategy"], "overnight_ah.OvernightAH")

        self.assertTrue(self.repo.delete_profile_baseline(created["id"]))
        self.assertEqual(self.repo.list_profile_baselines("pytest-profile"), [])
        self.assertIsNone(self.repo.get_profile_baseline(created["id"]))

    def test_unknown_profile_returns_empty(self):
        self.assertEqual(self.repo.list_profile_baselines("never-seen-profile"), [])

    def test_duplicate_label_raises(self):
        common = dict(
            profile="pytest-profile", strategy="s", label=self.LABEL, params={},
            window_start=date(2015, 1, 1), window_end=date(2026, 1, 1),
            metrics={}, pnl_values=[], sample_size=0,
        )
        self.repo.insert_profile_baseline(**common)
        with self.assertRaisesRegex(ValueError, "duplicate_baseline_label"):
            self.repo.insert_profile_baseline(**common)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bt-core/.venv/bin/python -m pytest tests/test_profile_baselines.py -v`
Expected: FAIL — `AttributeError: 'WatchtowerRepository' object has no attribute 'insert_profile_baseline'`

- [ ] **Step 3: Implement the four methods**

In `bt-core/watchtower_runtime.py`, immediately after `list_strategy_footprints` (search for `def list_strategy_footprints`), add:

```python
    # ------------------------------------------------------------------
    # Profile baselines (Baseline Manager,
    # docs/superpowers/specs/2026-08-29-baseline-manager-design.md).
    # On-demand named baselines per cron profile. Separate from
    # strategy_footprints (period enum) and stat_baselines (single-row key).
    # ------------------------------------------------------------------

    def insert_profile_baseline(
        self,
        profile: str,
        strategy: str,
        label: str,
        params: dict[str, Any],
        window_start: date,
        window_end: date,
        metrics: dict[str, Any],
        pnl_values: list[float],
        sample_size: int,
        as_of_date: date | None = None,
        ticker: str | None = None,
        provider: str | None = None,
        run_id: str | None = None,
        code_commit: str | None = None,
        source_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        p_hash = strategy_params_hash(params or {})
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO profile_baselines (
                            profile, strategy, label, params, params_hash,
                            as_of_date, window_start, window_end, code_commit,
                            ticker, provider, sample_size, metrics, pnl_values,
                            run_id, source_meta
                        ) VALUES (
                            %s, %s, %s, %s::jsonb, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s::jsonb, %s::jsonb,
                            %s, %s::jsonb
                        )
                        RETURNING id
                        """,
                        (
                            profile, strategy, label, stable_json(params or {}), p_hash,
                            as_of_date, window_start, window_end, code_commit,
                            ticker, provider, int(sample_size),
                            stable_json(metrics or {}), stable_json(list(pnl_values or [])),
                            run_id, stable_json(source_meta or {}),
                        ),
                    )
                    new_id = _cursor_row(cur)["id"]
                conn.commit()
        except Exception as exc:  # noqa: BLE001 -- translate the unique violation
            if "profile_baselines" in str(exc) and "unique" in str(exc).lower():
                raise ValueError(f"duplicate_baseline_label:{profile}/{label}") from exc
            raise
        return {"id": new_id, "profile": profile, "label": label}

    def list_profile_baselines(self, profile: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM profile_baselines"
        params: list[Any] = []
        if profile:
            sql += " WHERE profile = %s"
            params.append(profile)
        sql += " ORDER BY created_at DESC"
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = _cursor_rows(cur)
        for row in rows:
            for key in ("as_of_date", "window_start", "window_end"):
                if isinstance(row.get(key), date):
                    row[key] = row[key].isoformat()
            self._normalize_dates(row, "created_at")
        return rows

    def get_profile_baseline(self, baseline_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM profile_baselines WHERE id = %s", (baseline_id,))
                row = _cursor_row(cur)
        if row:
            for key in ("as_of_date", "window_start", "window_end"):
                if isinstance(row.get(key), date):
                    row[key] = row[key].isoformat()
            self._normalize_dates(row, "created_at")
        return row

    def delete_profile_baseline(self, baseline_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM profile_baselines WHERE id = %s RETURNING id", (baseline_id,))
                row = _cursor_row(cur)
            conn.commit()
        return row is not None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bt-core/.venv/bin/python -m pytest tests/test_profile_baselines.py -v`
Expected: PASS (3 tests). If `LIVE_EVENTS_DB_DSN` is unset the tests SKIP — in that case run once with `set -a; source /home/htpc/backtrader/env/bt-live-events; set +a` first.

- [ ] **Step 5: Commit**

```bash
git add bt-core/watchtower_runtime.py bt-core/tests/test_profile_baselines.py
git commit -m "feat(watchtower): profile_baselines CRUD on WatchtowerRepository"
```

---

### Task 3: Pure helpers — trade extraction & backtest command

**Files:**
- Create: `bt-core/profile_baseline.py`
- Test: `bt-core/tests/test_profile_baseline_helpers.py` (new)

**Interfaces:**
- Consumes: `watchtower_runtime.baseline_metrics_from_trades`, `watchtower_runtime._safe_float`.
- Produces:
  - `extract_baseline_inputs(trades: list[dict]) -> dict` → `{"metrics": dict, "pnl_values": list[float], "sample_size": int}`.
  - `build_baseline_backtest_cmd(bt_core_python: str, strategy: str, ticker: str, provider: str, alpaca_feed: str, margin_leverage: str, fromdate: date, todate: date, stratargs_str: str, run_id: str) -> list[str]`.

- [ ] **Step 1: Write the failing test**

Create `bt-core/tests/test_profile_baseline_helpers.py`:

```python
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from profile_baseline import build_baseline_backtest_cmd, extract_baseline_inputs


class ExtractBaselineInputsTests(unittest.TestCase):
    def test_pulls_pnl_and_metrics(self):
        trades = [
            {"pnl_pct": 0.5, "asset": "MU", "entry_side": "long"},
            {"pnl_pct": -0.25, "asset": "ARM", "entry_side": "long"},
            {"pnl_percent": 0.1, "asset": "ON", "entry_side": "long"},
            {"asset": "NOPNL"},
        ]
        out = extract_baseline_inputs(trades)
        self.assertEqual(out["sample_size"], 3)
        self.assertEqual(sorted(out["pnl_values"]), [-0.25, 0.1, 0.5])
        self.assertIn("mean", out["metrics"])
        self.assertEqual(out["metrics"]["sample_size"], 3)

    def test_empty(self):
        out = extract_baseline_inputs([])
        self.assertEqual(out["sample_size"], 0)
        self.assertEqual(out["pnl_values"], [])


class BuildBacktestCmdTests(unittest.TestCase):
    def test_contains_window_and_mode(self):
        cmd = build_baseline_backtest_cmd(
            bt_core_python="/x/.venv/bin/python",
            strategy="overnight_ah.OvernightAH",
            ticker="allmib.json",
            provider="alpaca",
            alpaca_feed="iex",
            margin_leverage="1",
            fromdate=date(2015, 1, 1),
            todate=date(2026, 1, 1),
            stratargs_str="period=8 auction=True",
            run_id="footprint_x",
        )
        self.assertEqual(cmd[0], "/x/.venv/bin/python")
        self.assertIn("btmain.py", cmd)
        self.assertIn("--mode", cmd)
        self.assertIn("backtest", cmd)
        self.assertIn("--fromdate", cmd)
        self.assertIn("2015-01-01", cmd)
        self.assertIn("--todate", cmd)
        self.assertIn("2026-01-01", cmd)
        self.assertIn("--id", cmd)
        self.assertIn("footprint_x", cmd)
        self.assertIn("period=8 auction=True", cmd)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bt-core/.venv/bin/python -m pytest tests/test_profile_baseline_helpers.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'profile_baseline'`

- [ ] **Step 3: Create the module with the two helpers**

Create `bt-core/profile_baseline.py`:

```python
"""Baseline Manager core (docs/superpowers/specs/2026-08-29-baseline-manager-design.md).

Pure helpers + backtest orchestration for on-demand named baselines of a
scheduled cron profile. Mirrors bin/watchtower_compute_footprint_drift.py
for the btmain.py invocation, but the window is operator-chosen and the
result is persisted to profile_baselines (not strategy_footprints).

Decision D1a: parameters are historicised via
WatchtowerRepository.resolve_params_as_of(profile, as_of_date); the
backtest runs against the CURRENT code checkout (no git worktree).
"""

from __future__ import annotations

import subprocess
import sys
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import watchtower_runtime as wr

DEFAULT_ALPACA_FEED = "iex"
DEFAULT_MARGIN_LEVERAGE = "1"
RECENT_WINDOW_MIN = 3
RECENT_WINDOW_MAX = 30
RECENT_WINDOW_DEFAULT = 10


def extract_baseline_inputs(trades: list[dict[str, Any]]) -> dict[str, Any]:
    pnl_values = [
        v
        for v in (
            wr._safe_float(t.get("pnl_pct") if t.get("pnl_pct") is not None else t.get("pnl_percent"))
            for t in trades
        )
        if v is not None
    ]
    metrics = wr.baseline_metrics_from_trades(trades)
    sample_size = int(metrics.get("sample_size") or len(pnl_values))
    return {"metrics": metrics, "pnl_values": pnl_values, "sample_size": sample_size}


def build_baseline_backtest_cmd(
    bt_core_python: str,
    strategy: str,
    ticker: str,
    provider: str,
    alpaca_feed: str,
    margin_leverage: str,
    fromdate: date,
    todate: date,
    stratargs_str: str,
    run_id: str,
) -> list[str]:
    return [
        bt_core_python, "btmain.py",
        "--strat", strategy,
        "--ticker", ticker,
        "--fromdate", fromdate.isoformat(),
        "--todate", todate.isoformat(),
        "--timeframe", "daily",
        "--provider", provider,
        "--alpaca-feed", alpaca_feed,
        "--commission", "none",
        "--margin-leverage", str(margin_leverage),
        "--mode", "backtest",
        "--id", run_id,
        "--stratargs", stratargs_str,
    ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bt-core/.venv/bin/python -m pytest tests/test_profile_baseline_helpers.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add bt-core/profile_baseline.py bt-core/tests/test_profile_baseline_helpers.py
git commit -m "feat(baseline): pure helpers for trade extraction and backtest cmd"
```

---

### Task 4: Baseline context resolution

**Files:**
- Modify: `bt-core/profile_baseline.py`
- Modify: `bt-core/tests/test_profile_baseline_helpers.py`

**Interfaces:**
- Consumes: `WatchtowerRepository.resolve_params_as_of(profile, trading_date) -> dict` (returns a row with keys `strategy`, `stratargs` (dict), `metadata` (dict), `core_commit`); `build_stratargs_string` helper (define here).
- Produces:
  - `build_stratargs_string(stratargs: dict) -> str` — `"k1=v1 k2=v2"` with `repr()` values, matching `watchtower_compute_footprint_drift.build_stratargs_string`.
  - `resolve_baseline_context(repo, profile: str, profile_env: dict[str, str], as_of_date: date) -> dict` → `{"strategy": str, "stratargs": dict, "stratargs_str": str, "ticker": str, "provider": str, "alpaca_feed": str, "margin_leverage": str, "params_hash": str}`. `auction` is coerced to `True` in `stratargs` (backtest-meaningless otherwise — same as the footprint script). `ticker`/`provider` come from the version's `metadata` first, then `profile_env`, then defaults.

- [ ] **Step 1: Write the failing test**

Append to `bt-core/tests/test_profile_baseline_helpers.py`:

```python
from profile_baseline import build_stratargs_string, resolve_baseline_context
from watchtower_runtime import WatchtowerRepository


class ResolveBaselineContextTests(unittest.TestCase):
    def _repo(self, version):
        class StubRepo(WatchtowerRepository):
            def resolve_params_as_of(self_inner, profile, trading_date):
                return version
        return StubRepo()

    def test_uses_version_metadata_then_env(self):
        repo = self._repo({
            "strategy": "overnight_ah.OvernightAH",
            "stratargs": {"period": 8, "auction": False},
            "metadata": {"TICKER": "allmib.json"},
            "core_commit": "abc123",
        })
        ctx = resolve_baseline_context(
            repo, "development",
            profile_env={"DATA_PROVIDER": "alpaca", "TICKER": "ignored.json"},
            as_of_date=date(2026, 1, 1),
        )
        self.assertEqual(ctx["strategy"], "overnight_ah.OvernightAH")
        self.assertEqual(ctx["ticker"], "allmib.json")        # metadata wins over env
        self.assertEqual(ctx["provider"], "alpaca")           # from env
        self.assertTrue(ctx["stratargs"]["auction"])          # coerced True
        self.assertIn("period=8", ctx["stratargs_str"])
        self.assertEqual(len(ctx["params_hash"]), 64)

    def test_build_stratargs_string_uses_repr(self):
        self.assertEqual(build_stratargs_string({"a": 1, "b": "x"}), "a=1 b='x'")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bt-core/.venv/bin/python -m pytest tests/test_profile_baseline_helpers.py::ResolveBaselineContextTests -v`
Expected: FAIL — `ImportError: cannot import name 'resolve_baseline_context'`

- [ ] **Step 3: Add the two functions**

Append to `bt-core/profile_baseline.py`:

```python
def build_stratargs_string(stratargs: dict[str, Any]) -> str:
    return " ".join(f"{k}={v!r}" for k, v in stratargs.items())


def resolve_baseline_context(
    repo: "wr.WatchtowerRepository",
    profile: str,
    profile_env: dict[str, str],
    as_of_date: date,
) -> dict[str, Any]:
    version = repo.resolve_params_as_of(profile, as_of_date)
    meta = version.get("metadata") or {}
    stratargs = dict(version.get("stratargs") or {})
    # auction=False is a broker-execution flag, degenerate in a pure
    # backtest -- same coercion as watchtower_compute_footprint_drift.py.
    if not stratargs.get("auction", True):
        stratargs["auction"] = True

    ticker = meta.get("TICKER") or profile_env.get("TICKER") or ""
    provider = meta.get("DATA_PROVIDER") or profile_env.get("DATA_PROVIDER") or "yahoo"
    alpaca_feed = meta.get("ALPACA_FEED") or profile_env.get("ALPACA_FEED") or DEFAULT_ALPACA_FEED
    margin_leverage = str(meta.get("MARGIN_LEVERAGE") or profile_env.get("MARGIN_LEVERAGE") or DEFAULT_MARGIN_LEVERAGE)

    return {
        "strategy": version["strategy"],
        "stratargs": stratargs,
        "stratargs_str": build_stratargs_string(stratargs),
        "ticker": ticker,
        "provider": provider,
        "alpaca_feed": alpaca_feed,
        "margin_leverage": margin_leverage,
        "params_hash": wr.strategy_params_hash(stratargs),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bt-core/.venv/bin/python -m pytest tests/test_profile_baseline_helpers.py -v`
Expected: PASS (all classes, 7 tests total)

- [ ] **Step 5: Commit**

```bash
git add bt-core/profile_baseline.py bt-core/tests/test_profile_baseline_helpers.py
git commit -m "feat(baseline): resolve_baseline_context (params-as-of, D1a)"
```

---

### Task 5: End-to-end compute + drift orchestration

**Files:**
- Modify: `bt-core/profile_baseline.py`
- Modify: `bt-core/tests/test_profile_baseline_helpers.py`

**Interfaces:**
- Consumes: Task 3–4 functions; `WatchtowerRepository.insert_profile_baseline` / `get_profile_baseline` (Task 2); `watchtower_runtime.evaluate_outcomes`, `watchtower_runtime.monte_carlo_subset_test`, `watchtower_runtime.wasserstein_distance`.
- Produces:
  - `read_env_file(path: Path) -> dict[str, str]` — `KEY=VALUE` lines, `#` comments ignored (mirror the footprint script's `_read_env_file`).
  - `load_profile_env(profile: str, scheduled_dir: Path | None = None) -> dict[str, str]` — merges `~/.config/backtrader/scheduled/<profile>.env` and the `STRATEGY_CONFIG` file it points at (relative to `CODE_ROOT`). Returns `{}` if the profile env file is absent.
  - `run_baseline_backtest(bt_core_repo: Path, cmd: list[str], strategy: str, run_id: str) -> Path` — `subprocess.run(cmd, cwd=bt_core_repo, check=True)`, returns `out/<mod>/<Class>/<run_id>/trades.json`, raises `FileNotFoundError` if absent.
  - `compute_profile_baseline(repo, profile: str, label: str, window_start: date, window_end: date, as_of_date: date | None = None, profile_env: dict | None = None) -> dict` — full flow, persists, returns `repo.get_profile_baseline(new_id)`.
  - `compute_baseline_drift(repo, baseline: dict, recent_window_days: int = RECENT_WINDOW_DEFAULT, profile_env: dict | None = None) -> dict` — runs a recent backtest, compares to `baseline["metrics"]`/`baseline["pnl_values"]`, returns verdict dict with keys `status`, `confidence`, `score`, `z_mean`, `z_median`, `ks_distance`, `wasserstein_distance`, `monte_carlo`, `baseline_sample_size`, `recent_sample_size`, `recent_window_days`, `recent_window_start`, `recent_window_end`.

- [ ] **Step 1: Write the failing test**

Append to `bt-core/tests/test_profile_baseline_helpers.py`:

```python
import json
from profile_baseline import compute_baseline_drift, compute_profile_baseline


_FIXTURE_TRADES = [
    {"pnl_pct": 0.4, "asset": "MU", "entry_side": "long", "duration_bars": 1},
    {"pnl_pct": -0.2, "asset": "ARM", "entry_side": "long", "duration_bars": 1},
    {"pnl_pct": 0.15, "asset": "ON", "entry_side": "long", "duration_bars": 1},
    {"pnl_pct": 0.05, "asset": "TXN", "entry_side": "long", "duration_bars": 1},
]


class ComputeProfileBaselineTests(unittest.TestCase):
    def _repo(self):
        captured = {}

        class StubRepo(WatchtowerRepository):
            def resolve_params_as_of(self_inner, profile, trading_date):
                return {"strategy": "overnight_ah.OvernightAH", "stratargs": {"period": 8},
                        "metadata": {"TICKER": "allmib.json", "DATA_PROVIDER": "alpaca"},
                        "core_commit": "abc"}

            def insert_profile_baseline(self_inner, **kw):
                captured.update(kw)
                return {"id": 99, "profile": kw["profile"], "label": kw["label"]}

            def get_profile_baseline(self_inner, baseline_id):
                return {"id": baseline_id, **captured}

        return StubRepo(), captured

    def test_compute_persists_metrics_and_pnl(self):
        repo, captured = self._repo()
        import profile_baseline as pb
        orig = pb.run_baseline_backtest
        pb.run_baseline_backtest = lambda *a, **k: _write_fixture(self)
        try:
            row = compute_profile_baseline(
                repo, profile="development", label="hist-2026",
                window_start=date(2015, 1, 1), window_end=date(2026, 1, 1),
                as_of_date=date(2026, 1, 1), profile_env={},
            )
        finally:
            pb.run_baseline_backtest = orig
        self.assertEqual(row["id"], 99)
        self.assertEqual(captured["sample_size"], 4)
        self.assertEqual(len(captured["pnl_values"]), 4)
        self.assertIn("mean", captured["metrics"])
        self.assertEqual(captured["strategy"], "overnight_ah.OvernightAH")

    def test_drift_returns_verdict(self):
        baseline = {
            "metrics": __import__("watchtower_runtime").baseline_metrics_from_trades(_FIXTURE_TRADES),
            "pnl_values": [0.4, -0.2, 0.15, 0.05],
            "sample_size": 4,
            "strategy": "overnight_ah.OvernightAH",
            "profile": "development",
            "params": {"period": 8}, "as_of_date": "2026-01-01",
            "ticker": "allmib.json", "provider": "alpaca",
        }
        import profile_baseline as pb
        orig = pb.run_baseline_backtest
        pb.run_baseline_backtest = lambda *a, **k: _write_fixture(self)

        class StubRepo(WatchtowerRepository):
            def resolve_params_as_of(self_inner, profile, trading_date):
                return {"strategy": "overnight_ah.OvernightAH", "stratargs": {"period": 8},
                        "metadata": {"TICKER": "allmib.json", "DATA_PROVIDER": "alpaca"}, "core_commit": "abc"}
        try:
            verdict = compute_baseline_drift(StubRepo(), baseline, recent_window_days=10, profile_env={})
        finally:
            pb.run_baseline_backtest = orig
        self.assertIn(verdict["status"], ("ok", "warning", "missing_baseline"))
        self.assertEqual(verdict["recent_window_days"], 10)
        self.assertEqual(verdict["recent_sample_size"], 4)
        self.assertIn("confidence", verdict)


def _write_fixture(testcase):
    import tempfile
    d = Path(tempfile.mkdtemp())
    p = d / "trades.json"
    p.write_text(json.dumps(_FIXTURE_TRADES), encoding="utf-8")
    return p
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bt-core/.venv/bin/python -m pytest tests/test_profile_baseline_helpers.py::ComputeProfileBaselineTests -v`
Expected: FAIL — `ImportError: cannot import name 'compute_profile_baseline'`

- [ ] **Step 3: Implement the orchestration**

Append to `bt-core/profile_baseline.py`:

```python
BT_CORE = Path(__file__).resolve().parent
SCHEDULED_DIR = Path.home() / ".config" / "backtrader" / "scheduled"


def read_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def load_profile_env(profile: str, scheduled_dir: Path | None = None) -> dict[str, str]:
    scheduled_dir = scheduled_dir or SCHEDULED_DIR
    env = read_env_file(scheduled_dir / f"{profile}.env")
    if not env:
        return {}
    code_root = Path(env.get("CODE_ROOT") or BT_CORE.parent)
    strat_cfg = env.get("STRATEGY_CONFIG")
    if strat_cfg:
        strat_path = Path(strat_cfg)
        if not strat_path.is_absolute():
            strat_path = code_root / strat_path
        env = {**read_env_file(strat_path), **env}
    return env


def run_baseline_backtest(bt_core_repo: Path, cmd: list[str], strategy: str, run_id: str) -> Path:
    subprocess.run(cmd, cwd=str(bt_core_repo), check=True)
    parts = strategy.split(".")
    base = bt_core_repo / "out" / (parts[0].lower() if parts else strategy)
    if len(parts) == 2:
        base = base / parts[1]
    trades_path = base / run_id / "trades.json"
    if not trades_path.exists():
        raise FileNotFoundError(f"backtest produced no trades.json at {trades_path}")
    return trades_path


def _clamp_window(recent_window_days: int) -> int:
    try:
        n = int(recent_window_days)
    except (TypeError, ValueError):
        n = RECENT_WINDOW_DEFAULT
    return max(RECENT_WINDOW_MIN, min(RECENT_WINDOW_MAX, n))


def _recent_window_bounds(recent_window_days: int, today: date | None = None) -> tuple[date, date]:
    today = today or date.today()
    # Approximate N trading days as N*7/5 calendar days + a 5-day cushion for
    # holidays. Deliberately generous: the backtest itself only produces
    # trades on real sessions, so a slightly wider fromdate is harmless.
    span = int(round(recent_window_days * 7 / 5)) + 5
    return today - timedelta(days=span), today


def compute_profile_baseline(
    repo: "wr.WatchtowerRepository",
    profile: str,
    label: str,
    window_start: date,
    window_end: date,
    as_of_date: date | None = None,
    profile_env: dict[str, str] | None = None,
) -> dict[str, Any]:
    if window_end < window_start:
        raise ValueError("window_end_before_window_start")
    as_of = as_of_date or window_end
    env = profile_env if profile_env is not None else load_profile_env(profile)
    ctx = resolve_baseline_context(repo, profile, env, as_of)
    if not ctx["ticker"]:
        raise ValueError(f"no TICKER resolvable for profile={profile!r} (version metadata / env)")

    code_root = Path(env.get("CODE_ROOT") or BT_CORE.parent)
    bt_core_repo = code_root / "bt-core"
    bt_core_python = str(bt_core_repo / ".venv" / "bin" / "python")
    run_id = f"baseline_{profile}_{uuid.uuid4().hex[:8]}"

    cmd = build_baseline_backtest_cmd(
        bt_core_python=bt_core_python, strategy=ctx["strategy"], ticker=ctx["ticker"],
        provider=ctx["provider"], alpaca_feed=ctx["alpaca_feed"],
        margin_leverage=ctx["margin_leverage"], fromdate=window_start, todate=window_end,
        stratargs_str=ctx["stratargs_str"], run_id=run_id,
    )
    trades_path = run_baseline_backtest(bt_core_repo, cmd, ctx["strategy"], run_id)
    trades = __import__("json").loads(trades_path.read_text(encoding="utf-8"))
    parsed = extract_baseline_inputs(trades)

    created = repo.insert_profile_baseline(
        profile=profile, strategy=ctx["strategy"], label=label,
        params=ctx["stratargs"], window_start=window_start, window_end=window_end,
        metrics=parsed["metrics"], pnl_values=parsed["pnl_values"], sample_size=parsed["sample_size"],
        as_of_date=as_of, ticker=ctx["ticker"], provider=ctx["provider"], run_id=run_id,
        source_meta={"builder": "profile_baseline.compute_profile_baseline", "trades_path": str(trades_path)},
    )
    return repo.get_profile_baseline(created["id"])


def compute_baseline_drift(
    repo: "wr.WatchtowerRepository",
    baseline: dict[str, Any],
    recent_window_days: int = RECENT_WINDOW_DEFAULT,
    profile_env: dict[str, str] | None = None,
) -> dict[str, Any]:
    n = _clamp_window(recent_window_days)
    profile = baseline["profile"]
    env = profile_env if profile_env is not None else load_profile_env(profile)
    win_start, win_end = _recent_window_bounds(n)
    ctx = resolve_baseline_context(repo, profile, env, win_end)

    code_root = Path(env.get("CODE_ROOT") or BT_CORE.parent)
    bt_core_repo = code_root / "bt-core"
    bt_core_python = str(bt_core_repo / ".venv" / "bin" / "python")
    run_id = f"baselinedrift_{profile}_{uuid.uuid4().hex[:8]}"
    cmd = build_baseline_backtest_cmd(
        bt_core_python=bt_core_python, strategy=ctx["strategy"], ticker=ctx["ticker"],
        provider=ctx["provider"], alpaca_feed=ctx["alpaca_feed"],
        margin_leverage=ctx["margin_leverage"], fromdate=win_start, todate=win_end,
        stratargs_str=ctx["stratargs_str"], run_id=run_id,
    )
    trades_path = run_baseline_backtest(bt_core_repo, cmd, ctx["strategy"], run_id)
    trades = __import__("json").loads(trades_path.read_text(encoding="utf-8"))
    recent = extract_baseline_inputs(trades)

    baseline_pnl = list(baseline.get("pnl_values") or [])
    outcome = wr.evaluate_outcomes(recent["pnl_values"], baseline.get("metrics") or {})
    mc = wr.monte_carlo_subset_test(baseline_pnl, recent["pnl_values"], seed=f"{profile}:{baseline.get('id')}")
    wdist = wr.wasserstein_distance(baseline_pnl, recent["pnl_values"])
    return {
        "status": outcome["status"],
        "confidence": outcome.get("confidence"),
        "score": outcome.get("score"),
        "z_mean": outcome.get("metrics", {}).get("z_mean"),
        "z_median": outcome.get("metrics", {}).get("z_median"),
        "ks_distance": outcome.get("metrics", {}).get("ks_distance"),
        "wasserstein_distance": wdist,
        "monte_carlo": mc,
        "baseline_sample_size": int(baseline.get("sample_size") or len(baseline_pnl)),
        "recent_sample_size": recent["sample_size"],
        "recent_window_days": n,
        "recent_window_start": win_start.isoformat(),
        "recent_window_end": win_end.isoformat(),
    }
```

Note: `evaluate_outcomes` tolerates a metrics dict that is a superset (it reads `mean`, `median`, `stddev`, `mad`, `sample_size`, `histogram`); `baseline_metrics_from_trades` output contains all of these via the `**summary, **moments` spread. If `evaluate_outcomes` returns `status == "missing_baseline"` the verdict still returns cleanly (empty baseline).

- [ ] **Step 4: Run test to verify it passes**

Run: `bt-core/.venv/bin/python -m pytest tests/test_profile_baseline_helpers.py -v`
Expected: PASS (all, 11 tests)

- [ ] **Step 5: Run the full watchtower test file to check no regression**

Run: `bt-core/.venv/bin/python -m pytest tests/test_watchtower_runtime.py tests/test_profile_baselines.py tests/test_profile_baseline_helpers.py -v`
Expected: all PASS (or SKIP for the DB-guarded ones if DSN unset)

- [ ] **Step 6: Commit**

```bash
git add bt-core/profile_baseline.py bt-core/tests/test_profile_baseline_helpers.py
git commit -m "feat(baseline): compute_profile_baseline + compute_baseline_drift orchestration"
```

---

### Task 6: bt-api endpoints + background job

**Files:**
- Modify: `bt-api/app/watchtower.py` (imports near top; job dict near the existing `_baseline_jobs = {}` at line ~19; routes near the existing `/watchtower/cron/<profile>/overview` at line ~1122)

**Interfaces:**
- Consumes: `repo` singleton; `profile_baseline.compute_profile_baseline`, `profile_baseline.compute_baseline_drift`, `profile_baseline.load_profile_env`, `profile_baseline._clamp_window`.
- Produces (all under the blueprint's `/dyn/obs` prefix):
  - `GET  /watchtower/cron/<profile>/baselines` → `200 [ {baseline row}, ... ]`
  - `POST /watchtower/cron/<profile>/baselines` body `{"label", "window_start":"YYYY-MM-DD", "window_end":"YYYY-MM-DD", "as_of_date"?:"YYYY-MM-DD"}` → `202 {"job_id","status":"queued"}`; `400` on bad/missing fields.
  - `GET  /watchtower/cron/baselines/jobs/<job_id>` → `200 {job_id,status,done,error,baseline_id?}` ; `404` if unknown.
  - `DELETE /watchtower/cron/<profile>/baselines/<int:baseline_id>` → `200 {"deleted": true}` or `404`.
  - `GET  /watchtower/cron/<profile>/baselines/<int:baseline_id>/drift?recent_window_days=N` → `200 {verdict}` ; `404` if baseline id not found or not owned by `<profile>`.

- [ ] **Step 1: Add imports and the job registry**

At the top of `bt-api/app/watchtower.py`, after the existing `from watchtower_runtime import ...` line, add:

```python
import profile_baseline as _pbl
```

Next to the existing `_baseline_jobs = {}` / `_baseline_jobs_lock = threading.Lock()` (around line 19), add:

```python
_profile_baseline_jobs = {}
_profile_baseline_jobs_lock = threading.Lock()
```

- [ ] **Step 2: Add the job runner helper**

After the existing `_start_baseline_job` function, add:

```python
def _pbl_job_snapshot(job_id):
    with _profile_baseline_jobs_lock:
        job = _profile_baseline_jobs.get(job_id)
        return dict(job) if job else None


def _pbl_job_update(job_id, **updates):
    with _profile_baseline_jobs_lock:
        job = _profile_baseline_jobs.get(job_id)
        if job:
            job.update(updates)
            job["updated_at"] = time.time()


def _start_profile_baseline_job(profile, payload):
    def _parse_date(key, required=True):
        raw = str(payload.get(key) or "").strip()
        if not raw:
            if required:
                raise ValueError(f"{key} is required")
            return None
        try:
            return date.fromisoformat(raw)
        except ValueError:
            raise ValueError(f"{key} must be YYYY-MM-DD")

    label = str(payload.get("label") or "").strip()
    if not label:
        raise ValueError("label is required")
    window_start = _parse_date("window_start")
    window_end = _parse_date("window_end")
    as_of_date = _parse_date("as_of_date", required=False)
    if window_end < window_start:
        raise ValueError("window_end must be on or after window_start")

    job_id = uuid.uuid4().hex
    with _profile_baseline_jobs_lock:
        _profile_baseline_jobs[job_id] = {
            "job_id": job_id, "status": "queued", "done": False,
            "profile": profile, "label": label, "baseline_id": None,
            "created_at": time.time(), "updated_at": time.time(), "error": None,
        }

    def _runner():
        _pbl_job_update(job_id, status="running")
        try:
            row = _pbl.compute_profile_baseline(
                repo, profile=profile, label=label,
                window_start=window_start, window_end=window_end, as_of_date=as_of_date,
            )
            _pbl_job_update(job_id, status="completed", done=True, baseline_id=row.get("id"))
        except Exception as exc:  # noqa: BLE001
            _pbl_job_update(job_id, status="failed", done=True, error=str(exc))

    threading.Thread(target=_runner, name=f"pbl-{job_id[:8]}", daemon=True).start()
    return _pbl_job_snapshot(job_id)
```

- [ ] **Step 3: Add the five routes**

After the existing `watchtower_cron_overview` route (`@obs_bp.route("/watchtower/cron/<profile>/overview" ...)`), add:

```python
@obs_bp.route("/watchtower/cron/<profile>/baselines", methods=["GET"])
def watchtower_cron_baselines(profile):
    missing = _require_repo()
    if missing:
        return missing
    return jsonify(repo.list_profile_baselines(profile))


@obs_bp.route("/watchtower/cron/<profile>/baselines", methods=["POST"])
def watchtower_cron_baseline_create(profile):
    missing = _require_repo()
    if missing:
        return missing
    payload = request.get_json(silent=True) or {}
    try:
        job = _start_profile_baseline_job(profile, payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(job), 202


@obs_bp.route("/watchtower/cron/baselines/jobs/<job_id>", methods=["GET"])
def watchtower_cron_baseline_job(job_id):
    job = _pbl_job_snapshot(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    return jsonify(job)


@obs_bp.route("/watchtower/cron/<profile>/baselines/<int:baseline_id>", methods=["DELETE"])
def watchtower_cron_baseline_delete(profile, baseline_id):
    missing = _require_repo()
    if missing:
        return missing
    existing = repo.get_profile_baseline(baseline_id)
    if not existing or existing.get("profile") != profile:
        return jsonify({"error": "baseline not found"}), 404
    return jsonify({"deleted": repo.delete_profile_baseline(baseline_id)})


@obs_bp.route("/watchtower/cron/<profile>/baselines/<int:baseline_id>/drift", methods=["GET"])
def watchtower_cron_baseline_drift(profile, baseline_id):
    missing = _require_repo()
    if missing:
        return missing
    baseline = repo.get_profile_baseline(baseline_id)
    if not baseline or baseline.get("profile") != profile:
        return jsonify({"error": "baseline not found"}), 404
    window = _pbl._clamp_window(request.args.get("recent_window_days", _pbl.RECENT_WINDOW_DEFAULT))
    try:
        verdict = _pbl.compute_baseline_drift(repo, baseline, recent_window_days=window)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500
    return jsonify(verdict)
```

- [ ] **Step 4: Restart bt-api@dev and smoke-test the read endpoints**

Run:
```bash
systemctl --user restart bt-api@dev
sleep 3
BASE=http://127.0.0.1:9090/dyn/obs/watchtower/cron
curl -sS "$BASE/never-seen/baselines"            # expect: []
curl -sS "$BASE/development/baselines"            # expect: [] (nothing created yet)
curl -sS "$BASE/baselines/jobs/deadbeef"         # expect: {"error":"job not found"}  (404)
curl -sS -X POST "$BASE/development/baselines" -H 'Content-Type: application/json' \
     -d '{"label":"x"}'                           # expect: {"error":"window_start is required"} (400)
```
Expected: exactly the shapes in the comments. (Port: check `env/bt-api-dev` for `SERVER_PORT`; the roadmap notes dev = 9090.)

- [ ] **Step 5: Commit**

```bash
git add bt-api/app/watchtower.py
git commit -m "feat(bt-api): profile baseline endpoints + background compute job"
```

---

### Task 7: CLI wrapper for manual baseline runs

**Files:**
- Create: `bin/watchtower_build_profile_baseline.py`

**Interfaces:**
- Consumes: `profile_baseline.compute_profile_baseline`, `WatchtowerRepository`.
- Produces: CLI `--profile --label --from --to [--as-of] [--db-dsn]`; `--print-cmd` prints the resolved backtest command without running it.

- [ ] **Step 1: Create the script**

Create `bin/watchtower_build_profile_baseline.py`:

```python
#!/usr/bin/env python3
"""Manual driver for the Baseline Manager (on-demand, not scheduled).

Runs a long backtest for a scheduled cron profile over an operator-chosen
window and persists it to profile_baselines. Same core as the bt-api
POST /watchtower/cron/<profile>/baselines endpoint.

    bin/watchtower_build_profile_baseline.py \
        --profile development --label hist-2026 \
        --from 2015-01-01 --to 2026-01-01
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

BT_CORE = Path(__file__).resolve().parent.parent / "bt-core"
if str(BT_CORE) not in sys.path:
    sys.path.insert(0, str(BT_CORE))

import profile_baseline as pbl  # noqa: E402
from watchtower_runtime import WatchtowerRepository  # noqa: E402


def _d(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--from", dest="frm", required=True, type=_d)
    ap.add_argument("--to", dest="to", required=True, type=_d)
    ap.add_argument("--as-of", dest="as_of", default=None, type=_d)
    ap.add_argument("--db-dsn", default=None)
    ap.add_argument("--print-cmd", action="store_true", help="resolve and print the backtest command, do not run")
    args = ap.parse_args(argv)

    repo = WatchtowerRepository(args.db_dsn)
    if not repo.available():
        print("Postgres DSN missing", file=sys.stderr)
        return 2

    env = pbl.load_profile_env(args.profile)
    if args.print_cmd:
        ctx = pbl.resolve_baseline_context(repo, args.profile, env, args.as_of or args.to)
        code_root = Path(env.get("CODE_ROOT") or BT_CORE.parent)
        cmd = pbl.build_baseline_backtest_cmd(
            bt_core_python=str(code_root / "bt-core" / ".venv" / "bin" / "python"),
            strategy=ctx["strategy"], ticker=ctx["ticker"], provider=ctx["provider"],
            alpaca_feed=ctx["alpaca_feed"], margin_leverage=ctx["margin_leverage"],
            fromdate=args.frm, todate=args.to, stratargs_str=ctx["stratargs_str"],
            run_id=f"baseline_{args.profile}_PREVIEW",
        )
        print(" ".join(cmd))
        return 0

    row = pbl.compute_profile_baseline(
        repo, profile=args.profile, label=args.label,
        window_start=args.frm, window_end=args.to, as_of_date=args.as_of, profile_env=env,
    )
    print(json.dumps({"id": row["id"], "label": row["label"], "sample_size": row["sample_size"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Smoke-test `--help` and `--print-cmd`**

Run:
```bash
cd /home/htpc/backtrader
chmod +x bin/watchtower_build_profile_baseline.py
bt-core/.venv/bin/python bin/watchtower_build_profile_baseline.py --help
set -a; source env/bt-live-events; set +a
bt-core/.venv/bin/python bin/watchtower_build_profile_baseline.py \
    --profile development --label preview --from 2026-08-01 --to 2026-08-20 --print-cmd
```
Expected: `--help` prints usage; `--print-cmd` prints a `.../python btmain.py --strat overnight_ah.OvernightAH --ticker ... --fromdate 2026-08-01 --todate 2026-08-20 --mode backtest --id baseline_development_PREVIEW --stratargs ...` line. Do NOT run a full baseline here (multi-year backtest, RAM budget — user runs that deliberately).

- [ ] **Step 3: Commit**

```bash
git add bin/watchtower_build_profile_baseline.py
git commit -m "feat(baseline): bin/watchtower_build_profile_baseline.py manual driver"
```

---

### Task 8: bt-dash — "Gestione baseline" page

**Files:**
- Create: `bt-dash/src/bt/pages/ProfileBaselineManager.vue`
- Modify: `bt-dash/src/router/routes.js` (add route next to `/ScheduledProfiles/Configuration`)
- Modify: `bt-dash/src/layouts/MainLayout.vue` (add menu item in the "Strategie Schedulate" `q-expansion-item`)

**Interfaces:**
- Consumes: `GET /dyn/obs/watchtower/cron/profiles`, `GET|POST|DELETE /dyn/obs/watchtower/cron/<profile>/baselines`, `GET /dyn/obs/watchtower/cron/baselines/jobs/<job_id>`.
- Produces: route `/ScheduledProfiles/Baselines`.

- [ ] **Step 1: Create the page**

Create `bt-dash/src/bt/pages/ProfileBaselineManager.vue`:

```vue
<template>
  <q-page class="q-pa-md">
    <div class="text-h5">Strategie Schedulate — Gestione baseline</div>
    <div class="text-caption text-grey-7 q-mb-md">
      Baseline statistiche on-demand (run di backtest su finestra lunga). Servono come
      riferimento per il confronto di stabilità nella pagina Configurazione.
    </div>

    <q-card flat bordered class="q-mb-md">
      <q-card-section class="row q-col-gutter-md items-end">
        <div class="col-12 col-md-4">
          <q-select
            v-model="selectedProfile" use-input fill-input hide-selected hide-dropdown-icon
            clearable input-debounce="0" new-value-mode="add-unique" :options="filteredOptions"
            label="Profilo" outlined dense
            @filter="filterProfiles" @input-value="v => profileText = v || ''"
            @update:model-value="v => { profileText = v || ''; loadBaselines() }"
            @clear="() => { profileText = ''; baselines = [] }"
          />
        </div>
        <div class="col-auto">
          <q-btn flat round icon="refresh" :loading="loading" :disable="!profile" @click="loadBaselines" />
        </div>
      </q-card-section>
    </q-card>

    <template v-if="profile">
      <q-banner v-if="loadError" class="bg-red-1 text-red-9 q-mb-md" rounded>{{ loadError }}</q-banner>

      <q-card flat bordered class="q-mb-md">
        <q-card-section class="text-subtitle2 text-grey-8">Nuova baseline per <code>{{ profile }}</code></q-card-section>
        <q-separator />
        <q-card-section class="row q-col-gutter-md items-end">
          <div class="col-12 col-md-3">
            <q-input v-model="form.label" label="Nome (label)" outlined dense />
          </div>
          <div class="col-6 col-md-2">
            <q-input v-model="form.window_start" label="Data inizio" mask="####-##-##" outlined dense hint="YYYY-MM-DD" />
          </div>
          <div class="col-6 col-md-2">
            <q-input v-model="form.window_end" label="Data fine" mask="####-##-##" outlined dense hint="YYYY-MM-DD" />
          </div>
          <div class="col-6 col-md-2">
            <q-input v-model="form.as_of_date" label="Versione (as-of)" mask="####-##-##" outlined dense
                     hint="opz. — STRATARGS a questa data" />
          </div>
          <div class="col-auto">
            <q-btn color="primary" label="Calcola" icon="play_arrow" :loading="jobRunning"
                   :disable="!canSubmit" @click="startJob" />
          </div>
          <div class="col-12">
            <div class="text-caption text-grey-6">
              Il backtest gira sul codice corrente del checkout (solo i parametri sono storicizzati alla data as-of).
            </div>
            <div v-if="jobState" class="text-body2 q-mt-xs" :class="jobStateClass">{{ jobStateText }}</div>
          </div>
        </q-card-section>
      </q-card>

      <q-card flat bordered>
        <q-card-section class="text-subtitle2 text-grey-8">Baseline esistenti</q-card-section>
        <q-separator />
        <q-table :rows="baselines" :columns="columns" row-key="id" dense flat
                 :pagination="{ rowsPerPage: 10 }" :loading="loading">
          <template #body-cell-actions="props">
            <q-td :props="props">
              <q-btn flat dense round icon="delete" color="negative" @click="removeBaseline(props.row)" />
            </q-td>
          </template>
        </q-table>
      </q-card>
    </template>

    <div v-else class="text-grey-6 q-pa-lg text-center">Digita un profilo per gestirne le baseline.</div>
  </q-page>
</template>

<script>
import { api } from 'boot/axios'
import { Notify } from 'quasar'

export default {
  name: 'ProfileBaselineManager',
  data() {
    return {
      knownProfiles: [], filteredOptions: [], selectedProfile: null, profileText: '',
      baselines: [], loading: false, loadError: '',
      form: { label: '', window_start: '2000-01-01', window_end: `${new Date().getFullYear()}-01-01`, as_of_date: '' },
      jobId: null, jobState: null, jobTimer: null,
    }
  },
  computed: {
    profile() { return (this.profileText || '').trim() },
    columns() {
      return [
        { name: 'label', label: 'Nome', field: 'label', align: 'left' },
        { name: 'window', label: 'Finestra', field: (r) => `${r.window_start} → ${r.window_end}`, align: 'left' },
        { name: 'as_of_date', label: 'Versione as-of', field: 'as_of_date', align: 'left' },
        { name: 'sample_size', label: 'Trade', field: 'sample_size', align: 'right' },
        { name: 'created_at', label: 'Calcolata', field: (r) => String(r.created_at || '').slice(0, 16).replace('T', ' '), align: 'left' },
        { name: 'actions', label: '', field: 'actions', align: 'right' },
      ]
    },
    canSubmit() {
      return this.profile && this.form.label.trim() &&
        /^\d{4}-\d{2}-\d{2}$/.test(this.form.window_start) && /^\d{4}-\d{2}-\d{2}$/.test(this.form.window_end)
    },
    jobRunning() { return this.jobState && !this.jobState.done },
    jobStateText() {
      if (!this.jobState) return ''
      const s = this.jobState
      if (s.status === 'completed') return `Baseline creata (id ${s.baseline_id}).`
      if (s.status === 'failed') return `Job fallito: ${s.error || 'errore sconosciuto'}`
      return 'Backtest in corso… (può richiedere minuti)'
    },
    jobStateClass() {
      if (!this.jobState) return ''
      return { completed: 'text-positive', failed: 'text-negative' }[this.jobState.status] || 'text-grey-8'
    },
  },
  mounted() { this.loadProfiles() },
  beforeUnmount() { if (this.jobTimer) clearInterval(this.jobTimer) },
  methods: {
    async loadProfiles() {
      try {
        const { data } = await api.get('/dyn/obs/watchtower/cron/profiles')
        this.knownProfiles = (data || []).map((p) => p.profile).filter(Boolean)
      } catch (e) { this.knownProfiles = [] }
    },
    filterProfiles(val, update) {
      update(() => {
        const n = (val || '').toLowerCase()
        this.filteredOptions = n ? this.knownProfiles.filter((p) => p.toLowerCase().includes(n)) : this.knownProfiles.slice()
      })
    },
    async loadBaselines() {
      if (!this.profile) return
      this.loading = true; this.loadError = ''
      try {
        const { data } = await api.get(`/dyn/obs/watchtower/cron/${encodeURIComponent(this.profile)}/baselines`)
        this.baselines = data || []
      } catch (e) {
        this.loadError = `Impossibile caricare le baseline: ${e?.message || e}`; this.baselines = []
      } finally { this.loading = false }
    },
    async startJob() {
      try {
        const { data } = await api.post(`/dyn/obs/watchtower/cron/${encodeURIComponent(this.profile)}/baselines`, {
          label: this.form.label.trim(),
          window_start: this.form.window_start,
          window_end: this.form.window_end,
          as_of_date: this.form.as_of_date || undefined,
        })
        this.jobId = data.job_id
        this.jobState = data
        this.pollJob()
      } catch (e) {
        Notify.create({ type: 'negative', message: e?.response?.data?.error || e?.message || 'Errore avvio job' })
      }
    },
    pollJob() {
      if (this.jobTimer) clearInterval(this.jobTimer)
      this.jobTimer = setInterval(async () => {
        try {
          const { data } = await api.get(`/dyn/obs/watchtower/cron/baselines/jobs/${this.jobId}`)
          this.jobState = data
          if (data.done) {
            clearInterval(this.jobTimer); this.jobTimer = null
            if (data.status === 'completed') { this.form.label = ''; this.loadBaselines() }
          }
        } catch (e) { clearInterval(this.jobTimer); this.jobTimer = null }
      }, 3000)
    },
    async removeBaseline(row) {
      try {
        await api.delete(`/dyn/obs/watchtower/cron/${encodeURIComponent(this.profile)}/baselines/${row.id}`)
        Notify.create({ type: 'positive', message: `Baseline "${row.label}" eliminata`, timeout: 1200 })
        this.loadBaselines()
      } catch (e) {
        Notify.create({ type: 'negative', message: `Eliminazione non riuscita: ${e?.message || e}` })
      }
    },
  },
}
</script>
```

- [ ] **Step 2: Add the route**

In `bt-dash/src/router/routes.js`, after the `/ScheduledProfiles/Configuration` line, add:

```javascript
      { path: '/ScheduledProfiles/Baselines', component: () => import('src/bt/pages/ProfileBaselineManager.vue')},
```

- [ ] **Step 3: Add the menu item**

In `bt-dash/src/layouts/MainLayout.vue`, inside the `q-expansion-item` labelled `"Strategie Schedulate"`, after the `"Configurazione"` `q-item`, add:

```html
          <q-item to="/ScheduledProfiles/Baselines" active-class="q-item-no-link-highlighting" class="q-pl-xl">
            <q-item-section>
              <q-item-label>Gestione baseline</q-item-label>
            </q-item-section>
          </q-item>
```

Also extend `isScheduledRoute` in the same file's `setup()` so the group stays open on this route:

```javascript
    const isScheduledRoute = computed(
      () => currentPath.value.startsWith('/ScheduledProfiles')
        || currentPath.value === '/Watchtower/CronMonitoring'
    )
```
(`startsWith('/ScheduledProfiles')` already covers `/ScheduledProfiles/Baselines` — verify no change needed; if the existing computed differs, make it match the snippet above.)

- [ ] **Step 4: Compile-check and build**

Run:
```bash
cd /home/htpc/backtrader/bt-dash
node -e 'const fs=require("fs");const {parse,compileTemplate,compileScript}=require("@vue/compiler-sfc");
for(const f of ["src/bt/pages/ProfileBaselineManager.vue","src/layouts/MainLayout.vue"]){
 const src=fs.readFileSync(f,"utf8");const {descriptor,errors}=parse(src,{filename:f});
 if(errors.length){console.log("PARSE",f,errors);process.exit(1)}
 const s=compileScript(descriptor,{id:f});
 const t=compileTemplate({source:descriptor.template.content,filename:f,id:f,compilerOptions:{bindingMetadata:s.bindings}});
 if(t.errors.length){console.log("TPL",f,t.errors);process.exit(1)} console.log("OK",f)}'
node --check src/router/routes.js
export PATH="/home/htpc/.nvm/versions/node/v20.20.0/bin:$PATH" && npx quasar build -m pwa 2>&1 | grep -E "Build succeeded|Build failed"
```
Expected: `OK ...` for both SFCs, `OK` routes.js check (no output = pass), `Build succeeded`.

- [ ] **Step 5: Commit**

```bash
git add src/bt/pages/ProfileBaselineManager.vue src/router/routes.js src/layouts/MainLayout.vue
git commit -m "feat(bt-dash): Gestione baseline page + route + menu entry"
```

---

### Task 9: bt-dash — Configurazione page: baseline selector + drift panel

**Files:**
- Modify: `bt-dash/src/bt/pages/ScheduledProfileConfiguration.vue`

**Interfaces:**
- Consumes: `GET /dyn/obs/watchtower/cron/<profile>/baselines`, `GET /dyn/obs/watchtower/cron/<profile>/baselines/<id>/drift?recent_window_days=N`.
- Produces: no new exports; UI section "Baseline / stabilità strategia".

- [ ] **Step 1: Rename the level-2 indicator to remove ambiguity**

In `ScheduledProfileConfiguration.vue`, in the `indicators()` computed, change the footprint indicator's `label` from `'Baseline footprint calcolata'` to `'Footprint pre/post attivazione'` and its non-ok `detail` text to:
`'Nessun footprint pre/post: lo script watchtower_compute_footprint_drift.py non è schedulato, va lanciato a mano (step 7). Diverso dalla baseline gestita qui sotto. Non blocca l\'attivazione.'`

- [ ] **Step 2: Add data + load logic**

In `data()` add:
```javascript
      baselineOptions: [],
      selectedBaselineId: null,
      drift: null,
      driftLoading: false,
      recentWindowDays: 10,
```
In `loadStatus()` (after `this.overview = data`), add a call `this.loadBaselineOptions()`. Add methods:
```javascript
    async loadBaselineOptions() {
      const p = this.profile
      if (!p) { this.baselineOptions = []; return }
      try {
        const { data } = await api.get(`/dyn/obs/watchtower/cron/${encodeURIComponent(p)}/baselines`)
        this.baselineOptions = (data || []).map((b) => ({
          label: `${b.label} (${b.window_start}→${b.window_end}, ${b.sample_size} trade)`, value: b.id,
        }))
      } catch (e) { this.baselineOptions = [] }
    },
    async checkDrift() {
      if (!this.selectedBaselineId) { this.drift = null; return }
      this.driftLoading = true
      try {
        const { data } = await api.get(
          `/dyn/obs/watchtower/cron/${encodeURIComponent(this.profile)}/baselines/${this.selectedBaselineId}/drift`,
          { params: { recent_window_days: this.recentWindowDays } },
        )
        this.drift = data
      } catch (e) {
        this.drift = { error: e?.response?.data?.error || e?.message || 'errore' }
      } finally { this.driftLoading = false }
    },
    driftHeadlineIt() {
      if (!this.drift) return ''
      if (this.drift.error) return `Errore: ${this.drift.error}`
      if (this.drift.status === 'warning') return 'Possibile regime sfavorevole: il backtest recente si discosta dalla baseline.'
      if (this.drift.status === 'missing_baseline') return 'Baseline senza campione sufficiente.'
      return 'Backtest recente statisticamente coerente con la baseline.'
    },
```

- [ ] **Step 3: Add the template section**

In `ScheduledProfileConfiguration.vue`, immediately before the `<!-- Sezione 4 — Link successivo -->` comment, add:

```html
      <!-- Baseline / stabilità strategia (livello 1) -->
      <q-card flat bordered class="q-mb-md">
        <q-card-section class="text-subtitle2 text-grey-8">Baseline / stabilità strategia</q-card-section>
        <q-separator />
        <q-card-section class="row q-col-gutter-md items-end">
          <div class="col-12 col-md-6">
            <q-select v-model="selectedBaselineId" :options="baselineOptions" label="Baseline di confronto"
                      dense outlined emit-value map-options clearable
                      @update:model-value="checkDrift" />
          </div>
          <div class="col-6 col-md-3">
            <q-input v-model.number="recentWindowDays" type="number" min="3" max="30"
                     label="Giorni recenti" dense outlined @blur="checkDrift" />
          </div>
          <div class="col-auto">
            <q-btn flat icon="refresh" label="Verifica" :loading="driftLoading"
                   :disable="!selectedBaselineId" @click="checkDrift" />
          </div>
          <div class="col-auto">
            <q-btn flat color="primary" label="Gestione baseline" icon-right="open_in_new"
                   to="/ScheduledProfiles/Baselines" />
          </div>
        </q-card-section>
        <q-card-section v-if="drift && !drift.error">
          <div class="text-body1" :class="drift.status === 'warning' ? 'text-orange-9' : 'text-positive'">
            {{ driftHeadlineIt() }}
          </div>
          <div class="text-caption text-grey-7 q-mt-xs">
            confidence {{ drift.confidence != null ? (drift.confidence * 100).toFixed(0) + '%' : '—' }} ·
            z-mean {{ drift.z_mean != null ? drift.z_mean.toFixed(2) : '—' }} ·
            KS {{ drift.ks_distance != null ? drift.ks_distance.toFixed(3) : '—' }} ·
            campione recente {{ drift.recent_sample_size }} trade
            ({{ drift.recent_window_start }}→{{ drift.recent_window_end }}) vs baseline {{ drift.baseline_sample_size }}
          </div>
        </q-card-section>
        <q-card-section v-else-if="drift && drift.error" class="text-negative">{{ drift.error }}</q-card-section>
        <q-card-section v-else class="text-grey-6">
          Seleziona una baseline per verificare la coerenza del backtest recente.
        </q-card-section>
      </q-card>
```

- [ ] **Step 4: Compile-check and build**

Run:
```bash
cd /home/htpc/backtrader/bt-dash
node -e 'const fs=require("fs");const {parse,compileTemplate,compileScript}=require("@vue/compiler-sfc");
const f="src/bt/pages/ScheduledProfileConfiguration.vue";const src=fs.readFileSync(f,"utf8");
const {descriptor,errors}=parse(src,{filename:f});if(errors.length){console.log(errors);process.exit(1)}
const s=compileScript(descriptor,{id:f});
const t=compileTemplate({source:descriptor.template.content,filename:f,id:f,compilerOptions:{bindingMetadata:s.bindings}});
if(t.errors.length){console.log(t.errors);process.exit(1)}console.log("SFC OK")'
export PATH="/home/htpc/.nvm/versions/node/v20.20.0/bin:$PATH" && npx quasar build -m pwa 2>&1 | grep -E "Build succeeded|Build failed"
```
Expected: `SFC OK`, `Build succeeded`.

- [ ] **Step 5: Commit**

```bash
git add src/bt/pages/ScheduledProfileConfiguration.vue
git commit -m "feat(bt-dash): baseline selector + drift panel on Configurazione page"
```

---

### Task 10: Integration smoke + docs

**Files:**
- Modify: `docs/context/watchtower_completion_roadmap_2026-08-29.md` (mark baseline manager done)

- [ ] **Step 1: Full bt-core test run**

Run: `cd /home/htpc/backtrader/bt-core && set -a; source ../env/bt-live-events; set +a && .venv/bin/python -m pytest tests/test_watchtower_runtime.py tests/test_profile_baselines.py tests/test_profile_baseline_helpers.py -v`
Expected: all PASS.

- [ ] **Step 2: End-to-end baseline create against dev (real backtest — short window)**

Run:
```bash
BASE=http://127.0.0.1:9090/dyn/obs/watchtower/cron
JOB=$(curl -sS -X POST "$BASE/development/baselines" -H 'Content-Type: application/json' \
  -d '{"label":"smoke-short","window_start":"2026-06-01","window_end":"2026-08-20"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["job_id"])')
for i in $(seq 1 60); do sleep 5; S=$(curl -sS "$BASE/baselines/jobs/$JOB"); echo "$S"; echo "$S" | grep -q '"done": true' && break; done
curl -sS "$BASE/development/baselines" | python3 -m json.tool
```
Expected: job reaches `"status": "completed"`, then a baseline row `smoke-short` with `sample_size > 0`.

- [ ] **Step 3: Drift + delete**

Run:
```bash
BID=$(curl -sS "$BASE/development/baselines" | python3 -c 'import sys,json;print([b["id"] for b in json.load(sys.stdin) if b["label"]=="smoke-short"][0])')
curl -sS "$BASE/development/baselines/$BID/drift?recent_window_days=10" | python3 -m json.tool
curl -sS -X DELETE "$BASE/development/baselines/$BID"
```
Expected: drift returns a verdict object with `status` in `ok|warning|missing_baseline`; delete returns `{"deleted": true}`.

- [ ] **Step 4: Rebuild prod bundle**

Run: `cd /home/htpc/backtrader && bash scripts/bt-dash-build-prod.sh 2>&1 | tail -5`
Expected: `Build succeeded` / `Done. Output: .../dist/pwa`.

- [ ] **Step 5: Update the roadmap doc**

In `docs/context/watchtower_completion_roadmap_2026-08-29.md`, under "Approvato ma non ancora implementato", move/annotate the baseline item to done with the spec+plan paths, and note the new endpoints (`/dyn/obs/watchtower/cron/<profile>/baselines*`), the `profile_baselines` table, and `bin/watchtower_build_profile_baseline.py`.

- [ ] **Step 6: Commit**

```bash
git add docs/context/watchtower_completion_roadmap_2026-08-29.md
git commit -m "docs(watchtower): baseline manager implemented — endpoints, table, CLI"
```

---

## Self-Review

**1. Spec coverage:**
- "calcolare/salvare/elencare/selezionare/cancellare più baseline" → Tasks 2 (CRUD), 6 (endpoints), 8 (manager page: create/list/delete), 9 (select).
- "configuro data inizio, data fine e versione (data passata)" → Task 8 form (`window_start`/`window_end`/`as_of_date`); Task 4 `resolve_baseline_context` (params-as-of, D1a).
- "pagina/popup per calcolare le baseline" → Task 8 page (`/ScheduledProfiles/Baselines`). Popup vs page: plan ships a page; a `q-dialog` variant is a later cosmetic change, noted in the spec.
- "nella maschera attuale selezionare una baseline (o cancellarla)" → Task 9 selector + drift; delete lives on the manager page (Task 8) — link provided from Configurazione.
- "probabilità che gli ultimi N giorni siano della stessa statistica" → Task 5 `compute_baseline_drift` (`evaluate_outcomes` + `monte_carlo_subset_test`), Task 6 drift endpoint, Task 9 panel. Default 10, range 3–30 (D3).
- Cardinal constraint (any profile, empty not error) → Task 2 `test_unknown_profile_returns_empty`, Task 6 Step 4 `never-seen` curl.
- Rename level-2 indicator to avoid confusion → Task 9 Step 1.
- "Non incluso": no git worktree (D1a — Task 4 comment), no schedule (no cron added), no `stat_baselines`/`Watchtower.vue` touch (nothing in the plan does).

**2. Placeholder scan:** No "TBD"/"handle edge cases"/"similar to Task N". Every code step has a full code block. Frontend build/verify steps have exact commands. The one soft spot — `_recent_window_bounds` approximates trading days as calendar days — is called out explicitly in a code comment with rationale, not left vague.

**3. Type consistency:**
- `insert_profile_baseline(...)` kwargs in Task 2 == kwargs passed in Task 5 `compute_profile_baseline` == captured in Task 5 test.
- `compute_profile_baseline(repo, profile, label, window_start, window_end, as_of_date=None, profile_env=None)` — same signature in Task 5 impl, Task 6 `_runner`, Task 7 CLI.
- `compute_baseline_drift(repo, baseline, recent_window_days=10, profile_env=None)` — same in Task 5 impl, Task 6 drift route.
- Verdict dict keys (`status`, `confidence`, `z_mean`, `ks_distance`, `recent_sample_size`, `recent_window_start/end`, `baseline_sample_size`, `recent_window_days`) — produced in Task 5, consumed in Task 9 template.
- `resolve_baseline_context` return keys (`strategy`, `stratargs`, `stratargs_str`, `ticker`, `provider`, `alpaca_feed`, `margin_leverage`, `params_hash`) — produced Task 4, consumed Task 5 and Task 7 `--print-cmd`.
- `_clamp_window` / `RECENT_WINDOW_DEFAULT` referenced in Task 6 are defined in Task 3's module header.
- Endpoint paths identical between Task 6 (definition), Task 8 and Task 9 (callers): `/dyn/obs/watchtower/cron/<profile>/baselines`, `/baselines/jobs/<job_id>`, `/<profile>/baselines/<id>`, `/<profile>/baselines/<id>/drift`.

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-08-29-baseline-manager.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
