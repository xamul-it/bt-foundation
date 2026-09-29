import datetime as dt
import importlib.util
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("calendar_refresh", ROOT / "bin" / "refresh_alpaca_calendar_cache.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def entry(day, open_time="09:30:00", close_time="16:00:00"):
    return SimpleNamespace(date=dt.date.fromisoformat(day), open=dt.time.fromisoformat(open_time), close=dt.time.fromisoformat(close_time))


def test_incremental_merge_preserves_existing_months_and_adds_future_sessions():
    cache = {"version": 1, "months": {"2026-09": {"days": {"2026-09-28": {"open": "09:30:00", "close": "16:00:00"}}}}}
    added, changed = MODULE.merge_calendar(cache, [entry("2026-10-01")], "2026-09-29T00:00:00Z")
    assert (added, changed) == (1, 0)
    assert "2026-09-28" in cache["months"]["2026-09"]["days"]
    assert "2026-10-01" in cache["months"]["2026-10"]["days"]


def test_last_cached_session_finds_latest_date_across_months():
    cache = {"version": 1, "months": {"2026-09": {"days": {"2026-09-28": {}}}, "2026-10": {"days": {"2026-10-01": {}}}}}
    assert MODULE.last_cached_session(cache) == dt.date(2026, 10, 1)
