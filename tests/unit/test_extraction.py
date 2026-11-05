import asyncio
from datetime import datetime, timedelta, timezone
import json
from unittest.mock import AsyncMock

import httpx
import pytest

from extract import extract_api as extraction


@pytest.mark.parametrize("frequency,expected", [
    ("hourly", "2026-02-03T04"), ("monthly", "2026-02"),
])
def test_format_period(frequency, expected):
    assert extraction.format_period(datetime(2026, 2, 3, 4, 5), frequency) == expected


def test_extraction_end_converts_to_naive_utc():
    value = datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=-6)))
    assert extraction.extraction_end(value) == datetime(2026, 1, 1, 6)


@pytest.mark.parametrize("periods,end,expected", [
    (extraction.monthly_periods, datetime(2024, 3, 1), [
        (datetime(2024, 1, 1), datetime(2024, 1, 31, 23)),
        (datetime(2024, 2, 1), datetime(2024, 2, 29, 23)),
        (datetime(2024, 3, 1), datetime(2024, 3, 1)),
    ]),
    (extraction.yearly_periods, datetime(2025, 2, 1), [
        (datetime(2024, 1, 1), datetime(2024, 12, 31, 23)),
        (datetime(2025, 1, 1), datetime(2025, 2, 1)),
    ]),
])
def test_windows_include_leap_day_and_clip_final_window(periods, end, expected):
    assert list(periods(datetime(2024, 1, 1), end)) == expected


@pytest.mark.parametrize("periods", [extraction.monthly_periods, extraction.yearly_periods])
def test_windows_are_empty_when_start_is_after_end(periods):
    assert list(periods(datetime(2026, 2, 1), datetime(2026, 1, 1))) == []


@pytest.mark.parametrize("previous", [None, {"demand": {"watermark": "2025-12-31T23"}}])
def test_state_is_pending_until_committed(tmp_path, monkeypatch, previous):
    path = tmp_path / "state.json"
    monkeypatch.setattr(extraction, "STATE_PATH", path)
    if previous is not None:
        path.write_text(json.dumps(previous), encoding="utf-8")
    state = {"demand": {"watermark": "2026-01-01T00"}}
    extraction.create_state(state)

    assert extraction.get_state() == previous
    pending = path.with_suffix(".json.tmp")
    assert json.loads(pending.read_text(encoding="utf-8")) == state

    pending.replace(path)
    assert extraction.get_state() == state


def test_corrupt_state_is_not_silently_discarded(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    path.write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(extraction, "STATE_PATH", path)
    with pytest.raises(json.JSONDecodeError):
        extraction.get_state()


def run_window(monkeypatch, responses, **overrides):
    request = AsyncMock(side_effect=responses)
    monkeypatch.setattr(extraction, "request_page", request)
    options = dict(client=object(), start="2026-01-01T00", end="2026-01-01T02",
                   page_size=2, url="https://eia.invalid/data", params={}, frequency="hourly")
    options.update(overrides)
    return asyncio.run(extraction.extract_window(**options)), request


def page(rows, total):
    return {"response": {"data": rows, "total": str(total)}}


def test_pagination_uses_offsets_and_keeps_input_params(monkeypatch):
    rows = [{"period": f"2026-01-01T0{hour}"} for hour in range(3)]
    params = {"frequency": "hourly"}
    actual, request = run_window(monkeypatch, [page(rows[:2], 3), page(rows[2:], 3)], params=params)
    assert actual == rows
    assert [call.args[2]["offset"] for call in request.await_args_list] == [0, 2]
    assert params == {"frequency": "hourly"}


@pytest.mark.parametrize("size", [0, -1, 5001, True, 1.5, "2"])
def test_invalid_page_size_is_rejected_before_request(monkeypatch, size):
    with pytest.raises(ValueError, match="Page size"):
        run_window(monkeypatch, [], page_size=size)
    extraction.request_page.assert_not_awaited()


def test_reversed_window_is_rejected(monkeypatch):
    with pytest.raises(ValueError, match="after end"):
        run_window(monkeypatch, [], start="2026-02-01T00")


@pytest.mark.parametrize("responses,message", [
    ([{}], "Malformed"),
    ([{"response": {"data": {}}}], "Malformed"),
    ([page([], 1)], "Unexpected empty page"),
    ([page([{"period": "bad"}], 1)], "invalid period"),
    ([page([{"period": "2025-12-31T23"}], 1)], "invalid period"),
    ([page([{"period": "2026-1-1T0"}], 1)], "invalid period"),
    ([page([{"period": "2026-01-01T00"}], 0)], "exceeds"),
    ([page([{"period": "2026-01-01T00"}], 3), page([], 4)], "Inconsistent total"),
    ([page([{"period": "2026-01-01T00"}], 3)] * 2, "repeated a page"),
])
def test_untrustworthy_responses_are_rejected(monkeypatch, responses, message):
    with pytest.raises(RuntimeError, match=message):
        run_window(monkeypatch, responses)


def test_empty_dataset_is_valid(monkeypatch):
    rows, request = run_window(monkeypatch, [page([], 0)])
    assert rows == []
    assert request.await_count == 1


@pytest.mark.parametrize("status", [404, 429, 500])
def test_http_errors_hide_credentials(monkeypatch, caplog, status):
    monkeypatch.setattr(extraction, "get_env", lambda name: "secret-key")

    async def run():
        transport = httpx.MockTransport(lambda request: httpx.Response(status, request=request))
        async with httpx.AsyncClient(transport=transport) as client:
            await extraction.request_page(client, "https://eia.invalid/data", {})

    with pytest.raises(RuntimeError, match=f"HTTP {status}") as error:
        asyncio.run(run())
    assert "secret-key" not in str(error.value)
    assert "secret-key" not in caplog.text


@pytest.mark.parametrize("has_state", [False, True])
def test_orchestrator_selects_backfill_or_incremental(monkeypatch, has_state):
    config = {"datasets": {}}
    monkeypatch.setattr(extraction, "get_config", lambda: config)
    monkeypatch.setattr(extraction, "get_state", lambda: {} if has_state else None)
    backfill, incremental = AsyncMock(), AsyncMock()
    monkeypatch.setattr(extraction, "run_backfill", backfill)
    monkeypatch.setattr(extraction, "run_incremental", incremental)
    asyncio.run(extraction.api_extraction())
    selected, other = (incremental, backfill) if has_state else (backfill, incremental)
    selected.assert_awaited_once_with(config)
    other.assert_not_awaited()
