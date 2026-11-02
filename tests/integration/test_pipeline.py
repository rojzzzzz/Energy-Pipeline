"""Real HTTP parsing, disk IO, pandas preparation and Great Expectations checks.

Only external service boundaries (EIA transport and Databricks connection) are mocked.
"""

import asyncio
from datetime import datetime
import json
from unittest.mock import AsyncMock

import httpx
import pandas as pd
import pytest

from extract import extract_api as extraction
from load import load
from prepare import preparation
from validate import validate

pytestmark = pytest.mark.integration


def test_http_pages_to_raw_to_validation_and_merge(
    isolated_paths, gx_context, warehouse, monkeypatch, demand_source, demand_rows,
):
    requests = []

    def respond(request):
        requests.append(request)
        offset = int(request.url.params["offset"])
        return httpx.Response(200, json={"response": {"data": demand_rows[offset:offset + 1], "total": "2"}})

    monkeypatch.setattr(extraction, "get_env", lambda name: "test-api-key")

    async def collect():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            return await extraction.extract_window(
                client, "2026-01-01T00", "2026-01-01T01", 1,
                "https://eia.invalid/data", {"frequency": "hourly"}, "hourly",
            )

    rows = asyncio.run(collect())
    extraction.write_in_raw(demand_source["name"], rows, "2026-01-01T00", "2026-01-01T01")
    frame = preparation.read_raw_data("DOM")[demand_source["name"]]
    frame = preparation.config_columns(preparation.delete_overlap(frame, demand_source["silver"]["keys"]), demand_source)
    result = load.load_source(frame, demand_source)

    assert [request.url.params["offset"] for request in requests] == ["0", "1"]
    assert all(request.url.params["api_key"] == "test-api-key" for request in requests)
    assert result == {"table_name": "test_catalog.silver.hourly_pjm_dom_demand",
                      "mode": "incremental", "row_count": 2, "status": "loaded"}
    query, parameters = warehouse[1].execute.call_args.args
    assert "MERGE INTO `test_catalog`.`silver`.`hourly_pjm_dom_demand`" in query
    loaded_values = {parameters[offset]: parameters[offset + 3] for offset in range(0, len(parameters), 5)}
    assert loaded_values == {datetime(2026, 1, 1): 12000.0, datetime(2026, 1, 1, 1): 12500.0}
    report = json.loads((isolated_paths / "validation/hourly_pjm_dom_demand_validation_resume.json").read_text())
    assert report["overall_passed"] is True
    assert report["completeness"]["pct"] == 1.0


@pytest.mark.parametrize("invalid", ["negative_value", "duplicate_key"])
def test_real_validation_blocks_invalid_data_before_connecting(
    isolated_paths, gx_context, warehouse, demand_source, demand_rows, invalid,
):
    if invalid == "negative_value":
        demand_rows[0]["value"] = "-1"
    else:
        demand_rows.append(demand_rows[0].copy())
    frame = preparation.config_columns(pd.DataFrame(demand_rows), demand_source)
    with pytest.raises(ValueError, match="Validation failed.*no data uploaded"):
        load.load_source(frame, demand_source)
    warehouse[0].assert_not_called()
    report = json.loads((isolated_paths / "validation/hourly_pjm_dom_demand_validation_resume.json").read_text())
    assert report["overall_passed"] is False


def test_all_file_formats_are_prepared_using_repository_contract(isolated_paths, sources):
    raw = isolated_paths / "raw"
    forecast = pd.DataFrame({"Zone": ["VEPCO"], "Year": [2026], "Metric": ["Actual"], "Load MW": [15000]})
    forecast.to_excel(raw / "pjm_forecast_accuracy.xlsx", sheet_name="DOM", index=False)
    weather = {
        "STATION": ["USW00093738"], "DATE": ["2026-01-01T00:00:00"],
        "LATITUDE": [38.9], "LONGITUDE": [-77.4], "temperature": [None],
        "dew_point_temperature": [0.0], "relative_humidity": [50.0], "wind_speed": [2.0], "precipitation": [0.0],
    }
    for year, name in zip((2024, 2025, 2026), sources["files"][1]["files"]):
        weather["DATE"] = [f"{year}-01-01T00:00:00"]
        pd.DataFrame(weather).to_csv(raw / name, sep="|", index=False)
    price = {"period": "2026-01", "stateid": "VA", "sectorid": "RES", "price": "12.5",
             "price-units": "cents per kilowatt-hour"}
    demand = {"period": "2026-01-01T00", "subba": "DOM", "parent": "PJM", "value": "10000",
              "value-units": "megawatthours"}
    for name, row in [("monthly_va_residential_price", price), ("hourly_pjm_dom_demand", demand)]:
        # A later ingestion revises the same observation; both versions stay in bronze.
        old = {**row, "_ingested_at": "2026-01-02T00:00:00Z"}
        new = {**row, "_ingested_at": "2026-01-03T00:00:00Z"}
        new["price" if "price" in row else "value"] = "20"
        (raw / f"{name}.jsonl").write_text(json.dumps(old) + "\n" + json.dumps(new) + "\n", encoding="utf-8")

    frames = preparation.prepare_dataframes(sources)

    assert set(frames) == {source["name"] for group in sources.values() for source in group}
    assert len(frames["noaa_GHCNh"]) == 3
    assert frames["noaa_GHCNh"]["temperature"].isna().all()
    for name, column in [("monthly_va_residential_price", "price"), ("hourly_pjm_dom_demand", "value")]:
        assert frames[name][column].tolist() == [20.0]
        assert "_ingested_at" not in frames[name]
    assert str(frames["hourly_pjm_dom_demand"]["period"].dt.tz) == "UTC"
    assert frames["pjm_forecast_accuracy"]["Year"].tolist() == [2026]


def test_atomic_raw_append_preserves_history_and_adds_metadata(isolated_paths):
    path = isolated_paths / "raw/demand.jsonl"
    path.write_text('{"period": "2026-01-01T00", "value": 10}', encoding="utf-8")
    extraction.write_in_raw("demand", [{"period": "2026-01-01T01", "value": 20}], "2026-01-01T00", "2026-01-01T01")
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert [row["value"] for row in rows] == [10, 20]
    assert rows[1]["_window_start"] == "2026-01-01T00"
    assert rows[1]["_window_end"] == "2026-01-01T01"
    assert datetime.fromisoformat(rows[1]["_ingested_at"]).utcoffset().total_seconds() == 0
    assert list(path.parent.iterdir()) == [path]


def test_failed_raw_write_keeps_existing_file_and_removes_temporary_file(isolated_paths, monkeypatch):
    path = isolated_paths / "raw/demand.jsonl"
    original = '{"value": 10}\n'
    path.write_text(original, encoding="utf-8")

    def fail_replace(*args):
        raise OSError("disk failure")

    monkeypatch.setattr(extraction.os, "replace", fail_replace)
    with pytest.raises(OSError, match="disk failure"):
        extraction.write_in_raw("demand", [{"value": 20}], "2026-01", "2026-02")
    assert path.read_text() == original
    assert list(path.parent.iterdir()) == [path]


def extraction_config():
    return {"base_url": "https://eia.invalid", "timeout_seconds": 5, "max_retries": 0,
            "datasets": {"demand": {"name": "demand", "endpoint": "data", "params": {"frequency": "hourly"},
                "ingestion": {"initial_start": "2026-01-01T00", "window": "month", "page_size": 2, "overlap": 24}}}}


def test_backfill_then_incremental_persists_revisions_and_advances_watermark(isolated_paths, monkeypatch):
    monkeypatch.setattr(extraction, "extraction_end", lambda: datetime(2026, 1, 2))
    fetch = AsyncMock(side_effect=[
        [{"period": "2026-01-01T23", "value": 10}],
        [{"period": "2026-01-01T23", "value": 11}, {"period": "2026-01-02T00", "value": 12}],
    ])
    monkeypatch.setattr(extraction, "extract_window", fetch)
    config = extraction_config()
    asyncio.run(extraction.run_backfill(config))
    assert extraction.get_state()["demand"]["watermark"] == "2026-01-01T23"
    asyncio.run(extraction.run_incremental(config))
    assert extraction.get_state()["demand"]["watermark"] == "2026-01-02T00"
    assert fetch.await_args.kwargs["start"] == "2026-01-01T00"  # overlap clipped to initial_start
    rows = [json.loads(line) for line in (isolated_paths / "raw/demand.jsonl").read_text().splitlines()]
    assert [row["value"] for row in rows] == [10, 11, 12]


def test_failed_incremental_extraction_does_not_advance_checkpoint(isolated_paths, monkeypatch):
    state = {"demand": {"watermark": "2026-01-01T23", "last_run": "previous-run"}}
    extraction.create_state(state)
    monkeypatch.setattr(extraction, "extraction_end", lambda: datetime(2026, 1, 2))
    monkeypatch.setattr(extraction, "extract_window", AsyncMock(side_effect=RuntimeError("bad page")))
    with pytest.raises(RuntimeError, match="bad page"):
        asyncio.run(extraction.run_incremental(extraction_config()))
    assert extraction.get_state() == state
    assert not list((isolated_paths / "raw").iterdir())


@pytest.mark.parametrize("problem,error,message", [
    ("missing_frame", TypeError, "must be a pandas DataFrame"),
    ("empty_frame", ValueError, "dataframe is empty"),
    ("missing_column", ValueError, "Column value is missing"),
    ("wrong_type", TypeError, "Prepare hourly_pjm_dom_demand.value as float"),
])
def test_validation_rejects_unprepared_inputs(
    isolated_paths, gx_context, demand_source, demand_rows, problem, error, message,
):
    frame = preparation.config_columns(pd.DataFrame(demand_rows), demand_source)
    if problem == "missing_frame":
        frame = None
    elif problem == "empty_frame":
        frame = frame.iloc[:0]
    elif problem == "missing_column":
        frame = frame.drop(columns="value")
    else:
        frame["value"] = frame["value"].astype(str)
    with pytest.raises(error, match=message):
        validate.run_validation({demand_source["name"]: frame}, {"api": [demand_source]})


def test_duplicate_sources_are_rejected_before_context_creation(demand_source, monkeypatch):
    def unexpected_context():
        pytest.fail("Duplicate contracts must be rejected before creating a GX context")

    monkeypatch.setattr(validate.gx, "get_context", unexpected_context)
    with pytest.raises(ValueError, match="Duplicate source names"):
        validate.run_validation({}, {"api": [demand_source, demand_source]})
