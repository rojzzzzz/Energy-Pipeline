from datetime import date, datetime
import json

import numpy as np
import pandas as pd
import pytest

from load import load


@pytest.mark.parametrize("value", [None, float("nan"), pd.NA, pd.NaT])
def test_sql_null_conversion(value):
    assert load._sql_value(value, "float") is None


def test_sql_values_use_native_python_types_and_utc():
    value = pd.Timestamp("2026-01-01T23:00:00-06:00")
    assert load._sql_value(value, "datetime") == datetime(2026, 1, 2, 5)
    assert type(load._sql_value(value, "datetime")) is datetime
    assert load._sql_value(value, "date") == date(2026, 1, 2)
    assert type(load._sql_value(np.int64(3), "int")) is int


def test_identifiers_escape_backticks():
    assert load._identifier("price-units") == "`price-units`"
    assert load._identifier("a`b") == "`a``b`"


@pytest.mark.parametrize("overrides,message", [
    ({"mode": "append"}, "mode"), ({"table_name": "silver.demand"}, "catalog.schema.table"),
    ({"keys": []}, "Merge keys"),
])
def test_upload_rejects_invalid_configuration_before_connecting(warehouse, overrides, message):
    options = dict(table_name="catalog.silver.demand", mode="incremental", keys=["id"], schema={"id": "int"})
    options.update(overrides)
    with pytest.raises(ValueError, match=message):
        load.upload_dataframe(pd.DataFrame({"id": [1]}), **options)
    warehouse[0].assert_not_called()


def test_empty_upload_does_not_connect(warehouse):
    result = load.upload_dataframe(pd.DataFrame(), "catalog.silver.demand", "backfill",
                                   keys=["id"], schema={"id": "int"})
    assert result == {"table_name": "catalog.silver.demand", "mode": "backfill", "row_count": 0, "status": "empty"}
    warehouse[0].assert_not_called()


def test_merge_batches_rows_and_parameterizes_values(warehouse):
    value = "O'Brien; DROP TABLE demand"
    frame = pd.DataFrame({"id": range(101), "text": [value] * 101})
    result = load.upload_dataframe(frame, "catalog.silver.demand", "incremental",
                                   keys=["id"], schema={"id": "int", "text": "string"})
    calls = [call.args for call in warehouse[1].execute.call_args_list if call.args[0].startswith("MERGE")]
    assert result["row_count"] == 101
    assert result["status"] == "loaded"
    assert len(calls) == 2
    assert len(calls[0][1]) == 200
    assert calls[1][1] == [100, value]
    assert value not in calls[0][0]
    assert calls[0][1][:2] == [0, value]
    assert "ON t.`id` = s.`id`" in calls[0][0]
    assert "WHEN MATCHED THEN UPDATE SET t.`text` = s.`text`" in calls[0][0]
    assert "WHEN NOT MATCHED THEN INSERT" in calls[0][0]

    queries = [call.args[0] for call in warehouse[1].execute.call_args_list]
    begin = queries.index("BEGIN TRANSACTION")
    assert all(index < begin for index, query in enumerate(queries) if query.startswith("CREATE"))
    assert all(index > begin for index, query in enumerate(queries) if query.startswith("MERGE"))
    assert queries[-1] == "COMMIT"


def test_key_only_table_omits_update_clause(warehouse):
    load.upload_dataframe(pd.DataFrame({"id": [1]}), "catalog.silver.ids", "backfill",
                          keys=["id"], schema={"id": "int"})
    query, params = next(call.args for call in warehouse[1].execute.call_args_list
                         if call.args[0].startswith("MERGE"))
    assert "WHEN MATCHED" not in query
    assert params == [1]


@pytest.mark.parametrize("row_count", [10000, 10001, 100001])
def test_large_upload_stays_within_transaction_write_limit(warehouse, row_count):
    cursor = warehouse[1]
    writes = 0

    def execute(query, *args):
        nonlocal writes
        if query.startswith("MERGE"):
            writes += 1
            if writes > 100:
                raise RuntimeError("MAX_INTERMEDIATE_COMMITS_EXCEEDED")

    cursor.execute.side_effect = execute
    frame = pd.DataFrame({"id": range(row_count)})
    load.upload_dataframe(frame, "catalog.silver.ids", "incremental",
                          keys=["id"], schema={"id": "int"})

    calls = [call.args for call in cursor.execute.call_args_list if call.args[0].startswith("MERGE")]
    assert len(calls) <= 100
    assert [value for _, parameters in calls for value in parameters] == list(range(row_count))
    assert [call.args for call in cursor.execute.call_args_list].count(("COMMIT",)) == 1
    assert cursor.execute.call_args.args == ("COMMIT",)


@pytest.mark.parametrize("dataset,period,expected", [
    ("demand", "2026-01-01T23", datetime(2026, 1, 1, 23)),
    ("price", "2026-01", datetime(2026, 1, 1)),
])
def test_api_checkpoint_uses_extracted_period_and_dataset_key(
    warehouse, tmp_path, monkeypatch, dataset, period, expected,
):
    path = tmp_path / "state.json"
    monkeypatch.setattr(load, "STATE_PATH", path, raising=False)
    path.with_suffix(".json.tmp").write_text(json.dumps({dataset: {"watermark": period}}), encoding="utf-8")

    load.upload_dataframe(pd.DataFrame({"id": [1]}), f"test_catalog.silver.{dataset}", "incremental",
                          keys=["id"], schema={"id": "int"}, api=True)

    calls = [call.args for call in warehouse[1].execute.call_args_list]
    checkpoint = next(call for call in calls if call[0].startswith("MERGE INTO test_catalog.silver.eia_state"))
    assert checkpoint[1] == [dataset, expected]
    assert "current_timestamp() AS watermark" not in checkpoint[0]
    assert calls.index(checkpoint) < calls.index(("COMMIT",))


def test_missing_pending_api_checkpoint_blocks_upload(warehouse, tmp_path, monkeypatch):
    monkeypatch.setattr(load, "STATE_PATH", tmp_path / "missing.json", raising=False)
    with pytest.raises(FileNotFoundError):
        load.upload_dataframe(pd.DataFrame({"id": [1]}), "test_catalog.silver.demand", "incremental",
                              keys=["id"], schema={"id": "int"}, api=True)
    warehouse[0].assert_not_called()


def test_checkpoint_write_failure_does_not_commit_data(warehouse, tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    monkeypatch.setattr(load, "STATE_PATH", path, raising=False)
    path.with_suffix(".json.tmp").write_text(json.dumps({"demand": {"watermark": "2026-01-01T00"}}), encoding="utf-8")
    cursor = warehouse[1]

    def execute(query, *args):
        if query.startswith("MERGE INTO test_catalog.silver.eia_state"):
            raise RuntimeError("checkpoint write failed")

    cursor.execute.side_effect = execute
    with pytest.raises(RuntimeError, match="checkpoint write failed"):
        load.upload_dataframe(pd.DataFrame({"id": [1]}), "test_catalog.silver.demand", "incremental",
                              keys=["id"], schema={"id": "int"}, api=True)
    assert "COMMIT" not in [call.args[0] for call in cursor.execute.call_args_list]


@pytest.mark.parametrize("failure", ["setup", "second_batch", "commit"])
def test_warehouse_errors_propagate_and_resources_close(warehouse, failure):
    connect, cursor = warehouse
    merges = 0

    def execute(query, *args):
        nonlocal merges
        if query.startswith("MERGE"):
            merges += 1
        if (failure == "setup" or
                failure == "second_batch" and query.startswith("MERGE") and merges == 2 or
                failure == "commit" and query == "COMMIT"):
            raise RuntimeError("warehouse unavailable")

    cursor.execute.side_effect = execute
    with pytest.raises(RuntimeError, match="warehouse unavailable"):
        load.upload_dataframe(pd.DataFrame({"id": range(101)}), "catalog.silver.ids", "incremental",
                              keys=["id"], schema={"id": "int"})
    if failure != "commit":
        assert "COMMIT" not in [call.args[0] for call in cursor.execute.call_args_list]
    connect.return_value.__exit__.assert_called_once()
    connect.return_value.__enter__.return_value.cursor.return_value.__exit__.assert_called_once()
