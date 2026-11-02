from datetime import date

import pandas as pd
import pytest

from prepare import preparation


@pytest.mark.parametrize("frame,message", [
    (pd.DataFrame(), "empty"),
    (pd.DataFrame([[1, 2]], columns=["x", "x"]), "duplicated columns"),
])
def test_superficial_check_rejects_invalid_frames(frame, message):
    with pytest.raises(ValueError, match=message):
        preparation.superfitial_check(frame)


def test_overlap_keeps_latest_revision_for_each_composite_key():
    frame = pd.DataFrame([
        {"id": 1, "region": "A", "value": 10, "_ingested_at": "2026-01-01"},
        {"id": 1, "region": "A", "value": 20, "_ingested_at": "2026-01-02"},
        {"id": 1, "region": "B", "value": 30, "_ingested_at": "2026-01-01"},
    ])
    result = preparation.delete_overlap(frame, ["id", "region"])
    assert result.set_index("region")["value"].to_dict() == {"A": 20, "B": 30}


def test_overlap_requires_ingestion_timestamp():
    with pytest.raises(ValueError, match="_ingested_at"):
        preparation.delete_overlap(pd.DataFrame({"id": [1]}), ["id"])


def test_columns_are_cast_ordered_and_extras_removed():
    source = {"silver": {"schema": [
        {"name": "day", "type": "date"}, {"name": "time", "type": "datetime"},
        {"name": "value", "type": "float"}, {"name": "year", "type": "int"},
    ]}}
    frame = pd.DataFrame({"extra": ["drop"], "year": ["2026"], "value": ["12.5"],
                          "time": ["2026-01-01T00:00:00-06:00"], "day": ["2026-01-01"]})
    result = preparation.config_columns(frame, source)
    assert list(result.columns) == ["day", "time", "value", "year"]
    assert result.loc[0, "day"] == date(2026, 1, 1)
    assert result.loc[0, "time"] == pd.Timestamp("2026-01-01T06:00:00Z")
    assert result.loc[0, "value"] == 12.5
    assert pd.api.types.is_integer_dtype(result["year"])


def test_missing_contract_column_is_rejected(demand_source, demand_rows):
    frame = pd.DataFrame(demand_rows).drop(columns="value")
    with pytest.raises(ValueError, match="missing.*value"):
        preparation.config_columns(frame, demand_source)


def test_invalid_numeric_value_is_not_coerced_to_null(demand_source, demand_rows):
    demand_rows[0]["value"] = "unknown"
    with pytest.raises(ValueError):
        preparation.config_columns(pd.DataFrame(demand_rows), demand_source)


def test_missing_raw_file_is_reported(tmp_path, monkeypatch):
    monkeypatch.setattr(preparation, "RAW_FILES_PATH", tmp_path)
    with pytest.raises(FileNotFoundError, match="missing.jsonl"):
        preparation.check_file_existence({"api": [{"files": ["missing.jsonl"]}]})


def test_unsupported_raw_file_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(preparation, "RAW_FILES_PATH", tmp_path)
    (tmp_path / "unexpected.txt").write_text("data", encoding="utf-8")
    with pytest.raises(ValueError, match="unexpected.txt"):
        preparation.read_raw_data("DOM")
