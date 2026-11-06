"""Shared fixtures; no credentials or project data are needed."""

from copy import deepcopy
from pathlib import Path

import pytest
import yaml


@pytest.fixture
def sources():
    path = Path(__file__).resolve().parents[1] / "data/config/data_contract.yml"
    return deepcopy(yaml.safe_load(path.read_text(encoding="utf-8"))["contract"]["sources"])


@pytest.fixture
def demand_source(sources):
    return next(source for source in sources["api"] if source["name"] == "hourly_pjm_dom_demand")


@pytest.fixture
def demand_rows():
    return [
        {"period": "2026-01-01T00", "subba": "DOM", "parent": "PJM",
         "value": "12000", "value-units": "megawatthours"},
        {"period": "2026-01-01T01", "subba": "DOM", "parent": "PJM",
         "value": "12500", "value-units": "megawatthours"},
    ]


@pytest.fixture(autouse=True)
def checkpoint_warehouse(monkeypatch):
    from unittest.mock import MagicMock
    from extract import extract_api

    connect = MagicMock(name="checkpoint_connect")
    cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = []
    monkeypatch.setattr(extract_api.sql, "connect", connect)
    monkeypatch.setattr(extract_api, "get_env", {
        "DATABRICKS_HOSTNAME": "warehouse.invalid",
        "DATABRICKS_HTTP": "/sql/test",
        "DATABRICKS_KEY": "test-token",
        "DATABRICKS_CATALOG": "test_catalog",
        "DATABRICKS_SCHEMA": "silver",
        "DATABRICKS_STATE_TABLE": "eia_state",
    }.__getitem__)
    return connect, cursor


@pytest.fixture
def isolated_paths(tmp_path, monkeypatch):
    from extract import extract_api
    from prepare import preparation
    from validate import validate

    raw = tmp_path / "raw"
    reports = tmp_path / "validation"
    raw.mkdir()
    reports.mkdir()
    monkeypatch.setattr(extract_api, "RAW_FILES_PATH", raw)
    monkeypatch.setattr(extract_api, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(preparation, "RAW_FILES_PATH", raw)
    monkeypatch.setattr(validate, "VALIDATION_PATH", reports)
    return tmp_path


@pytest.fixture
def gx_context(monkeypatch):
    """Run real expectations in a fresh, non-persistent context for each test."""
    import great_expectations as gx
    from validate import validate

    context = gx.get_context(mode="ephemeral")
    context.enable_analytics(False)
    monkeypatch.setattr(validate.gx, "get_context", lambda: context)
    return context


@pytest.fixture
def warehouse(monkeypatch):
    from unittest.mock import MagicMock
    from load import load

    connect = MagicMock(name="databricks_connect")
    cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    monkeypatch.setattr(load.sql, "connect", connect)
    monkeypatch.setattr(load, "get_env", {
        "DATABRICKS_HOSTNAME": "warehouse.invalid",
        "DATABRICKS_HTTP": "/sql/test",
        "DATABRICKS_KEY": "test-token",
        "DATABRICKS_CATALOG": "test_catalog",
        "DATABRICKS_SCHEMA": "silver",
        "DATABRICKS_STATE_TABLE": "eia_state",
    }.__getitem__)
    return connect, cursor
