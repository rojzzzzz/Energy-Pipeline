from dataclasses import replace

import pandas as pd
import pytest

from validate.classses import Contract, DimensionResult, QualityResults, SourceContract
from validate import validate


def quality(name="demand", *, expectations=True, pct=1.0):
    return QualityResults(name, DimensionResult(name, "uniqueness", pct, 0.95),
                          DimensionResult(name, "completeness", 1.0, 0.95), expectations)


def test_threshold_is_inclusive():
    result = DimensionResult("demand", "completeness", 0.95, 0.95)
    assert result.passed
    assert not replace(result, pct=0.949).passed


@pytest.mark.parametrize("results", [[], [quality(expectations=False)], [quality(pct=0.5)],
                                        [quality("other")], [quality(), quality()]])
def test_contract_requires_one_passing_result_per_source(results):
    assert not Contract([SourceContract("demand")], results).accomplished


def test_empty_or_duplicate_source_contract_cannot_pass():
    assert not Contract().accomplished
    assert not Contract([SourceContract("demand")] * 2, [quality()] * 2).accomplished


def test_complete_contract_passes_independent_of_result_order():
    contract = Contract([SourceContract("demand"), SourceContract("weather")],
                        [quality("weather"), quality("demand")])
    assert contract.accomplished


def test_repository_contract_loads_all_sources(sources):
    contract = validate.load_contract(sources)
    assert {source.name for source in contract.sources} == {
        "pjm_forecast_accuracy", "noaa_GHCNh", "monthly_va_residential_price", "hourly_pjm_dom_demand",
    }
    demand = next(source for source in contract.sources if source.name == "hourly_pjm_dom_demand")
    assert demand.keys == ["period", "subba", "parent"]
    assert demand.schema[0].nullable is True
    assert demand.dimensions["completeness"]["threshold"] == 0.95


def test_completeness_only_measures_configured_columns():
    source = SourceContract("demand", dimensions={"completeness": {
        "columns": ["value"], "threshold": 0.75,
    }})
    result = validate.measure_completeness(pd.DataFrame({"value": [10, None], "ignored": [None, None]}), source)
    assert result.pct == 0.5
    assert not result.passed


def test_completeness_defaults_to_all_columns():
    source = SourceContract("demand", dimensions={"completeness": {"threshold": 0.75}})
    result = validate.measure_completeness(pd.DataFrame({"a": [1, None], "b": [2, 3]}), source)
    assert result.pct == 0.75
    assert result.passed


def test_uniqueness_counts_all_rows_in_duplicate_key_groups():
    source = SourceContract("demand", dimensions={"uniqueness": {"threshold": 0.9}}, keys=["id", "zone"])
    frame = pd.DataFrame({"id": [1, 1, 2, 2], "zone": ["A", "A", "A", "B"]})
    result = validate.measure_uniqueness(frame, source)
    assert result.pct == 0.5
    assert not result.passed


def test_unknown_expectation_is_rejected():
    with pytest.raises(ValueError, match="not found"):
        validate.expectation_class("expect_nonexistent_check")


def test_suite_contains_contract_checks(demand_source):
    source = validate.load_contract({"api": [demand_source]}).sources[0]
    suite = validate.build_suite(source)
    types = [expectation.expectation_type for expectation in suite.expectations]
    assert "expect_table_columns_to_match_set" in types
    assert "expect_compound_columns_to_be_unique" in types
    assert types.count("expect_column_values_to_not_be_null") == 3
    assert "expect_column_values_to_be_between" in types
