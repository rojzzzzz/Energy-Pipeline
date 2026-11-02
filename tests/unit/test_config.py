import logging

import pytest

import config
from config.logging_config import RedactingUTCFormatter


def test_get_env_returns_value_without_reading_dotenv(monkeypatch):
    monkeypatch.setattr(config, "load_dotenv", lambda: None)
    monkeypatch.setenv("PIPELINE_TEST_VALUE", "configured")
    assert config.get_env("PIPELINE_TEST_VALUE") == "configured"


@pytest.mark.parametrize("value", [None, ""])
def test_get_env_rejects_missing_or_empty_values(monkeypatch, value):
    monkeypatch.setattr(config, "load_dotenv", lambda: None)
    if value is None:
        monkeypatch.delenv("PIPELINE_TEST_VALUE", raising=False)
    else:
        monkeypatch.setenv("PIPELINE_TEST_VALUE", value)
    with pytest.raises(ValueError, match="PIPELINE_TEST_VALUE is not set"):
        config.get_env("PIPELINE_TEST_VALUE")


def test_formatter_redacts_environment_key_and_query_parameters(monkeypatch):
    monkeypatch.setenv("EIA_KEY", "environment-secret")
    record = logging.LogRecord("pipeline", logging.ERROR, __file__, 1,
                               "key environment-secret URL ?api_key=other-secret&offset=0", (), None)
    output = RedactingUTCFormatter("%(message)s").format(record)
    assert "environment-secret" not in output
    assert "other-secret" not in output
    assert "api_key=[REDACTED]&offset=0" in output
