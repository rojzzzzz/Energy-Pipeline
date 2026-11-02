# Tests

Use Python 3.12, matching the pipeline container. From the repository root:

```bash
python -m pip install -r requirements.txt
python -m pytest
```

`requirements.txt` includes pytest alongside the pipeline dependencies.

```bash
python -m pytest tests/unit
python -m pytest -m integration
```

Integration tests exercise HTTP response parsing, JSONL/PSV/XLSX files, pandas
preparation, real Great Expectations validation, and loading through a mocked
Databricks connection. They also cover extraction checkpoints and interrupted
writes. Files and reports use pytest temporary directories. No API keys, warehouse,
Airflow, dbt, Docker, or downloaded source datasets are needed.

These tests do not run live cloud services or dbt SQL models.
