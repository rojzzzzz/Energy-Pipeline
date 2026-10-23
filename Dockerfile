FROM apache/airflow:3.3.2-python3.12

# Keep Airflow fixed while adding the dependencies used by src/.
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "pandas==3.1.0" \
    "great-expectations==1.24.0" \
    "httpx==0.28.1" \
    "PyYAML==6.0.3" \
    "python-dateutil==2.9.0.post0" \
    "python-dotenv==1.2.4" \
    "openpyxl==3.1.5" \
    "databricks-sql-connector==4.4.0"

# Keep dbt's dependencies separate from Airflow's Python environment.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
    "dbt-core==1.12.5" \
    "dbt-databricks==1.12.7"
