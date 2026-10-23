"""Quarterly extraction, silver loading, and gold model builds."""

from airflow.sdk import dag, task
import pendulum


@dag(
    dag_id="Complete_Pipeline",
    schedule="0 0 1 1,4,7,10 *",
    start_date=pendulum.datetime(2026, 10, 22, tz="America/Managua"),
    catchup=False,
    max_active_runs=1,
)
def complete_pipeline():
    @task(do_xcom_push=False)
    def extract_api():
        import asyncio
        from extract.extract_api import api_extraction

        asyncio.run(api_extraction())

    @task(do_xcom_push=False)
    def prepare_and_load():
        import yaml
        from config import DATA_CONTRACT_PATH
        from prepare.preparation import prepare_dataframes
        from load.load import load_source

        with DATA_CONTRACT_PATH.open(encoding="utf-8") as file:
            sources = yaml.safe_load(file)["contract"]["sources"]

        dataframes = prepare_dataframes(sources)
        for datasets in sources.values():
            for source in datasets:
                load_source(dataframes[source["name"]], source)

    @task.bash(do_xcom_push=False, skip_on_exit_code=None)
    def build_gold():
        return '"$DBT_EXECUTABLE" build --select +path:models/gold'

    extract_api() >> prepare_and_load() >> build_gold()


complete_pipeline()
