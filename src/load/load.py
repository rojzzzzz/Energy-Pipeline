from datetime import datetime, timezone
from config import get_env, STATE_PATH
from databricks import sql
import pandas as pd
import logging
import json

logger = logging.getLogger(__name__)


def _identifier(name: str) -> str:
    return '`' + name.replace('`', '``') + '`'


def _sql_value(value, kind):
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        if kind == 'date':
            return value.date()
        return value.to_pydatetime() if isinstance(value, pd.Timestamp) else value
    return value.item() if hasattr(value, 'item') else value


def upload_dataframe(df: pd.DataFrame, table_name: str, mode: str, *,
                    keys: list[str], schema: dict[str, str], api: bool = False) -> dict:

    if mode not in ('backfill', 'incremental'):
        raise ValueError("mode must be 'backfill' or 'incremental'")
    if len(table_name.split('.')) != 3:
        raise ValueError('table_name must be catalog.schema.table')
    if not keys:
        raise ValueError('Merge keys are required')
    result = {'table_name': table_name, 'mode': mode, 'row_count': len(df),
            'status': 'empty' if df.empty else 'loaded'}
    if df.empty:
        return result

    if api:
        dataset = table_name.rsplit('.', 1)[-1]
        with open(f"{STATE_PATH}.tmp", encoding="utf-8") as file:
            watermark = json.load(file)[dataset]['watermark']
        state_parameters = [dataset, datetime.strptime(watermark, '%Y-%m' if len(watermark) == 7 else '%Y-%m-%dT%H')]

    names = list(schema)
    target = '.'.join(_identifier(part) for part in table_name.split('.'))
    columns = ', '.join(_identifier(name) for name in names)
    matches = ' AND '.join(f't.{_identifier(key)} = s.{_identifier(key)}' for key in keys)
    updates = ', '.join(f't.{_identifier(name)} = s.{_identifier(name)}' for name in names if name not in keys)
    values = ', '.join(f's.{_identifier(name)}' for name in names)
    state_table = f"{get_env('DATABRICKS_CATALOG')}.{get_env('DATABRICKS_SCHEMA')}.{get_env('DATABRICKS_STATE_TABLE')}"
    matched = f' WHEN MATCHED THEN UPDATE SET {updates}' if updates else ''
    row_placeholder = '(' + ', '.join('?' for _ in names) + ')'
    create_columns = ", ".join(f"{_identifier(col)} {'timestamp' if kind == 'datetime' else kind}" for col, kind in schema.items())
    batch_size = max(100, (len(df) + 99) // 100)

    logger.info(f'Starting load for {table_name}: {len(df)} rows')
    try:
        with sql.connect(server_hostname=get_env('DATABRICKS_HOSTNAME'),
                        http_path=get_env('DATABRICKS_HTTP'),
                        access_token=get_env('DATABRICKS_KEY')) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET TIME ZONE 'UTC'")
                cursor.execute(f"CREATE CATALOG IF NOT EXISTS {get_env('DATABRICKS_CATALOG')}")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {get_env('DATABRICKS_CATALOG')}.{get_env('DATABRICKS_SCHEMA')}")
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {state_table} (dataset STRING, watermark TIMESTAMP, last_run TIMESTAMP) USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
                cursor.execute(f"ALTER TABLE {state_table} SET TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')")
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {target} ({create_columns}) USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
                cursor.execute(f"ALTER TABLE {target} SET TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')")
                cursor.execute("BEGIN TRANSACTION")

                for offset in range(0, len(df), batch_size):
                    batch = df[names].iloc[offset:offset + batch_size]
                    parameters = [_sql_value(value, kind)
                                for row in batch.itertuples(index=False, name=None)
                                for value, kind in zip(row, schema.values())]
                    placeholders = ', '.join(row_placeholder for _ in range(len(batch)))
                    cursor.execute(f'MERGE INTO {target} AS t '
                                   f'USING (SELECT * FROM VALUES {placeholders} AS batch ({columns})) AS s '
                                    f'ON {matches}{matched} '
                                    f'WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values});', parameters)

                if api:
                    cursor.execute(f"MERGE INTO {state_table} AS t "
                                   f"USING (SELECT ? AS dataset, ? AS watermark, current_timestamp() AS last_run) AS s "
                                   f"ON t.dataset = s.dataset "
                                   f"WHEN MATCHED THEN UPDATE SET watermark = s.watermark, last_run = s.last_run "
                                   f"WHEN NOT MATCHED THEN INSERT (dataset, watermark, last_run) VALUES (s.dataset, s.watermark, s.last_run);", state_parameters)
                cursor.execute("COMMIT")

    except Exception as error:
        logger.error(f'Load failed for {table_name}: {type(error).__name__}')
        raise
    logger.info(f'Load completed for {table_name}: {len(df)} rows')
    return result



def load_source(df: pd.DataFrame, source: dict, mode: str = 'incremental', api: bool = False) -> None:
    """Validate one prepared source, then upsert it into its existing destination."""
    from validate.validate import run_validation

    group = 'files' if 'connection' in source else 'api'
    contract = run_validation({source['name']: df}, {group: [source]})
    if not contract.accomplished:
        raise ValueError(f"Validation failed for {source['name']}; no data uploaded")


    table_name = f"{get_env('DATABRICKS_CATALOG')}.{get_env('DATABRICKS_SCHEMA')}.{source['name']}"
    upload_dataframe(df, table_name, mode, keys=source['silver']['keys'],
                            schema={col['name']: col['type'] for col in source['silver']['schema']},
                            api=api)
