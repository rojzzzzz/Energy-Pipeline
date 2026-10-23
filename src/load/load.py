from config import get_env
from databricks import sql
from datetime import datetime, timezone
import pandas as pd
import logging

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
                    keys: list[str], schema: dict[str, str]) -> dict:

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

    names = list(schema)
    target = '.'.join(_identifier(part) for part in table_name.split('.'))
    columns = ', '.join(_identifier(name) for name in names)
    matches = ' AND '.join(f't.{_identifier(key)} = s.{_identifier(key)}' for key in keys)
    updates = ', '.join(f't.{_identifier(name)} = s.{_identifier(name)}' for name in names if name not in keys)
    values = ', '.join(f's.{_identifier(name)}' for name in names)
    matched = f' WHEN MATCHED THEN UPDATE SET {updates}' if updates else ''
    row_placeholder = '(' + ', '.join('?' for _ in names) + ')'
    create_columns = ", ".join(f"{_identifier(col)} {'timestamp' if kind == 'datetime' else kind}" for col, kind in schema.items())

    logger.info(f'Starting load for {table_name}: {len(df)} rows')
    try:
        with sql.connect(server_hostname=get_env('DATABRICKS_HOSTNAME'),
                        http_path=get_env('DATABRICKS_HTTP'),
                        access_token=get_env('DATABRICKS_KEY')) as connection:
            with connection.cursor() as cursor:

                cursor.execute("SET TIME ZONE 'UTC'")
                cursor.execute(f"CREATE CATALOG IF NOT EXISTS {get_env('DATABRICKS_CATALOG')}")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {get_env('DATABRICKS_CATALOG')}.{get_env('DATABRICKS_SCHEMA')}")
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {target} ({create_columns}) USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

                for offset in range(0, len(df), 100):
                    batch = df[names].iloc[offset:offset + 100]
                    parameters = [_sql_value(value, kind)
                                for row in batch.itertuples(index=False, name=None)
                                for value, kind in zip(row, schema.values())]
                    placeholders = ', '.join(row_placeholder for _ in range(len(batch)))
                    cursor.execute(f'MERGE INTO {target} AS t '
                                   f'USING (SELECT * FROM VALUES {placeholders} AS batch ({columns})) AS s '
                                    f'ON {matches}{matched} '
                                    f'WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})', parameters)
    except Exception as error:
        logger.error(f'Load failed for {table_name}: {type(error).__name__}')
        raise
    logger.info(f'Load completed for {table_name}: {len(df)} rows')
    return result


def load_source(df: pd.DataFrame, source: dict, mode: str = 'incremental') -> dict:
    """Validate one prepared source, then upsert it into its existing destination."""
    from validate.validate import run_validation

    group = 'files' if 'connection' in source else 'api'
    contract = run_validation({source['name']: df}, {group: [source]})
    if not contract.accomplished:
        raise ValueError(f"Validation failed for {source['name']}; no data uploaded")


    table_name = f"{get_env('DATABRICKS_CATALOG')}.{get_env('DATABRICKS_SCHEMA')}.{source['name']}"
    return upload_dataframe(df, table_name, mode, keys=source['silver']['keys'],
                            schema={col['name']: col['type'] for col in source['silver']['schema']})
