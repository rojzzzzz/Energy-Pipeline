"""Makes the requests to the API and returns the data as a DataFrame"""

from config import STATE_PATH, RAW_FILES_PATH, EIA_CONFIG_PATH, get_env
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from tempfile import NamedTemporaryFile
from pathlib import Path
import logging
import asyncio
import httpx
import yaml
import time
import json
import os

logger = logging.getLogger(__name__)

def get_config() -> dict:
    try:
        with open(EIA_CONFIG_PATH, 'r') as file:
            raw_config = yaml.safe_load(file)
            logger.info(f"Successfully loaded config for EIA API from {EIA_CONFIG_PATH}")
            return raw_config['eia_config']

    except FileNotFoundError:
        logger.error(f"EIA API config file not found at {EIA_CONFIG_PATH}")
        raise

    except Exception as e:
        logger.error(f"Error while trying to load EIA API config from {EIA_CONFIG_PATH}. Error: {e.__class__.__name__}: {e}")
        raise

def get_state() -> dict | None:
    """Reads the state file and returns its content as a dict. Returns None if the file doesn't exist"""

    state_path = STATE_PATH
    if not state_path.exists():
        logger.info("No previous state has been found")
        return None

    try:
        with state_path.open("r", encoding="utf-8") as f:
            state = json.load(f)
            logger.info("Successfully read the state file.")
            return state
    except json.JSONDecodeError:
        logger.error("State file is corrupted or not in valid JSON format.")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while reading the state file: {e}")
        raise

def create_state(state: dict):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=4)

def write_in_raw(dataset_name: str, rows: list[dict], window_start: str, window_end: str):
    """Writes the rows to a raw file"""

    raw_path = RAW_FILES_PATH / f"{dataset_name}.jsonl"
    metadata = {
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
        "_window_start": window_start,
        "_window_end": window_end,
    }

    temporary = None
    try:
        with NamedTemporaryFile(mode='w', encoding='utf-8', dir=RAW_FILES_PATH, suffix='.jsonl', delete=False) as file:
            temporary = Path(file.name)
            if raw_path.exists():
                with raw_path.open(encoding="utf-8") as previous:
                    for line in previous:
                        file.write(line if line.endswith('\n') else line + '\n')

            for row in rows:
                file.write(json.dumps({**row, **metadata}) + '\n')

            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, raw_path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)

def extraction_end(end: datetime | None = None) -> datetime:
    end = end if end is not None else datetime.now(timezone.utc)
    if end.tzinfo is not None:
        end = end.astimezone(timezone.utc).replace(tzinfo=None)
    return end

def monthly_periods(start: datetime, end: datetime):
    """"You give it a start and end date, and returns each month between"""

    current = start
    while current <= end:
        next_month = current + relativedelta(months=1)
        window_end = min(next_month - timedelta(hours=1), end)
        yield current, window_end
        current = next_month

def yearly_periods(start: datetime, end: datetime):
    """"You give it a start and end date, and returns each year between"""

    current = start
    while current <= end:
        next_year = current + relativedelta(years=1)
        window_end = min(next_year - timedelta(hours=1), end)
        yield current, window_end
        current = next_year

def format_period(dt: datetime, frequency: str) -> str:
    """Receives the datetime and returns custom format depending on the API's frequency"""

    if frequency == "hourly":
        return dt.strftime("%Y-%m-%dT%H")
    elif frequency == "monthly":
        return dt.strftime("%Y-%m")

async def request_page(client: httpx.AsyncClient, url: str, params: dict) -> dict:
    """Makes the requests of the API for the given endpoint. Works with the httpx Client"""

    request_params = {**params, "api_key": get_env("EIA_KEY")}

    try:
        response = await client.get(url, params=request_params)
        response.raise_for_status()
        return response.json()
    
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            logger.error("API resource not found (HTTP 404)")
        logger.error("API request failed with HTTP %s", e.response.status_code)
        raise RuntimeError(f"API request failed with HTTP {e.response.status_code}") from None

    except Exception as e:
        logger.error("API request failed: %s", type(e).__name__)
        raise RuntimeError(f"API request failed: {type(e).__name__}") from None

async def extract_window(client: httpx.AsyncClient, start: str, end: str, page_size: int,
                        url: str, params: dict, frequency: str):
    """Extracts a window of data from the API. It handles pagination and returns a DataFrame"""

    if start > end:
        raise ValueError(f"Start date {start} is after end date {end}")
    if not isinstance(page_size, int) or isinstance(page_size, bool) or not 1 <= page_size <= 5000:
        raise ValueError('Page size must be an integer between 1 and 5000')

    offset = 0
    all_rows = []
    expected_total = None
    previous_page = None

    while True:
        params = {
            **params,
            "start": start,
            "end": end,
            "length": page_size,
            "offset": offset
        }

        response = await request_page(client, url, params)
        payload = response.get("response") if isinstance(response, dict) else None

        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            raise RuntimeError("Malformed EIA response: expected a data list")

        rows = payload['data']

        total = int(payload['total'])

        if expected_total is None:
            expected_total = total
        elif total != expected_total:
            raise RuntimeError('Inconsistent total in EIA response')

        if len(rows) > page_size or offset + len(rows) > total:
            raise RuntimeError('EIA page exceeds the requested size or reported total')

        pattern = '%Y-%m-%dT%H' if frequency == 'hourly' else '%Y-%m'
        for row in rows:
            period = row.get('period')
            try:
                parsed = datetime.strptime(period, pattern)
            except (ValueError, TypeError):
                raise RuntimeError('EIA observation has an invalid period')

            if format_period(parsed, frequency) != period or not start <= period <= end:
                raise RuntimeError('EIA observation has an invalid period')

        if rows == previous_page:
            raise RuntimeError('EIA repeated a page; pagination cannot be trusted')

        previous_page = rows

        if not rows and offset < total:
            raise RuntimeError(f"Unexpected empty page. Received {offset}/{total} rows")

        if not rows:
            break

        all_rows.extend(rows)
        offset += len(rows)

        if offset >= total:
            break

    return all_rows

async def run_backfill(eia_config: dict):
    """Runs the backfill. It's triggered when no previous state is found. """


    base_url = eia_config['base_url']
    timeout = eia_config['timeout_seconds']
    max_retries = eia_config['max_retries']
    datasets = eia_config['datasets']
    run_end = extraction_end()
    state = {}

    for dataset_name, dataset in datasets.items():
        logger.info(f"STARTING BACKFILL. Dataset: {dataset['name']}")
        endpoint = dataset['endpoint']
        ingestion_config = dataset['ingestion']
        frequency = dataset['params']['frequency']

        if frequency == "hourly":
            start = datetime.strptime(ingestion_config['initial_start'], "%Y-%m-%dT%H")
            end = run_end
        elif frequency == "monthly":
            start = datetime.strptime(ingestion_config['initial_start'], "%Y-%m")
            end = run_end
        else:
            logger.error(f"Received value for frequency '{frequency}' is not allowed. Check the source contract")
            raise ValueError(frequency)

        if ingestion_config['window'] == "month":
            windows = monthly_periods(start, end)
        elif ingestion_config['window'] == "year":
            windows = yearly_periods(start, end)

        transport = httpx.AsyncHTTPTransport(retries=max_retries)
        last_period = format_period(start, frequency)
        async with httpx.AsyncClient(timeout=timeout, transport=transport) as client:
            for window_start, window_end in windows:
                api_start = format_period(window_start, frequency)
                api_end = format_period(window_end, frequency)

                logger.info(f"Starting extraction for {dataset['name']} from {api_start} to {api_end}")
                rows = await extract_window(client=client, start=api_start, end=api_end, page_size=ingestion_config['page_size'],
                                            url=f"{base_url}/{endpoint}", params=dataset['params'], frequency=frequency)
                write_in_raw(dataset_name, rows, api_start, api_end)
                if rows:
                    last_period = max(last_period, max(row['period'] for row in rows))

        state[dataset_name] = {'watermark': last_period, 'last_run': datetime.now(timezone.utc).isoformat()}

    create_state(state)

async def run_incremental(eia_config: dict):
    state = get_state()
    max_retries = eia_config['max_retries']
    timeout = eia_config['timeout_seconds']

    for dataset_name, dataset in eia_config['datasets'].items():
        dataset_state = state[dataset_name]
        endpoint = dataset['endpoint']

        watermark = dataset_state['watermark']
        frequency = dataset['params']['frequency']
        ingestion_config = dataset['ingestion']
        params = dataset['params']

        if frequency == "hourly":
            watermark_dt = datetime.strptime(watermark, "%Y-%m-%dT%H")
            start_dt = watermark_dt - timedelta(hours=ingestion_config['overlap'])
            start_dt = max(start_dt, datetime.strptime(ingestion_config['initial_start'], '%Y-%m-%dT%H'))

        elif frequency == "monthly":
            watermark_dt = datetime.strptime(watermark, "%Y-%m")
            start_dt = watermark_dt - relativedelta(months=ingestion_config['overlap'])
            start_dt = max(start_dt, datetime.strptime(ingestion_config['initial_start'], '%Y-%m'))

        else:
            logger.error(f"Received value for frequency '{frequency}' is not allowed. Check the source contract")
            raise ValueError(frequency)

        run_end = extraction_end()
        api_start = format_period(start_dt, frequency)
        api_end = format_period(run_end, frequency)

        if api_start > api_end:
            logger.error(f"The watermark declared in the data contract for {dataset['name']} is after the current time")
            raise ValueError(f"Watermark {api_start} is after current time {api_end}")

        logger.info(f"STARTING INCREMENTAL. From {api_start} to {api_end} for dataset {dataset['name']}")

        transport = httpx.AsyncHTTPTransport(retries=max_retries)
        windows = monthly_periods(start_dt, run_end) if ingestion_config['window'] == "month" else yearly_periods(start_dt, run_end)
        last_period = watermark

        async with httpx.AsyncClient(timeout=timeout, transport=transport) as client:
            for window_start, window_end in windows:
                window_start_str = format_period(window_start, frequency)
                window_end_str = format_period(window_end, frequency)

                rows = await extract_window(client=client, start=window_start_str, end=window_end_str, page_size=ingestion_config['page_size'],
                                            url=f"{eia_config['base_url']}/{endpoint}", params=params, frequency=frequency)
                write_in_raw(dataset_name, rows, window_start_str, window_end_str)
                if rows:
                    last_period = max(last_period, max(row['period'] for row in rows))

        dataset_state['watermark'] = last_period
        dataset_state['last_run'] = datetime.now(timezone.utc).isoformat()

    create_state(state)


async def api_extraction():
    """Main orchestrator for the API extraction process. It handles both backfill and incremental extraction"""

    start = time.perf_counter()
    logger.info(f"Starting API extraction at {datetime.now(timezone.utc).isoformat()}")

    
    config = get_config()
    state = get_state()
    if state is None:
        await run_backfill(config)
    else:
        await run_incremental(config)

    duration = round(time.perf_counter() - start, 2)
    logger.info(f"API extraction completed successfully. Duration: {duration} seconds")
