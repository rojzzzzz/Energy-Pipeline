"""Transforms the raw data into a format suitable for loading and evaluation."""

from config import RAW_FILES_PATH
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def superfitial_check(df: pd.DataFrame):
    """Performs a superficial check on the dataframe to ensure it is not empty and has the expected columns."""

    if df.empty:
        logger.error("The dataframe is empty.")
        raise ValueError("The dataframe is empty.")

    if df.columns.duplicated().any():
        logger.error("The dataframe has duplicated columns.")
        raise ValueError("The dataframe has duplicated columns.")

def delete_overlap(df: pd.DataFrame, cols: list[str] | None = None) -> pd.DataFrame:
    """Deletes the overlapping rows based on the ingestion timestamp"""

    if "_ingested_at" not in df.columns:
        logger.error("The dataframe does not contain the '_ingested_at' column.")
        raise ValueError("The dataframe does not contain the '_ingested_at' column.")
    if cols is not None:
        return df.sort_values("_ingested_at", ascending=False).drop_duplicates(subset=cols, keep="first")
    return df.sort_values("_ingested_at", ascending=False).drop_duplicates(keep="first")

def check_file_existence(sources_contract: dict):
    """Checks if the files specified in the sources contract exist in the RAW files directory"""

    for format, datasets in sources_contract.items():
        for source_contract in datasets:
            files = source_contract['files']
            for file in files:
                file_path = RAW_FILES_PATH / file
                if not file_path.exists():
                    logger.error(f"The file {file} specified in the sources contract does not exist in the RAW files directory.")
                    raise FileNotFoundError(f"The file {file} specified in the sources contract does not exist in the RAW files directory.")

def read_raw_data(sheet: str) -> dict[str, pd.DataFrame]:
    """Reads the raw data from the RAW files directory and returns a dictionary containing the dataframes"""

    dataframes = {}
    noaa_dfs = []

    for file in RAW_FILES_PATH.iterdir():
        try:
            if file.suffix == ".jsonl":
                df = pd.read_json(file, lines=True)
                dataframes[file.stem] = df
                logger.info(f"Read {len(df)} rows from {file.name}")

            elif file.suffix == ".psv":
                df = pd.read_csv(file, sep="|")
                noaa_dfs.append(df)
                logger.info(f"Read {len(df)} rows from {file.name}")

            elif file.suffix == ".xlsx":
                df = pd.read_excel(file, sheet_name=sheet, header=0, keep_default_na=False, na_values=[""])
                dataframes[file.stem] = df
                logger.info(f"Read {len(df)} rows from {file.name}")

            else:
                logger.error(f"The file {file.name} has an unsupported format ({file.suffix}). Supported formats are .jsonl, .psv, and .xlsx.")
                raise ValueError(file.name)
            
        except FileNotFoundError:
            logger.error(f"The file {file.name} was not found.")
            raise


    if noaa_dfs:
        dataframes['noaa_GHCNh'] = pd.concat(noaa_dfs, ignore_index=True)

    return dataframes

def config_columns(df: pd.DataFrame, source_contract: dict) -> pd.DataFrame:
    """Configures the columns and types of the dataframe based on the source contract."""

    schema = source_contract['silver']['schema']

    expected_cols = [column['name'] for column in schema]
    actual_cols = set(df.columns)
    missing = set(expected_cols) - actual_cols

    if missing:
        logger.error(f"The dataframe is missing the following columns: {missing}")
        raise ValueError(f"The dataframe is missing the following columns: {missing}")

    for column in schema:
        col, dtype = column['name'], column['type']
        if col in df.columns:
            try:
                if dtype in ("date", "datetime"):
                    df[col] = pd.to_datetime(df[col], errors='raise', utc=True, format='mixed')
                    if dtype == "date":
                        df[col] = df[col].dt.date
                else:
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                logger.error(f"Error converting column {col} to type {dtype}: {e}")
                raise

    return df[list(expected_cols)]
    
def prepare_dataframes(sources_contract: dict):
    """Orchestrates the reading and preparation of the RAW data"""

    excel_sheet = next(source['connection']['sheet'] for source in sources_contract['files']
                    if source['name'] == 'pjm_forecast_accuracy')

    check_file_existence(sources_contract)
    dataframes = read_raw_data(excel_sheet)
    for format, datasets in sources_contract.items():
        for source_contract in datasets:
            dataset = source_contract['name']
            df = dataframes.get(dataset)

            if df is None:
                raise ValueError(f"No raw data found for source: {dataset}")
            superfitial_check(df)
            if format == "api":
                df = delete_overlap(df, source_contract['silver']['keys'])
            dataframes[dataset] = config_columns(df, source_contract)

    return dataframes




        