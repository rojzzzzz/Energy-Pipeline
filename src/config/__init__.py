"""Configuration shared by pipeline stages"""

from dotenv import load_dotenv
from pathlib import Path
import os

def get_env(name: str) -> str:
    load_dotenv()
    value = os.environ.get(name)
    if value:
        return value
    raise ValueError(f"Environment variable {name} is not set")

DATA_PATH = Path(__file__).parents[2] / "data"
STATE_PATH = DATA_PATH / "config" / "eia_state.json"

VALIDATION_PATH = DATA_PATH / "validation"
VALIDATION_PATH.mkdir(parents=True, exist_ok=True)
DATA_CONTRACT_PATH = DATA_PATH / "config" / "data_contract.yml"
RAW_FILES_PATH = DATA_PATH / "raw"
EIA_CONFIG_PATH = DATA_PATH / "config" / "eia_ingestion.yml"