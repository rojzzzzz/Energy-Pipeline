"""Central logging setup for the project."""

import logging
import logging.config
import os
import re
import time
from pathlib import Path


DEFAULT_LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
_API_KEY_PATTERN = re.compile(r"(?i)(\bapi_key\s*=\s*)[^&\s]+")


class RedactingUTCFormatter(logging.Formatter):
    """Use UTC timestamps and keep the EIA API key out of log output."""

    converter = time.gmtime

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        api_key = os.environ.get("EIA_KEY")
        if api_key:
            message = message.replace(api_key, "[REDACTED]")
        return _API_KEY_PATTERN.sub(r"\1[REDACTED]", message)


def configure_logging(log_dir: Path | str | None = None) -> Path:
    """Send INFO+ to the console, DEBUG+ to all.log, and ERROR+ to errors.log.

    File handlers rotate at 10 MB and retain five backups. Calling this function
    again replaces the previous configuration instead of adding duplicate handlers.
    """
    log_dir = Path(log_dir or os.environ.get("DATA_CENTER_LOG_DIR") or DEFAULT_LOG_DIR).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "project": {
                    "()": RedactingUTCFormatter,
                    "format": "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
                    "datefmt": "%Y-%m-%dT%H:%M:%SZ",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "INFO",
                    "formatter": "project",
                    "stream": "ext://sys.stderr",
                },
                "all_file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "level": "DEBUG",
                    "formatter": "project",
                    "filename": str(log_dir / "all.log"),
                    "maxBytes": 10 * 1024 * 1024,
                    "backupCount": 5,
                    "encoding": "utf-8",
                    "delay": True,
                },
                "errors_file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "level": "ERROR",
                    "formatter": "project",
                    "filename": str(log_dir / "errors.log"),
                    "maxBytes": 10 * 1024 * 1024,
                    "backupCount": 5,
                    "encoding": "utf-8",
                    "delay": True,
                },
            },
            "root": {
                "level": "DEBUG",
                "handlers": ["console", "all_file", "errors_file"],
            },
            "loggers": {
                "httpx": {"level": "WARNING"},
                "httpcore": {"level": "WARNING"},
            },
        }
    )
    return log_dir
