"""Structured logging configuration for Stats Bluenoser.

Provides JSON-formatted logging for production use and human-readable
logging for development. Includes log rotation to prevent disk fill.
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

LOGS_DIR = Path(__file__).parent.parent / "logs"


class JSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, default=str)


def setup_logging(json_format: bool | None = None, log_file: bool = True):
    """Configure logging for the pipeline.

    Args:
        json_format: If True, use JSON format. If None, auto-detect
            (JSON when LOG_FORMAT=json or not a TTY).
        log_file: If True, also write to a rotating log file.
    """
    level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    # Clear existing handlers
    root.handlers.clear()

    # Auto-detect format
    if json_format is None:
        json_format = os.environ.get("LOG_FORMAT") == "json" or not sys.stderr.isatty()

    # Console handler
    console = logging.StreamHandler(sys.stderr)
    if json_format:
        console.setFormatter(JSONFormatter())
    else:
        console.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        ))
    root.addHandler(console)

    # Rotating file handler (10 MB per file, keep 5 backups = 50 MB max)
    if log_file:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            LOGS_DIR / "pipeline.log",
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(JSONFormatter())
        root.addHandler(file_handler)

    # Quiet down noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
