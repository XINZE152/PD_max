import logging
import logging.handlers
import os
from pathlib import Path


def _parse_log_level(value: str) -> int:
    level_name = (value or "INFO").upper().strip()
    return getattr(logging, level_name, logging.INFO)


def setup_logging() -> None:
    """Initialize project-wide logging once."""
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    level = _parse_log_level(os.getenv("LOG_LEVEL", "INFO"))
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)
    root_logger.setLevel(level)

    log_file = os.getenv("LOG_FILE", "").strip()
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
