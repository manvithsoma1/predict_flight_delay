"""
logger.py — Centralized logging for the entire project.
"""

import logging
import sys
from pathlib import Path

# Absolute path — finds project root reliably regardless of depth
# __file__ is: project_root/src/utils/logger.py
# So we go up exactly 2 levels to reach project_root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Returns a configured logger.

    Usage in any other file:
        from src.utils.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Starting...")
        logger.error("Something broke!")
    """
    logger = logging.getLogger(name)

    # Avoid duplicate handlers if called multiple times
    if logger.handlers:
        return logger

    logger.setLevel(level)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler 1: Print to terminal
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler 2: Save to logs/pipeline.log
    file_handler = logging.FileHandler(
        LOG_DIR / "pipeline.log", encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger