"""
helpers.py — Shared utility functions used across the entire project.

These are small reusable functions that don't belong to any specific
module but are needed everywhere — database connections, timing,
data validation, etc.
"""

import os
import time
from pathlib import Path
from functools import wraps

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Load .env file
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def get_local_engine() -> Engine:
    """
    Returns SQLAlchemy engine for local PostgreSQL (flightdb_local).
    Used for heavy processing — raw data, feature engineering.
    """
    url = os.getenv("LOCAL_DATABASE_URL")
    if not url:
        raise ValueError("LOCAL_DATABASE_URL not found in .env")
    return create_engine(url, pool_pre_ping=True)


def get_neon_engine() -> Engine:
    """
    Returns SQLAlchemy engine for Neon DB (cloud).
    Used only for final clean tables the dashboard reads.
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        raise ValueError("DATABASE_URL not found in .env")
    return create_engine(url, pool_pre_ping=True)


def test_connection(engine: Engine, db_name: str) -> bool:
    """
    Tests if a database connection works.
    Returns True if connected, False if failed.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"✓ Connected to {db_name}")
        return True
    except Exception as e:
        logger.error(f"✗ Cannot connect to {db_name}: {e}")
        return False


def table_exists(engine: Engine, table_name: str) -> bool:
    """
    Check if a table already exists in the database.
    Used to avoid re-loading data that's already there.
    """
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = :table
            )
        """), {"table": table_name})
        return result.scalar()


def get_row_count(engine: Engine, table_name: str) -> int:
    """
    Fast row count using PostgreSQL statistics.
    Much faster than COUNT(*) on large tables.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT reltuples::BIGINT
                FROM pg_class
                WHERE relname = :table
            """), {"table": table_name})
            count = result.scalar()
            return count if count else 0
    except Exception:
        return 0


def timer(func):
    """
    Decorator that logs how long a function takes to run.

    Usage:
        @timer
        def my_function():
            ...

    Output:
        my_function completed in 3.45s
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logger.info(f"{func.__name__} completed in {elapsed:.2f}s")
        return result
    return wrapper