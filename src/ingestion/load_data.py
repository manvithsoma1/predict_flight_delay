"""
load_data.py — Loads raw CSV files into local PostgreSQL (flightdb_local)

WHY THIS FILE:
  flights.csv is 565MB / 5.8M rows. Loading it all at once into a
  DataFrame needs ~3GB RAM and crashes most laptops. We use chunked
  loading (50k rows at a time) so memory stays under 500MB.

  This is the ONLY file that reads from data/raw/ — everything
  downstream reads from PostgreSQL. This is standard production practice.

RUN ONCE:
  python src/ingestion/load_data.py
"""

import os
import sys
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv

# ── Setup paths ────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from src.utils.logger import get_logger
from src.utils.helpers import get_local_engine, table_exists, get_row_count, timer

logger = get_logger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
RAW_DIR = PROJECT_ROOT / "data" / "raw"
CHUNK_SIZE = 50_000  # Rows per chunk — safe for 8GB RAM laptops

# Declare dtypes upfront so pandas doesn't guess wrong on 5M rows
FLIGHTS_DTYPES = {
    "YEAR": "Int16",
    "MONTH": "Int8",
    "DAY": "Int8",
    "DAY_OF_WEEK": "Int8",
    "AIRLINE": "str",
    "FLIGHT_NUMBER": "Int32",
    "TAIL_NUMBER": "str",
    "ORIGIN_AIRPORT": "str",
    "DESTINATION_AIRPORT": "str",
    "SCHEDULED_DEPARTURE": "str",
    "DEPARTURE_TIME": "str",
    "DEPARTURE_DELAY": "float32",
    "TAXI_OUT": "float32",
    "WHEELS_OFF": "str",
    "SCHEDULED_TIME": "float32",
    "ELAPSED_TIME": "float32",
    "AIR_TIME": "float32",
    "DISTANCE": "float32",
    "WHEELS_ON": "str",
    "TAXI_IN": "float32",
    "SCHEDULED_ARRIVAL": "str",
    "ARRIVAL_TIME": "str",
    "ARRIVAL_DELAY": "float32",
    "DIVERTED": "Int8",
    "CANCELLED": "Int8",
    "CANCELLATION_REASON": "str",
    "AIR_SYSTEM_DELAY": "float32",
    "SECURITY_DELAY": "float32",
    "AIRLINE_DELAY": "float32",
    "LATE_AIRCRAFT_DELAY": "float32",
    "WEATHER_DELAY": "float32",
}


def load_small_csv(engine, filepath: Path, table_name: str) -> None:
    """
    Load a small CSV (airlines.csv, airports.csv) in one shot.
    These are under 1000 rows so no chunking needed.
    """
    if table_exists(engine, table_name):
        count = get_row_count(engine, table_name)
        logger.info(f"'{table_name}' already exists (~{count} rows) — skipping")
        return

    logger.info(f"Loading {filepath.name} → '{table_name}'...")
    df = pd.read_csv(filepath, encoding="utf-8", encoding_errors="replace")

    # Normalize column names to lowercase with underscores
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    df.to_sql(table_name, engine, if_exists="replace", index=False)
    logger.info(f"✓ '{table_name}': {len(df)} rows loaded")


@timer
def load_flights_chunked(engine, filepath: Path, table_name: str) -> None:
    """
    Load flights.csv in 50k row chunks.

    WHY CHUNKED:
      5.8M rows all at once = 3GB RAM = laptop crash.
      50k rows at a time = ~300MB RAM = safe on any laptop.
    """
    if table_exists(engine, table_name):
        count = get_row_count(engine, table_name)
        logger.info(
            f"'{table_name}' already exists (~{count:,} rows) — skipping\n"
            f"  To reload: run DROP TABLE {table_name}; in psql first"
        )
        return

    logger.info(f"Loading {filepath.name} → '{table_name}' in {CHUNK_SIZE:,}-row chunks")
    logger.info("This takes 3-8 minutes depending on your laptop...")

    total_rows = 0
    chunk_num = 0
    start = time.time()

    reader = pd.read_csv(
        filepath,
        dtype=FLIGHTS_DTYPES,
        chunksize=CHUNK_SIZE,
        encoding="utf-8",
        encoding_errors="replace",
        low_memory=False,
    )

    for chunk in reader:
        chunk_num += 1

        # Normalize column names
        chunk.columns = [
            c.strip().lower().replace(" ", "_") for c in chunk.columns
        ]

        # Add ML target column right at load time
        # is_delayed = 1 if departure_delay >= 15 minutes (FAA standard)
        chunk["is_delayed"] = (
            chunk["departure_delay"].fillna(0) >= 15
        ).astype("int8")

        # First chunk creates the table, rest append
        write_mode = "replace" if chunk_num == 1 else "append"

        chunk.to_sql(
            table_name,
            engine,
            if_exists=write_mode,
            index=False,
            method="multi",
            chunksize=1000,
        )

        total_rows += len(chunk)
        elapsed = time.time() - start
        rate = total_rows / elapsed
        logger.info(
            f"  Chunk {chunk_num:3d} | "
            f"{total_rows:>7,} rows | "
            f"{rate:,.0f} rows/sec"
        )

    logger.info(f"✓ '{table_name}': {total_rows:,} total rows loaded")


def create_indexes(engine, table_name: str) -> None:
    """
    Create indexes AFTER loading — much faster than during load.

    WHY INDEXES:
      Without indexes, every query scans all 5.8M rows.
      With indexes, queries on airline/airport/date run in milliseconds.
    """
    logger.info(f"Creating indexes on '{table_name}'...")
    indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_airline   ON {table_name}(airline)",
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_origin    ON {table_name}(origin_airport)",
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_dest      ON {table_name}(destination_airport)",
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date      ON {table_name}(year, month, day)",
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_delayed   ON {table_name}(is_delayed)",
    ]
    with engine.connect() as conn:
        for sql in indexes:
            conn.execute(text(sql))
            conn.commit()
    logger.info(f"✓ Indexes created on '{table_name}'")


def verify_load(engine) -> None:
    """Run sanity checks to confirm data loaded correctly."""
    logger.info("── Verification ──────────────────────────────────────")
    checks = {
        "flights": "SELECT COUNT(*), AVG(departure_delay), SUM(is_delayed) FROM flights",
        "airlines": "SELECT COUNT(*) FROM airlines",
        "airports": "SELECT COUNT(*) FROM airports",
    }
    with engine.connect() as conn:
        for table, query in checks.items():
            try:
                result = conn.execute(text(query)).fetchone()
                logger.info(f"  {table}: {result}")
            except Exception as e:
                logger.error(f"  {table}: FAILED — {e}")
    logger.info("──────────────────────────────────────────────────────")


def main():
    logger.info("Starting data ingestion into flightdb_local...")
    engine = get_local_engine()

    # Load small files first
    load_small_csv(engine, RAW_DIR / "airlines.csv", "airlines")
    load_small_csv(engine, RAW_DIR / "airports.csv",  "airports")

    # Load the big file
    load_flights_chunked(engine, RAW_DIR / "flights.csv", "flights")

    # Create indexes after loading
    create_indexes(engine, "flights")

    # Verify everything looks right
    verify_load(engine)

    logger.info("🎉 All data loaded into flightdb_local successfully!")
    logger.info("Next step: python src/ingestion/weather_api.py")


if __name__ == "__main__":
    main()