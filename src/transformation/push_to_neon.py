"""
push_to_neon.py — Pushes final mart tables from local PostgreSQL to Neon DB.

WHY THIS FILE:
  Local PostgreSQL has 5.8M rows — too big for Neon's 512MB free tier.
  But mart_delay_summary is only 678 rows (~50KB).
  We push ONLY the small aggregated tables to Neon.
  The Streamlit dashboard then reads from Neon (always available,
  no need to have your laptop running).

RUN: python src/transformation/push_to_neon.py
"""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from src.utils.logger import get_logger
from src.utils.helpers import get_local_engine, get_neon_engine, timer

logger = get_logger(__name__)

# Tables to push to Neon — ONLY small aggregated tables
# Never push flights (5.8M rows) or features (5.7M rows)
TABLES_TO_PUSH = [
    "mart_delay_summary",   # 678 rows — dashboard analytics
]


def push_table(local_engine, neon_engine, table_name: str) -> None:
    """
    Read table from local PostgreSQL, write to Neon DB.
    Uses replace so Neon always has fresh data.
    """
    logger.info(f"Reading '{table_name}' from local PostgreSQL...")
    df = pd.read_sql(f"SELECT * FROM {table_name}", local_engine)
    logger.info(f"  → {len(df):,} rows read")

    if df.empty:
        logger.warning(f"  '{table_name}' is empty — skipping")
        return

    logger.info(f"Pushing '{table_name}' to Neon DB...")
    df.to_sql(
        table_name,
        neon_engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=500,
    )

    # Verify
    with neon_engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
    logger.info(f"  ✓ '{table_name}': {count:,} rows in Neon DB")


@timer
def main():
    logger.info("Starting push to Neon DB...")
    local_engine = get_local_engine()
    neon_engine = get_neon_engine()

    for table in TABLES_TO_PUSH:
        push_table(local_engine, neon_engine, table)

    logger.info("✓ All tables pushed to Neon DB!")
    logger.info("Next step: python src/model/train.py")


if __name__ == "__main__":
    main()