"""
pipeline.py — Prefect flow that runs the full hourly pipeline.

WHY PREFECT:
  Without orchestration, you'd have to manually run 3 scripts every hour.
  Prefect automates this and gives you:
    - Automatic retries if weather API fails
    - A visual UI to show interviewers
    - Scheduling (runs every hour automatically)
    - Logs for every run

HOW TO RUN:
  One time test:  python src/ingestion/pipeline.py
  Start UI:       prefect server start  (open http://127.0.0.1:4200)
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")


# ── Tasks ──────────────────────────────────────────────────────────────────────
# Each @task is one unit of work.
# retries=3 means if weather API blips, Prefect retries 3 times
# before marking it failed — your pipeline keeps running.

@task(
    name="fetch-weather",
    retries=3,
    retry_delay_seconds=30,
)
def fetch_weather_task():
    """Fetch current weather for all 50 airports."""
    import os
    from sqlalchemy import create_engine
    from src.ingestion.weather_api import fetch_all_airports, save_weather

    logger = get_run_logger()
    api_key = os.getenv("OPENWEATHER_API_KEY")
    engine = create_engine(os.getenv("LOCAL_DATABASE_URL"), pool_pre_ping=True)

    logger.info("Fetching weather data...")
    df = fetch_all_airports(api_key)
    save_weather(engine, df)
    logger.info(f"Weather done: {len(df)} airports updated")
    return len(df)


@task(
    name="fetch-live-flights",
    retries=2,
    retry_delay_seconds=60,
)
def fetch_flights_task():
    """Fetch live flights from OpenSky Network."""
    import os
    from sqlalchemy import create_engine
    from src.ingestion.opensky_api import fetch_us_flights, save_flights

    logger = get_run_logger()
    engine = create_engine(os.getenv("LOCAL_DATABASE_URL"), pool_pre_ping=True)

    logger.info("Fetching live flights...")
    df = fetch_us_flights()
    save_flights(engine, df)
    logger.info(f"Flights done: {len(df)} aircraft tracked")
    return len(df)


# ── Main Flow ──────────────────────────────────────────────────────────────────

@flow(
    name="flight-delay-pipeline",
    description="Hourly: fetch weather + live flights → score → update dashboard",
    version="1.0",
)
def flight_delay_pipeline():
    """
    Main pipeline flow. Runs every hour via Prefect scheduler.

    Order:
      1. Weather fetch
      2. Live flights fetch
      (both run, then we're done for this iteration —
       predictions added in Week 3 after model is trained)
    """
    logger = get_run_logger()
    started_at = datetime.now(timezone.utc).isoformat()
    logger.info(f"Pipeline started at {started_at}")

    # Run both tasks
    weather_count = fetch_weather_task()
    flights_count = fetch_flights_task()

    logger.info(
        f"Pipeline complete — "
        f"Weather: {weather_count} airports | "
        f"Flights: {flights_count} tracked"
    )

    return {
        "status": "success",
        "started_at": started_at,
        "weather_airports": weather_count,
        "flights_tracked": flights_count,
    }


if __name__ == "__main__":
    # Run once immediately for testing
    result = flight_delay_pipeline()
    print(f"\nResult: {result}")