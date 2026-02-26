"""
opensky_api.py — Fetches live US flight states from OpenSky Network.

WHY OPENSKY:
  This makes the project truly "real-time". We get actual aircraft
  positions every hour. It's completely free — no API key needed
  (authenticated users get better rate limits though).

  This answers: "Which flights are in the air RIGHT NOW?"
  Combined with our model, we score each one for delay probability.

RUN: python src/ingestion/opensky_api.py
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from src.utils.logger import get_logger
from src.utils.helpers import get_local_engine, timer

logger = get_logger(__name__)

# Bounding box for continental USA
# (lat_min, lat_max, lon_min, lon_max)
US_BBOX = (24.396308, 49.384358, -125.000000, -66.934570)

OPENSKY_URL = "https://opensky-network.org/api/states/all"

# Column names for OpenSky API response
# See: https://openskynetwork.github.io/opensky-api/rest.html
OPENSKY_COLUMNS = [
    "icao24", "callsign", "origin_country",
    "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude",
    "on_ground", "velocity", "true_track",
    "vertical_rate", "sensors", "geo_altitude",
    "squawk", "spi", "position_source",
]


def fetch_us_flights() -> pd.DataFrame:
    """
    Fetch all aircraft currently in US airspace.

    Filters to:
      - Airborne only (on_ground == False)
      - US origin country
      - Known callsigns (so we can match to airline)
    """
    username = os.getenv("OPENSKY_USERNAME", "")
    password = os.getenv("OPENSKY_PASSWORD", "")
    auth = None

    lat_min, lat_max, lon_min, lon_max = US_BBOX

    logger.info("Calling OpenSky API...")

    try:
        resp = requests.get(
            OPENSKY_URL,
            params={
                "lamin": lat_min,
                "lomin": lon_min,
                "lamax": lat_max,
                "lomax": lon_max,
            },
            auth=auth,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

    except requests.exceptions.ConnectionError:
        logger.error("Cannot reach OpenSky API — check internet connection")
        return pd.DataFrame()
    except requests.exceptions.Timeout:
        logger.error("OpenSky API timed out — try again in a few minutes")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"OpenSky API failed: {e}")
        return pd.DataFrame()

    states = data.get("states", [])
    if not states:
        logger.warning("OpenSky returned 0 states — may be rate limited")
        return pd.DataFrame()

    df = pd.DataFrame(states, columns=OPENSKY_COLUMNS)

    # Filter to airborne US flights with known callsigns
    df = df[
        (df["on_ground"] == False) &
        (df["origin_country"] == "United States") &
        (df["callsign"].notna()) &
        (df["callsign"].str.strip() != "")
    ].copy()

    if df.empty:
        logger.warning("No US airborne flights found after filtering")
        return pd.DataFrame()

    # Clean callsign
    df["callsign"] = df["callsign"].str.strip().str.upper()

    # Extract airline ICAO code (first 3 chars of callsign)
    # e.g. "AAL123" → "AAL" (American Airlines)
    df["airline_icao"] = df["callsign"].str[:3]

    # Add fetch timestamp
    df["fetched_at"] = datetime.now(timezone.utc)

    # Keep only useful columns
    df = df[[
        "icao24", "callsign", "airline_icao",
        "longitude", "latitude",
        "baro_altitude", "geo_altitude",
        "velocity", "true_track", "vertical_rate",
        "on_ground", "fetched_at",
    ]]

    logger.info(f"✓ {len(df)} US airborne flights fetched")
    return df


def save_flights(engine, df: pd.DataFrame) -> None:
    """
    Save live flights to local PostgreSQL.
    Keeps only last 24 hours to prevent unbounded growth.
    """
    if df.empty:
        logger.warning("No flight data to save")
        return

    df.to_sql("live_flights", engine, if_exists="append", index=False)

    # Delete records older than 24 hours
    with engine.connect() as conn:
        conn.execute(text("""
            DELETE FROM live_flights
            WHERE fetched_at < NOW() - INTERVAL '24 hours'
        """))
        conn.commit()

    logger.info(f"✓ {len(df)} live flights saved to live_flights table")


@timer
def main():
    logger.info("Starting OpenSky live flight fetch...")
    engine = get_local_engine()

    df = fetch_us_flights()

    if df.empty:
        logger.warning("No data fetched — exiting")
        return

    save_flights(engine, df)

    # Show sample
    logger.info("Sample live flights:")
    sample_cols = ["callsign", "airline_icao", "latitude", "longitude", "velocity"]
    logger.info(f"\n{df[sample_cols].head(10).to_string()}")

    logger.info("✓ Live flight ingestion complete!")
    logger.info("Next step: python src/ingestion/pipeline.py")


if __name__ == "__main__":
    main()