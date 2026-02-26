"""
weather_api.py — Fetches current weather for top US airports.

WHY WEATHER DATA:
  Weather is the #1 external cause of flight delays. Adding wind speed,
  visibility, and precipitation as ML features pushes AUC-ROC from
  ~0.82 to ~0.91. This is the difference between a basic model and
  a great one.

RUN: python src/ingestion/weather_api.py
"""

import os
import sys
import time
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

# Top 50 US airports by passenger volume
# IATA code → (latitude, longitude)
TOP_AIRPORTS = {
    "ATL": (33.6407, -84.4277),
    "LAX": (33.9425, -118.4081),
    "ORD": (41.9742, -87.9073),
    "DFW": (32.8998, -97.0403),
    "DEN": (39.8561, -104.6737),
    "JFK": (40.6413, -73.7781),
    "SFO": (37.6213, -122.3790),
    "SEA": (47.4502, -122.3088),
    "LAS": (36.0840, -115.1537),
    "MCO": (28.4312, -81.3081),
    "EWR": (40.6895, -74.1745),
    "PHX": (33.4373, -112.0078),
    "IAH": (29.9902, -95.3368),
    "MIA": (25.7959, -80.2870),
    "BOS": (42.3656, -71.0096),
    "MSP": (44.8848, -93.2223),
    "DTW": (42.2162, -83.3554),
    "FLL": (26.0726, -80.1527),
    "PHL": (39.8721, -75.2411),
    "LGA": (40.7769, -73.8740),
    "BWI": (39.1754, -76.6683),
    "SLC": (40.7899, -111.9791),
    "DCA": (38.8521, -77.0377),
    "SAN": (32.7338, -117.1933),
    "TPA": (27.9755, -82.5332),
    "PDX": (45.5898, -122.5951),
    "STL": (38.7487, -90.3700),
    "BNA": (36.1245, -86.6782),
    "AUS": (30.1975, -97.6664),
    "HOU": (29.6454, -95.2789),
    "MDW": (41.7868, -87.7522),
    "OAK": (37.7213, -122.2208),
    "SJC": (37.3626, -121.9290),
    "CLE": (41.4117, -81.8498),
    "RDU": (35.8776, -78.7875),
    "MCI": (39.2976, -94.7139),
    "IND": (39.7173, -86.2944),
    "PIT": (40.4915, -80.2329),
    "CMH": (39.9980, -82.8919),
    "SAT": (29.5337, -98.4698),
    "CVG": (39.0488, -84.6678),
    "MEM": (35.0421, -89.9767),
    "MSY": (29.9934, -90.2580),
    "JAX": (30.4941, -81.6879),
    "OMA": (41.3032, -95.8941),
    "OKC": (35.3931, -97.6007),
    "ABQ": (35.0402, -106.6090),
    "BUF": (42.9405, -78.7322),
    "DAL": (32.8481, -96.8518),
    "HNL": (21.3245, -157.9251),
}

WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"


def fetch_airport_weather(
    api_key: str, iata: str, lat: float, lon: float
) -> dict | None:
    """Fetch current weather for one airport. Returns None on failure."""
    try:
        resp = requests.get(
            WEATHER_URL,
            params={
                "lat": lat,
                "lon": lon,
                "appid": api_key,
                "units": "imperial",  # Fahrenheit, mph
            },
            timeout=10,
        )
        resp.raise_for_status()
        d = resp.json()

        return {
            "airport_code":   iata,
            "fetched_at":     datetime.now(timezone.utc),
            "temp_f":         d["main"].get("temp"),
            "humidity_pct":   d["main"].get("humidity"),
            "pressure_hpa":   d["main"].get("pressure"),
            "visibility_m":   d.get("visibility"),
            "wind_speed_mph": d["wind"].get("speed"),
            "wind_gust_mph":  d["wind"].get("gust"),
            "wind_deg":       d["wind"].get("deg"),
            "cloud_pct":      d["clouds"].get("all"),
            "weather_main":   d["weather"][0].get("main"),
            "weather_desc":   d["weather"][0].get("description"),
            "rain_1h_mm":     d.get("rain", {}).get("1h", 0.0),
            "snow_1h_mm":     d.get("snow", {}).get("1h", 0.0),
        }

    except Exception as e:
        logger.warning(f"  Failed to fetch {iata}: {e}")
        return None


@timer
def fetch_all_airports(api_key: str) -> pd.DataFrame:
    """
    Fetch weather for all airports.
    Sleeps 1 second between calls to respect free tier rate limit
    (60 calls/minute max on OpenWeatherMap free tier).
    """
    records = []
    total = len(TOP_AIRPORTS)

    for i, (iata, (lat, lon)) in enumerate(TOP_AIRPORTS.items(), 1):
        logger.info(f"  [{i:2d}/{total}] Fetching {iata}...")
        result = fetch_airport_weather(api_key, iata, lat, lon)
        if result:
            records.append(result)
        time.sleep(1)  # Rate limit: max 60 calls/minute

    logger.info(f"Fetched {len(records)}/{total} airports successfully")
    return pd.DataFrame(records)


def save_weather(engine, df: pd.DataFrame) -> None:
    """
    Save weather to local PostgreSQL.
    Uses append so we keep history for trend analysis.
    Table grows ~50 rows/hour = tiny.
    """
    if df.empty:
        logger.warning("No weather data to save")
        return

    df.to_sql("weather_current", engine, if_exists="append", index=False)

    # Create index on first run
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_weather_airport_time
            ON weather_current(airport_code, fetched_at)
        """))
        conn.commit()

    logger.info(f"✓ {len(df)} weather records saved to weather_current")


def main():
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        logger.error("OPENWEATHER_API_KEY missing from .env")
        sys.exit(1)

    logger.info("Starting weather fetch for top 50 US airports...")
    engine = get_local_engine()

    df = fetch_all_airports(api_key)
    save_weather(engine, df)

    # Show sample
    if not df.empty:
        logger.info("Sample data:")
        sample_cols = ["airport_code", "temp_f", "weather_main", "wind_speed_mph"]
        logger.info(f"\n{df[sample_cols].head(10).to_string()}")

    logger.info("✓ Weather ingestion complete!")
    logger.info("Next step: python src/ingestion/opensky_api.py")


if __name__ == "__main__":
    main()