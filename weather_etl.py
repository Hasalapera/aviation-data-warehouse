import requests
import pandas as pd
from sqlalchemy import text

from database import dw_engine


# ==========================================
# AIRPORT COORDINATES
# ==========================================

AIRPORT_COORDS = {
    "CMB": {"lat": 7.1808, "lon": 79.8841},
    "DXB": {"lat": 25.2532, "lon": 55.3657},
    "SIN": {"lat": 1.3644, "lon": 103.9915},
    "LHR": {"lat": 51.4700, "lon": -0.4543},
    "KUL": {"lat": 2.7456, "lon": 101.7072},
    "DEL": {"lat": 28.5562, "lon": 77.1000},
    "DOH": {"lat": 25.2731, "lon": 51.6081},
    "BKK": {"lat": 13.6900, "lon": 100.7501},
}


# ==========================================
# WMO WEATHER CODE MAPPING
# ==========================================

WMO_CODE_MAP = {
    0: "Clear",
    1: "Clear",
    2: "Cloudy",
    3: "Cloudy",
    45: "Foggy",
    48: "Foggy",
    51: "Rain",
    61: "Rain",
    63: "Rain",
    80: "Rain",
    95: "Thunderstorm",
}


# ==========================================
# FETCH WEATHER DATA
# ==========================================

def fetch_weather(lat, lon):
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}"
        f"&longitude={lon}"
        f"&current=temperature_2m,"
        f"relative_humidity_2m,"
        f"precipitation,"
        f"weather_code,"
        f"wind_speed_10m"
        f"&timezone=auto"
    )

    response = requests.get(url, timeout=10)

    if response.status_code != 200:
        return None

    data = response.json()["current"]

    weather_code = data.get("weather_code", 0)

    return {
        "temperature": round(data.get("temperature_2m", 0.0), 2),
        "rainfall": round(data.get("precipitation", 0.0), 2),
        "wind_speed": round(data.get("wind_speed_10m", 0.0), 2),
        "visibility": 10.0,
        "weather_condition": WMO_CODE_MAP.get(
            weather_code,
            "Clear"
        ),
    }


# ==========================================
# WEATHER ETL
# ==========================================

def run_weather_etl():

    print("\n" + "=" * 70)
    print(" WEATHER API → DATA WAREHOUSE ETL")
    print("=" * 70)

    print("\n1. Fetching live weather data...")

    weather_records = []

    for airport_code, coordinates in AIRPORT_COORDS.items():

        weather = fetch_weather(
            coordinates["lat"],
            coordinates["lon"]
        )

        if weather:

            print(
                f" -> {airport_code}: "
                f"{weather['temperature']}°C | "
                f"{weather['weather_condition']} | "
                f"{weather['wind_speed']} km/h"
            )

            weather_records.append(weather)

        else:
            print(f" -> {airport_code}: Failed")

    if not weather_records:
        print("\n[!] No weather data received.")
        return

    weather_df = pd.DataFrame(weather_records).drop_duplicates()

    print(
        f"\n2. Loading {len(weather_df)} weather records "
        f"into dim_weather..."
    )

    with dw_engine.begin() as conn:

        for _, row in weather_df.iterrows():

            # Prevent duplicate weather records
            existing = conn.execute(
                text("""
                    SELECT weather_key
                    FROM dim_weather
                    WHERE temperature = :temperature
                      AND rainfall = :rainfall
                      AND wind_speed = :wind_speed
                      AND visibility = :visibility
                      AND weather_condition = :weather_condition
                    LIMIT 1
                """),
                row.to_dict()
            ).fetchone()

            if not existing:

                conn.execute(
                    text("""
                        INSERT INTO dim_weather
                        (
                            temperature,
                            rainfall,
                            wind_speed,
                            visibility,
                            weather_condition
                        )
                        VALUES
                        (
                            :temperature,
                            :rainfall,
                            :wind_speed,
                            :visibility,
                            :weather_condition
                        )
                    """),
                    row.to_dict()
                )

        count = conn.execute(
            text("SELECT COUNT(*) FROM dim_weather")
        ).scalar()

    print(
        f"\nWeather ETL completed successfully!"
        f"\nTotal dim_weather records: {count}"
    )


if __name__ == "__main__":
    run_weather_etl()