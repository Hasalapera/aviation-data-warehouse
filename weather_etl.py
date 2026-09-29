from urllib.parse import quote_plus
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# --- DATABASE CONFIGURATION ---
DB_USER = "root"
RAW_PASSWORD = "Hasala@32120"
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "aviation_dw"

encoded_password = quote_plus(RAW_PASSWORD)
connection_string = f"mysql+pymysql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# Airport Coordinates Mapping (Airports 8 සඳහා Lat/Lon)
AIRPORT_COORDS = {
    'CMB': {'lat': 7.1808, 'lon': 79.8841},   # Colombo Bandaranaike
    'DXB': {'lat': 25.2532, 'lon': 55.3657},  # Dubai
    'SIN': {'lat': 1.3644, 'lon': 103.9915},  # Singapore Changi
    'LHR': {'lat': 51.4700, 'lon': -0.4543},  # London Heathrow
    'KUL': {'lat': 2.7456, 'lon': 101.7072},  # Kuala Lumpur
    'DEL': {'lat': 28.5562, 'lon': 77.1000},  # Delhi
    'DOH': {'lat': 25.2731, 'lon': 51.6081},  # Doha Hamad
    'BKK': {'lat': 13.6900, 'lon': 100.7501}  # Bangkok Suvarnabhumi
}

# WMO Weather Codes to Conditions
WMO_CODE_MAP = {
    0: 'Clear',
    1: 'Clear',
    2: 'Cloudy',
    3: 'Cloudy',
    45: 'Foggy',
    48: 'Foggy',
    51: 'Rain',
    61: 'Rain',
    63: 'Rain',
    80: 'Rain',
    95: 'Thunderstorm'
}

def fetch_weather(lat, lon):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,relative_humidity_2m,precipitation,weather_code,wind_speed_10m&timezone=auto"
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        data = response.json()['current']
        weather_code = data.get('weather_code', 0)
        return {
            'temperature': round(data.get('temperature_2m', 0.0), 2),
            'rainfall': round(data.get('precipitation', 0.0), 2),
            'wind_speed': round(data.get('wind_speed_10m', 0.0), 2),
            'visibility': 10.0,  # Standard default visibility km
            'weather_condition': WMO_CODE_MAP.get(weather_code, 'Clear')
        }
    return None

def run_weather_etl():
    print("1. Fetching Live Weather Data from Open-Meteo API...")
    weather_records = []

    for code, coords in AIRPORT_COORDS.items():
        w_data = fetch_weather(coords['lat'], coords['lon'])
        if w_data:
            print(f" -> Airport {code}: Temp={w_data['temperature']}°C, Condition={w_data['weather_condition']}, Wind={w_data['wind_speed']} km/h")
            weather_records.append(w_data)

    if not weather_records:
        print("Failed to fetch weather data.")
        return

    print("2. Inserting Weather Records into dim_weather...")
    weather_df = pd.DataFrame(weather_records).drop_duplicates()

    with engine.begin() as conn:
        for _, row in weather_df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO dim_weather (temperature, rainfall, wind_speed, visibility, weather_condition)
                    VALUES (:temperature, :rainfall, :wind_speed, :visibility, :weather_condition)
                """),
                row.to_dict()
            )
        
        count = conn.execute(text("SELECT COUNT(*) FROM dim_weather")).scalar()
        print(f"Weather ETL Complete! Total rows in dim_weather: {count}")

if __name__ == "__main__":
    run_weather_etl()