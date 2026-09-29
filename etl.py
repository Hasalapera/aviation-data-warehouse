from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine, text

# --- DATABASE CONFIGURATION ---
DB_USER = "root"
RAW_PASSWORD = "Hasala@32120"
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "aviation_dw"

# Password URL Encoding
encoded_password = quote_plus(RAW_PASSWORD)
connection_string = f"mysql+pymysql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

def run_etl():
    print("1. Extracting data from CSV...")
    df = pd.read_csv("data/flight.csv")
    print(f"Loaded {len(df)} rows.")

    with engine.begin() as conn:
        print("2. Loading Dimensions...")

        # ----------------- A. dim_date -----------------
        dates = pd.to_datetime(df['flight_date']).drop_duplicates()
        dim_date = pd.DataFrame({
            'date_key': dates.dt.strftime('%Y%m%d').astype(int),
            'full_date': dates.dt.date,
            'day': dates.dt.day,
            'month': dates.dt.month,
            'month_name': dates.dt.strftime('%B'),
            'quarter': dates.dt.quarter,
            'year': dates.dt.year,
            'day_name': dates.dt.day_name()
        })
        for _, row in dim_date.iterrows():
            conn.execute(
                text("""
                    INSERT IGNORE INTO dim_date (date_key, full_date, day, month, month_name, quarter, year, day_name)
                    VALUES (:date_key, :full_date, :day, :month, :month_name, :quarter, :year, :day_name)
                """),
                row.to_dict()
            )
        print(" -> dim_date loaded.")

        # ----------------- B. dim_airline -----------------
        dim_airline = df[['airline_code', 'airline_name', 'airline_country']].drop_duplicates()
        dim_airline = dim_airline.rename(columns={'airline_country': 'country'})
        for _, row in dim_airline.iterrows():
            res = conn.execute(
                text("SELECT airline_key FROM dim_airline WHERE airline_code = :airline_code"),
                {'airline_code': row['airline_code']}
            ).fetchone()
            if not res:
                conn.execute(
                    text("""
                        INSERT INTO dim_airline (airline_code, airline_name, country)
                        VALUES (:airline_code, :airline_name, :country)
                    """),
                    row.to_dict()
                )
        print(" -> dim_airline loaded.")

        # ----------------- C. dim_airport -----------------
        airports = set(df['origin_airport'].unique()).union(set(df['destination_airport'].unique()))
        for code in airports:
            res = conn.execute(
                text("SELECT airport_key FROM dim_airport WHERE airport_code = :code"),
                {'code': code}
            ).fetchone()
            if not res:
                conn.execute(
                    text("""
                        INSERT INTO dim_airport (airport_code, airport_name, city, country)
                        VALUES (:code, :name, :city, :country)
                    """),
                    {'code': code, 'name': f"{code} Airport", 'city': code, 'country': 'Unknown'}
                )
        print(" -> dim_airport loaded.")

        # Airport keys lookup for Route mapping
        db_airports = pd.read_sql("SELECT airport_key, airport_code FROM dim_airport", conn)
        airport_dict = dict(zip(db_airports['airport_code'], db_airports['airport_key']))

        # ----------------- D. dim_route -----------------
        routes = df[['origin_airport', 'destination_airport']].drop_duplicates()
        for _, row in routes.iterrows():
            orig_key = airport_dict.get(row['origin_airport'])
            dest_key = airport_dict.get(row['destination_airport'])
            
            res = conn.execute(
                text("""
                    SELECT route_key FROM dim_route 
                    WHERE origin_airport_key = :orig AND destination_airport_key = :dest
                """),
                {'orig': orig_key, 'dest': dest_key}
            ).fetchone()

            if not res:
                conn.execute(
                    text("""
                        INSERT INTO dim_route (origin_airport_key, destination_airport_key)
                        VALUES (:orig, :dest)
                    """),
                    {'orig': orig_key, 'dest': dest_key}
                )
        print(" -> dim_route loaded.")

        # ----------------- E. dim_weather -----------------
        dim_weather = df[['temperature', 'rainfall', 'wind_speed', 'visibility', 'weather_condition']].drop_duplicates()
        for _, row in dim_weather.iterrows():
            conn.execute(
                text("""
                    INSERT INTO dim_weather (temperature, rainfall, wind_speed, visibility, weather_condition)
                    VALUES (:temperature, :rainfall, :wind_speed, :visibility, :weather_condition)
                """),
                row.to_dict()
            )
        print(" -> dim_weather loaded.")

        print("3. Transforming & Mapping Fact Table...")
        # Database එකේ generate වූ Keys කියවා ගැනීම
        db_dates = pd.read_sql("SELECT date_key, full_date FROM dim_date", conn)
        db_airlines = pd.read_sql("SELECT airline_key, airline_code FROM dim_airline", conn)
        db_routes = pd.read_sql("SELECT route_key, origin_airport_key, destination_airport_key FROM dim_route", conn)
        db_weather = pd.read_sql("SELECT weather_key, temperature, rainfall, wind_speed, visibility, weather_condition FROM dim_weather", conn)

        # Route Mapping
        routes_merged = df[['origin_airport', 'destination_airport']].copy()
        routes_merged['origin_airport_key'] = routes_merged['origin_airport'].map(airport_dict)
        routes_merged['destination_airport_key'] = routes_merged['destination_airport'].map(airport_dict)
        
        route_map = routes_merged.merge(
            db_routes, 
            on=['origin_airport_key', 'destination_airport_key'], 
            how='left'
        )
        df['route_key'] = route_map['route_key']

        # Date Mapping
        df['temp_date'] = pd.to_datetime(df['flight_date']).dt.date
        df = df.merge(db_dates, left_on='temp_date', right_on='full_date', how='left')

        # Airline Mapping
        df = df.merge(db_airlines, on='airline_code', how='left')

        # Weather Mapping
        df = df.merge(
            db_weather, 
            on=['temperature', 'rainfall', 'wind_speed', 'visibility', 'weather_condition'], 
            how='left'
        )

        # Safe Datetime conversions (NaN අගයන් ආරක්ෂිතව empty string එකකට හරවා concat කිරීම)
        dep_combined = df['flight_date'].astype(str) + ' ' + df['departure_time'].fillna('').astype(str)
        arr_combined = df['flight_date'].astype(str) + ' ' + df['arrival_time'].fillna('').astype(str)

        df['clean_departure_time'] = pd.to_datetime(dep_combined, errors='coerce')
        df['clean_arrival_time'] = pd.to_datetime(arr_combined, errors='coerce')

        # Fact Flight Record Table සකස් කිරීම
        fact_records = pd.DataFrame({
            'date_key': df['date_key'],
            'airline_key': df['airline_key'],
            'route_key': df['route_key'],
            'weather_key': df['weather_key'],
            'flight_number': df['flight_number'],
            'departure_time': df['clean_departure_time'],
            'arrival_time': df['clean_arrival_time'],
            'delay_minutes': pd.to_numeric(df['departure_delay_minutes'], errors='coerce').fillna(0).astype(int),
            'flight_duration_minutes': pd.to_numeric(df['flight_duration_minutes'], errors='coerce').fillna(0).astype(int),
            'passenger_count': None,
            'cancelled_flag': df['cancelled_flag'].astype(bool)
        })

        print("4. Loading fact_flight table...")
        fact_records.to_sql('fact_flight', conn, if_exists='append', index=False)
        print(f"Fact table populated successfully with {len(fact_records)} records!")

if __name__ == "__main__":
    run_etl()