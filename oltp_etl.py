import os
from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine, text

# Path to the CA certificate
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CA_PATH = os.path.join(BASE_DIR, "ca.pem")

# ==========================================
# 1. OLTP CONFIG (Aiven Cloud Source)
# ==========================================
OLTP_USER = "avnadmin"
OLTP_RAW_PASSWORD = "AVNS_bLAx8dFeOGTT79_t1Wb"    # Put your real Aiven password here
OLTP_HOST = "mysql-6743902-sadeedina2002-8ec4.c.aivencloud.com"                # Put your real Aiven host here
OLTP_PORT = 20825                            # Put your real numeric port here
OLTP_DB_NAME = "aviation_management_system"

encoded_oltp_pass = quote_plus(OLTP_RAW_PASSWORD)

# NOTICE: NO '?ssl_mode=REQUIRED' at the end of this URL!
oltp_conn_str = f"mysql+pymysql://{OLTP_USER}:{encoded_oltp_pass}@{OLTP_HOST}:{OLTP_PORT}/{OLTP_DB_NAME}"

# ==========================================
# 2. DATA WAREHOUSE CONFIG (Local MySQL)
# ==========================================
DW_USER = "root"
DW_RAW_PASSWORD = "Hasala@32120"
DW_HOST = "localhost"
DW_PORT = 3306
DW_DB_NAME = "aviation_dw"

encoded_dw_pass = quote_plus(DW_RAW_PASSWORD)
dw_conn_str = f"mysql+pymysql://{DW_USER}:{encoded_dw_pass}@{DW_HOST}:{DW_PORT}/{DW_DB_NAME}"

# ==========================================
# 3. CREATE ENGINES
# ==========================================
# PyMySQL handles SSL via connect_args using ca.pem
oltp_engine = create_engine(
    oltp_conn_str,
    connect_args={
        "ssl": {
            "ca": CA_PATH
        }
    }
)

dw_engine = create_engine(dw_conn_str)


def run_oltp_etl():
    print("==================================================")
    print(" Starting OLTP to Data Warehouse ETL Pipeline")
    print("==================================================")

    # ----------------------------------------------------
    # 1. EXTRACT DATA FROM OLTP DATABASE (Aiven)
    # ----------------------------------------------------
    print("\n1. Extracting data from OLTP (aviation_management_system)...")
    
    with oltp_engine.connect() as oltp_conn:
        # Extract Airlines
        airlines_df = pd.read_sql("SELECT airline_code, airline_name, country FROM airlines", oltp_conn)
        print(f" -> Extracted {len(airlines_df)} airlines.")

        # Extract Airports
        airports_df = pd.read_sql("SELECT airport_code, airport_name, city, country FROM airports", oltp_conn)
        print(f" -> Extracted {len(airports_df)} airports.")

        # Extract Flights with Aggregated Booking Counts, Delays, and Cancellations
        flight_extract_query = """
            SELECT 
                f.flight_id,
                f.flight_number,
                al.airline_code,
                dep.airport_code AS origin_code,
                arr.airport_code AS destination_code,
                f.scheduled_departure,
                f.actual_departure,
                f.actual_arrival,
                f.status AS flight_status,
                COALESCE(fd.delay_minutes, 
                    CASE 
                        WHEN f.actual_departure IS NOT NULL AND f.scheduled_departure IS NOT NULL 
                        THEN TIMESTAMPDIFF(MINUTE, f.scheduled_departure, f.actual_departure)
                        ELSE 0 
                    END
                ) AS delay_minutes,
                TIMESTAMPDIFF(MINUTE, f.actual_departure, f.actual_arrival) AS calculated_duration,
                COUNT(b.booking_id) AS passenger_count,
                fc.cancellation_id
            FROM flights f
            JOIN airlines al ON f.airline_id = al.airline_id
            JOIN airports dep ON f.departure_airport_id = dep.airport_id
            JOIN airports arr ON f.arrival_airport_id = arr.airport_id
            LEFT JOIN flight_delays fd ON f.flight_id = fd.flight_id
            LEFT JOIN flight_cancellations fc ON f.flight_id = fc.flight_id
            LEFT JOIN bookings b ON f.flight_id = b.flight_id AND b.booking_status = 'CONFIRMED'
            GROUP BY 
                f.flight_id, f.flight_number, al.airline_code, dep.airport_code, arr.airport_code,
                f.scheduled_departure, f.actual_departure, f.actual_arrival, f.status,
                fd.delay_minutes, fc.cancellation_id
        """
        flights_df = pd.read_sql(flight_extract_query, oltp_conn)
        print(f" -> Extracted {len(flights_df)} flight records.")

    if flights_df.empty:
        print("\n[!] No flight records found in OLTP database. ETL completed.")
        return

    # ----------------------------------------------------
    # 2. LOAD DIMENSIONS INTO DATA WAREHOUSE (Local DW)
    # ----------------------------------------------------
    print("\n2. Loading Dimensions into Data Warehouse...")

    with dw_engine.begin() as dw_conn:
        # A. Dim Date
        dates = pd.to_datetime(flights_df['scheduled_departure']).dt.date.drop_duplicates()
        for d in dates:
            d_ts = pd.to_datetime(d)
            date_key = int(d_ts.strftime('%Y%m%d'))
            dw_conn.execute(
                text("""
                    INSERT IGNORE INTO dim_date (date_key, full_date, day, month, month_name, quarter, year, day_name)
                    VALUES (:date_key, :full_date, :day, :month, :month_name, :quarter, :year, :day_name)
                """),
                {
                    'date_key': date_key,
                    'full_date': d,
                    'day': d_ts.day,
                    'month': d_ts.month,
                    'month_name': d_ts.strftime('%B'),
                    'quarter': d_ts.quarter,
                    'year': d_ts.year,
                    'day_name': d_ts.day_name()
                }
            )
        print(" -> dim_date updated.")

        # B. Dim Airline
        for _, row in airlines_df.iterrows():
            res = dw_conn.execute(
                text("SELECT airline_key FROM dim_airline WHERE airline_code = :code"),
                {'code': row['airline_code']}
            ).fetchone()
            if not res:
                dw_conn.execute(
                    text("INSERT INTO dim_airline (airline_code, airline_name, country) VALUES (:airline_code, :airline_name, :country)"),
                    row.to_dict()
                )
        print(" -> dim_airline updated.")

        # C. Dim Airport
        for _, row in airports_df.iterrows():
            res = dw_conn.execute(
                text("SELECT airport_key FROM dim_airport WHERE airport_code = :code"),
                {'code': row['airport_code']}
            ).fetchone()
            if not res:
                dw_conn.execute(
                    text("INSERT INTO dim_airport (airport_code, airport_name, city, country) VALUES (:airport_code, :airport_name, :city, :country)"),
                    row.to_dict()
                )
        print(" -> dim_airport updated.")

        # Airport Keys Lookup
        dw_airports = pd.read_sql("SELECT airport_key, airport_code FROM dim_airport", dw_conn)
        airport_dict = dict(zip(dw_airports['airport_code'], dw_airports['airport_key']))

        # D. Dim Route
        routes = flights_df[['origin_code', 'destination_code']].drop_duplicates()
        for _, row in routes.iterrows():
            orig_k = airport_dict.get(row['origin_code'])
            dest_k = airport_dict.get(row['destination_code'])
            if orig_k and dest_k:
                res = dw_conn.execute(
                    text("SELECT route_key FROM dim_route WHERE origin_airport_key = :orig AND destination_airport_key = :dest"),
                    {'orig': orig_k, 'dest': dest_k}
                ).fetchone()
                if not res:
                    dw_conn.execute(
                        text("INSERT INTO dim_route (origin_airport_key, destination_airport_key) VALUES (:orig, :dest)"),
                        {'orig': orig_k, 'dest': dest_k}
                    )
        print(" -> dim_route updated.")

        # ----------------------------------------------------
        # 3. TRANSFORM & MAP FACT TABLE
        # ----------------------------------------------------
        print("\n3. Transforming and Mapping Fact Records...")

        dw_dates = pd.read_sql("SELECT date_key, full_date FROM dim_date", dw_conn)
        dw_airlines = pd.read_sql("SELECT airline_key, airline_code FROM dim_airline", dw_conn)
        dw_routes = pd.read_sql("SELECT route_key, origin_airport_key, destination_airport_key FROM dim_route", dw_conn)

        # Mapping Keys
        flights_df['origin_key'] = flights_df['origin_code'].map(airport_dict)
        flights_df['dest_key'] = flights_df['destination_code'].map(airport_dict)

        flights_df = flights_df.merge(
            dw_routes,
            left_on=['origin_key', 'dest_key'],
            right_on=['origin_airport_key', 'destination_airport_key'],
            how='left'
        )

        flights_df['flight_date'] = pd.to_datetime(flights_df['scheduled_departure']).dt.date
        flights_df = flights_df.merge(dw_dates, left_on='flight_date', right_on='full_date', how='left')
        flights_df = flights_df.merge(dw_airlines, on='airline_code', how='left')

        # Flags & Durations
        is_cancelled = (flights_df['flight_status'] == 'CANCELLED') | (flights_df['cancellation_id'].notna())
        flights_df['cancelled_flag'] = is_cancelled.astype(bool)
        flights_df['delay_minutes'] = pd.to_numeric(flights_df['delay_minutes'], errors='coerce').fillna(0).astype(int)
        flights_df['flight_duration_minutes'] = pd.to_numeric(flights_df['calculated_duration'], errors='coerce').fillna(0).astype(int)
        flights_df['passenger_count'] = pd.to_numeric(flights_df['passenger_count'], errors='coerce').fillna(0).astype(int)

        # Default weather key
        flights_df['weather_key'] = None

        fact_records = pd.DataFrame({
            'date_key': flights_df['date_key'],
            'airline_key': flights_df['airline_key'],
            'route_key': flights_df['route_key'],
            'weather_key': flights_df['weather_key'],
            'flight_number': flights_df['flight_number'],
            'departure_time': pd.to_datetime(flights_df['actual_departure'], errors='coerce'),
            'arrival_time': pd.to_datetime(flights_df['actual_arrival'], errors='coerce'),
            'delay_minutes': flights_df['delay_minutes'],
            'flight_duration_minutes': flights_df['flight_duration_minutes'],
            'passenger_count': flights_df['passenger_count'],
            'cancelled_flag': flights_df['cancelled_flag']
        })

        # ----------------------------------------------------
        # 4. LOAD INTO FACT TABLE
        # ----------------------------------------------------
        print("\n4. Loading into fact_flight...")
        fact_records.to_sql('fact_flight', dw_conn, if_exists='append', index=False)
        print(f" -> Successfully loaded {len(fact_records)} records into fact_flight from OLTP!")

    print("\n==================================================")
    print(" OLTP ETL Process Finished Successfully")
    print("==================================================")


if __name__ == "__main__":
    run_oltp_etl()