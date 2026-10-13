import os
import pandas as pd
from sqlalchemy import text

from database import dw_engine


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "data", "flight.csv")


def _hhmm_to_minutes(series):
    numeric = pd.to_numeric(series, errors="coerce")

    hours = numeric // 100
    minutes = numeric % 100

    valid = (
        numeric.notna()
        & (hours >= 0)
        & (hours <= 23)
        & (minutes >= 0)
        & (minutes <= 59)
    )

    return (hours * 60 + minutes).where(valid)


def run_etl():

    print("\n" + "=" * 70)
    print(" CSV → DATA WAREHOUSE ETL")
    print("=" * 70)

    # =========================================================
    # 1. EXTRACT
    # =========================================================

    print("\n1. Extracting data from CSV...")

    df = pd.read_csv(CSV_PATH)

    print(f" -> Loaded {len(df)} CSV records.")

    if df.empty:
        print("\n[!] CSV file is empty.")
        return

    # =========================================================
    # 2. LOAD DIMENSIONS
    # =========================================================

    with dw_engine.begin() as conn:

        # -----------------------------------------------------
        # dim_date
        # -----------------------------------------------------

        dates = (
            pd.to_datetime(df["flight_date"], errors="coerce")
            .dropna()
            .dt.date
            .drop_duplicates()
        )

        for date_value in dates:

            date_ts = pd.to_datetime(date_value)

            date_key = int(
                date_ts.strftime("%Y%m%d")
            )

            conn.execute(
                text("""
                    INSERT IGNORE INTO dim_date
                    (
                        date_key,
                        full_date,
                        day,
                        month,
                        month_name,
                        quarter,
                        year,
                        day_name
                    )
                    VALUES
                    (
                        :date_key,
                        :full_date,
                        :day,
                        :month,
                        :month_name,
                        :quarter,
                        :year,
                        :day_name
                    )
                """),
                {
                    "date_key": date_key,
                    "full_date": date_value,
                    "day": date_ts.day,
                    "month": date_ts.month,
                    "month_name": date_ts.strftime("%B"),
                    "quarter": date_ts.quarter,
                    "year": date_ts.year,
                    "day_name": date_ts.strftime("%A"),
                }
            )

        print(" -> dim_date updated.")

        # -----------------------------------------------------
        # dim_airline
        # -----------------------------------------------------

        dim_airline = (
            df[
                [
                    "airline_code",
                    "airline_name",
                    "airline_country"
                ]
            ]
            .drop_duplicates()
            .rename(
                columns={
                    "airline_country": "country"
                }
            )
        )

        for _, row in dim_airline.iterrows():

            exists = conn.execute(
                text("""
                    SELECT airline_key
                    FROM dim_airline
                    WHERE airline_code = :code
                    LIMIT 1
                """),
                {
                    "code": row["airline_code"]
                }
            ).fetchone()

            if not exists:

                conn.execute(
                    text("""
                        INSERT INTO dim_airline
                        (
                            airline_code,
                            airline_name,
                            country
                        )
                        VALUES
                        (
                            :airline_code,
                            :airline_name,
                            :country
                        )
                    """),
                    row.to_dict()
                )

        print(" -> dim_airline updated.")

        # -----------------------------------------------------
        # dim_airport
        # -----------------------------------------------------

        airport_codes = set(
            df["origin_airport"]
            .dropna()
            .unique()
        )

        airport_codes.update(
            df["destination_airport"]
            .dropna()
            .unique()
        )

        for code in airport_codes:

            exists = conn.execute(
                text("""
                    SELECT airport_key
                    FROM dim_airport
                    WHERE airport_code = :code
                    LIMIT 1
                """),
                {
                    "code": code
                }
            ).fetchone()

            if not exists:

                conn.execute(
                    text("""
                        INSERT INTO dim_airport
                        (
                            airport_code,
                            airport_name,
                            city,
                            country
                        )
                        VALUES
                        (
                            :code,
                            :name,
                            :city,
                            :country
                        )
                    """),
                    {
                        "code": code,
                        "name": f"{code} Airport",
                        "city": code,
                        "country": "Unknown"
                    }
                )

        print(" -> dim_airport updated.")

        # -----------------------------------------------------
        # Airport mapping
        # -----------------------------------------------------

        db_airports = pd.read_sql(
            text("""
                SELECT
                    airport_key,
                    airport_code
                FROM dim_airport
            """),
            conn
        )

        airport_dict = dict(
            zip(
                db_airports["airport_code"],
                db_airports["airport_key"]
            )
        )

        # -----------------------------------------------------
        # dim_route
        # -----------------------------------------------------

        routes = (
            df[
                [
                    "origin_airport",
                    "destination_airport"
                ]
            ]
            .drop_duplicates()
        )

        for _, row in routes.iterrows():

            origin_key = airport_dict.get(
                row["origin_airport"]
            )

            destination_key = airport_dict.get(
                row["destination_airport"]
            )

            if not origin_key or not destination_key:
                continue

            exists = conn.execute(
                text("""
                    SELECT route_key
                    FROM dim_route
                    WHERE origin_airport_key = :origin
                      AND destination_airport_key = :destination
                    LIMIT 1
                """),
                {
                    "origin": origin_key,
                    "destination": destination_key
                }
            ).fetchone()

            if not exists:

                conn.execute(
                    text("""
                        INSERT INTO dim_route
                        (
                            origin_airport_key,
                            destination_airport_key
                        )
                        VALUES
                        (
                            :origin,
                            :destination
                        )
                    """),
                    {
                        "origin": origin_key,
                        "destination": destination_key
                    }
                )

        print(" -> dim_route updated.")

        # -----------------------------------------------------
        # dim_weather
        # -----------------------------------------------------

        weather_columns = [
            "temperature",
            "rainfall",
            "wind_speed",
            "visibility",
            "weather_condition"
        ]

        dim_weather = (
            df[weather_columns]
            .drop_duplicates()
        )

        for _, row in dim_weather.iterrows():

            if row.isna().all():
                continue

            exists = conn.execute(
                text("""
                    SELECT weather_key
                    FROM dim_weather
                    WHERE temperature <=> :temperature
                      AND rainfall <=> :rainfall
                      AND wind_speed <=> :wind_speed
                      AND visibility <=> :visibility
                      AND weather_condition <=> :weather_condition
                    LIMIT 1
                """),
                row.to_dict()
            ).fetchone()

            if not exists:

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

        print(" -> dim_weather updated.")

        # =====================================================
        # 3. CREATE DIMENSION MAPPINGS
        # =====================================================

        db_dates = pd.read_sql(
            text("""
                SELECT
                    date_key,
                    full_date
                FROM dim_date
            """),
            conn
        )

        db_airlines = pd.read_sql(
            text("""
                SELECT
                    airline_key,
                    airline_code
                FROM dim_airline
            """),
            conn
        )

        db_routes = pd.read_sql(
            text("""
                SELECT
                    route_key,
                    origin_airport_key,
                    destination_airport_key
                FROM dim_route
            """),
            conn
        )

        db_weather = pd.read_sql(
            text("""
                SELECT
                    weather_key,
                    temperature,
                    rainfall,
                    wind_speed,
                    visibility,
                    weather_condition
                FROM dim_weather
            """),
            conn
        )

        # -----------------------------------------------------
        # Route mapping
        # -----------------------------------------------------

        df["origin_airport_key"] = (
            df["origin_airport"]
            .map(airport_dict)
        )

        df["destination_airport_key"] = (
            df["destination_airport"]
            .map(airport_dict)
        )

        df = df.merge(
            db_routes,
            on=[
                "origin_airport_key",
                "destination_airport_key"
            ],
            how="left"
        )

        # -----------------------------------------------------
        # Date mapping
        # -----------------------------------------------------

        df["temp_date"] = (
            pd.to_datetime(
                df["flight_date"],
                errors="coerce"
            )
            .dt.date
        )

        df = df.merge(
            db_dates,
            left_on="temp_date",
            right_on="full_date",
            how="left"
        )

        # -----------------------------------------------------
        # Airline mapping
        # -----------------------------------------------------

        df = df.merge(
            db_airlines,
            on="airline_code",
            how="left"
        )

        # -----------------------------------------------------
        # Weather mapping
        # -----------------------------------------------------

        df = df.merge(
            db_weather,
            on=[
                "temperature",
                "rainfall",
                "wind_speed",
                "visibility",
                "weather_condition"
            ],
            how="left"
        )

        # =====================================================
        # 4. PREPARE FACT RECORDS
        # =====================================================

        print("\n4. Preparing fact records...")

        # -----------------------------------------------------
        # Flight number normalization
        # -----------------------------------------------------

        df["flight_number"] = (
            df["flight_number"]
            .astype(str)
            .str.strip()
        )

        # -----------------------------------------------------
        # Datetime conversion
        # -----------------------------------------------------

        base_date = (
            pd.to_datetime(df["flight_date"], errors="coerce")
            .dt.normalize()
        )

        departure_minutes = _hhmm_to_minutes(df["departure_time"])
        arrival_minutes = _hhmm_to_minutes(df["arrival_time"])

        overnight = (
            (arrival_minutes < departure_minutes)
            & arrival_minutes.notna()
            & departure_minutes.notna()
        )

        df["clean_departure_time"] = (
            base_date
            + pd.to_timedelta(departure_minutes, unit="m")
        )

        df["clean_arrival_time"] = (
            base_date
            + pd.to_timedelta(
                arrival_minutes + overnight.astype("float64") * 1440,
                unit="m"
            )
        )

        df["clean_scheduled_departure"] = (
            base_date
            + pd.to_timedelta(
                _hhmm_to_minutes(df["scheduled_departure"]),
                unit="m"
            )
        )

        # -----------------------------------------------------
        # Fact table dataframe
        # -----------------------------------------------------

        fact_records = pd.DataFrame({
            "source_system": "csv",
            "source_flight_id": df["flight_id"].astype(str),
            "date_key": df["date_key"],
            "airline_key": df["airline_key"],
            "route_key": df["route_key"],
            "weather_key": df["weather_key"],
            "flight_number": df["flight_number"],
            "scheduled_departure": df["clean_scheduled_departure"],
            "departure_time": df["clean_departure_time"],
            "arrival_time": df["clean_arrival_time"],
            "delay_minutes": pd.to_numeric(
                df["departure_delay_minutes"],
                errors="coerce"
            ).fillna(0).astype(int),
            "arrival_delay_minutes": pd.to_numeric(
                df["arrival_delay_minutes"],
                errors="coerce"
            ).fillna(0).astype(int),
            "flight_duration_minutes": pd.to_numeric(
                df["flight_duration_minutes"],
                errors="coerce"
            ).fillna(0).astype(int),
            "distance_km": pd.to_numeric(
                df["distance_km"],
                errors="coerce"
            ),
            "passenger_count": None,
            "cancelled_flag": (
                df["cancelled_flag"]
                .fillna(False)
                .astype(bool)
            ),
            "diverted_flag": (
                df["diverted_flag"]
                .fillna(False)
                .astype(bool)
            )
        })

        # =====================================================
        # 5. EXISTING RECORD PROTECTION
        #    (idempotent by source identity, never by flight_number)
        # =====================================================

        print("\n5. Checking existing source identities...")

        existing_sources = pd.read_sql(
            text("""
                SELECT source_system, source_flight_id
                FROM fact_flight
                WHERE source_flight_id IS NOT NULL
            """),
            conn
        )

        existing_keys = set(
            zip(
                existing_sources["source_system"].astype(str),
                existing_sources["source_flight_id"].astype(str)
            )
        )

        before_count = len(fact_records)

        # The source flight_id is the row identity; a repeated flight_number
        # is a legitimate distinct flight and must be preserved.
        fact_records = (
            fact_records
            .drop_duplicates(
                subset=["source_system", "source_flight_id"]
            )
            .copy()
        )

        fact_records = fact_records[
            ~fact_records.apply(
                lambda row: (
                    str(row["source_system"]),
                    str(row["source_flight_id"])
                ) in existing_keys,
                axis=1
            )
        ].copy()

        skipped_existing = (
            before_count - len(fact_records)
        )

        print(
            f" -> Existing CSV source records skipped: "
            f"{skipped_existing}"
        )

        # =====================================================
        # 6. LOAD FACT TABLE
        # =====================================================

        print("\n6. Loading new records into fact_flight...")

        if fact_records.empty:

            print(
                " -> No new CSV flights to insert."
            )

        else:

            fact_records.to_sql(
                "fact_flight",
                conn,
                if_exists="append",
                index=False
            )

            print(
                f" -> {len(fact_records)} NEW records "
                f"loaded into fact_flight."
            )

    # =========================================================
    # COMPLETE
    # =========================================================

    print("\n" + "=" * 70)
    print(" CSV ETL COMPLETED SUCCESSFULLY")
    print("=" * 70)


if __name__ == "__main__":
    run_etl()