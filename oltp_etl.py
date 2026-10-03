import pandas as pd
from sqlalchemy import text

from database import oltp_engine, dw_engine


def run_oltp_etl():

    print("\n" + "=" * 70)
    print(" AIVEN OLTP → DATA WAREHOUSE ETL")
    print("=" * 70)

    # =========================================================
    # 1. EXTRACT FROM OLTP
    # =========================================================

    print("\n1. Extracting data from Aiven OLTP...")

    with oltp_engine.connect() as oltp_conn:

        # -----------------------------------------------------
        # Airlines
        # -----------------------------------------------------

        airlines_df = pd.read_sql(
            text("""
                SELECT
                    airline_code,
                    airline_name,
                    country
                FROM airlines
            """),
            oltp_conn
        )

        print(
            f" -> Airlines extracted: "
            f"{len(airlines_df)}"
        )

        # -----------------------------------------------------
        # Airports
        # -----------------------------------------------------

        airports_df = pd.read_sql(
            text("""
                SELECT
                    airport_code,
                    airport_name,
                    city,
                    country
                FROM airports
            """),
            oltp_conn
        )

        print(
            f" -> Airports extracted: "
            f"{len(airports_df)}"
        )

        # -----------------------------------------------------
        # Flights
        #
        # Important:
        # Aggregate bookings, delays and cancellations
        # BEFORE joining them with flights.
        #
        # This prevents multiple rows for the same flight.
        # -----------------------------------------------------

        flight_query = """

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

                COALESCE(
                    fd.delay_minutes,

                    CASE
                        WHEN f.actual_departure IS NOT NULL
                         AND f.scheduled_departure IS NOT NULL
                        THEN TIMESTAMPDIFF(
                            MINUTE,
                            f.scheduled_departure,
                            f.actual_departure
                        )
                        ELSE 0
                    END
                ) AS delay_minutes,

                CASE
                    WHEN f.actual_departure IS NOT NULL
                     AND f.actual_arrival IS NOT NULL
                    THEN TIMESTAMPDIFF(
                        MINUTE,
                        f.actual_departure,
                        f.actual_arrival
                    )
                    ELSE 0
                END AS calculated_duration,

                COALESCE(
                    bc.passenger_count,
                    0
                ) AS passenger_count,

                fc.cancellation_id

            FROM flights f

            JOIN airlines al
                ON f.airline_id = al.airline_id

            JOIN airports dep
                ON f.departure_airport_id = dep.airport_id

            JOIN airports arr
                ON f.arrival_airport_id = arr.airport_id

            LEFT JOIN
            (
                SELECT
                    flight_id,
                    MAX(delay_minutes) AS delay_minutes
                FROM flight_delays
                GROUP BY flight_id
            ) fd
                ON f.flight_id = fd.flight_id

            LEFT JOIN
            (
                SELECT
                    flight_id,
                    MAX(cancellation_id) AS cancellation_id
                FROM flight_cancellations
                GROUP BY flight_id
            ) fc
                ON f.flight_id = fc.flight_id

            LEFT JOIN
            (
                SELECT
                    flight_id,
                    COUNT(*) AS passenger_count
                FROM bookings
                WHERE booking_status = 'CONFIRMED'
                GROUP BY flight_id
            ) bc
                ON f.flight_id = bc.flight_id
        """

        flights_df = pd.read_sql(
            text(flight_query),
            oltp_conn
        )

    print(
        f" -> Flights extracted: "
        f"{len(flights_df)}"
    )

    if flights_df.empty:

        print(
            "\n[!] No flights found in OLTP."
        )

        return

    # =========================================================
    # 2. LOAD INTO DATA WAREHOUSE
    # =========================================================

    with dw_engine.begin() as dw_conn:

        # =====================================================
        # dim_date
        # =====================================================

        dates = (
            pd.to_datetime(
                flights_df["scheduled_departure"],
                errors="coerce"
            )
            .dropna()
            .dt.date
            .drop_duplicates()
        )

        for date_value in dates:

            date_ts = pd.to_datetime(date_value)

            date_key = int(
                date_ts.strftime("%Y%m%d")
            )

            dw_conn.execute(
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

        # =====================================================
        # dim_airline
        # =====================================================

        for _, row in airlines_df.iterrows():

            exists = dw_conn.execute(
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

                dw_conn.execute(
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

        # =====================================================
        # dim_airport
        # =====================================================

        for _, row in airports_df.iterrows():

            exists = dw_conn.execute(
                text("""
                    SELECT airport_key
                    FROM dim_airport
                    WHERE airport_code = :code
                    LIMIT 1
                """),
                {
                    "code": row["airport_code"]
                }
            ).fetchone()

            if not exists:

                dw_conn.execute(
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
                            :airport_code,
                            :airport_name,
                            :city,
                            :country
                        )
                    """),
                    row.to_dict()
                )

        print(" -> dim_airport updated.")

        # =====================================================
        # Airport mapping
        # =====================================================

        dw_airports = pd.read_sql(
            text("""
                SELECT
                    airport_key,
                    airport_code
                FROM dim_airport
            """),
            dw_conn
        )

        airport_dict = dict(
            zip(
                dw_airports["airport_code"],
                dw_airports["airport_key"]
            )
        )

        # =====================================================
        # dim_route
        # =====================================================

        routes = (
            flights_df[
                [
                    "origin_code",
                    "destination_code"
                ]
            ]
            .drop_duplicates()
        )

        for _, row in routes.iterrows():

            origin_key = airport_dict.get(
                row["origin_code"]
            )

            destination_key = airport_dict.get(
                row["destination_code"]
            )

            if not origin_key or not destination_key:
                continue

            exists = dw_conn.execute(
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

                dw_conn.execute(
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

        # =====================================================
        # Get dimension mappings
        # =====================================================

        dw_dates = pd.read_sql(
            text("""
                SELECT
                    date_key,
                    full_date
                FROM dim_date
            """),
            dw_conn
        )

        dw_airlines = pd.read_sql(
            text("""
                SELECT
                    airline_key,
                    airline_code
                FROM dim_airline
            """),
            dw_conn
        )

        dw_routes = pd.read_sql(
            text("""
                SELECT
                    route_key,
                    origin_airport_key,
                    destination_airport_key
                FROM dim_route
            """),
            dw_conn
        )

        # =====================================================
        # Map airports
        # =====================================================

        flights_df["origin_key"] = (
            flights_df["origin_code"]
            .map(airport_dict)
        )

        flights_df["destination_key"] = (
            flights_df["destination_code"]
            .map(airport_dict)
        )

        # =====================================================
        # Map routes
        # =====================================================

        flights_df = flights_df.merge(
            dw_routes,
            left_on=[
                "origin_key",
                "destination_key"
            ],
            right_on=[
                "origin_airport_key",
                "destination_airport_key"
            ],
            how="left"
        )

        # =====================================================
        # Map dates
        # =====================================================

        flights_df["flight_date"] = (
            pd.to_datetime(
                flights_df["scheduled_departure"],
                errors="coerce"
            )
            .dt.date
        )

        flights_df = flights_df.merge(
            dw_dates,
            left_on="flight_date",
            right_on="full_date",
            how="left"
        )

        # =====================================================
        # Map airlines
        # =====================================================

        flights_df = flights_df.merge(
            dw_airlines,
            on="airline_code",
            how="left"
        )

        # =====================================================
        # Prepare numeric values
        # =====================================================

        flights_df["delay_minutes"] = (
            pd.to_numeric(
                flights_df["delay_minutes"],
                errors="coerce"
            )
            .fillna(0)
            .astype(int)
        )

        flights_df["flight_duration_minutes"] = (
            pd.to_numeric(
                flights_df["calculated_duration"],
                errors="coerce"
            )
            .fillna(0)
            .astype(int)
        )

        flights_df["passenger_count"] = (
            pd.to_numeric(
                flights_df["passenger_count"],
                errors="coerce"
            )
            .fillna(0)
            .astype(int)
        )

        # =====================================================
        # Cancelled flag
        # =====================================================

        flights_df["cancelled_flag"] = (
            (
                flights_df["flight_status"]
                == "CANCELLED"
            )
            |
            flights_df["cancellation_id"].notna()
        ).astype(bool)

        # =====================================================
        # Weather
        #
        # OLTP flights currently do not have a direct
        # weather relationship.
        # Therefore weather_key remains NULL.
        # =====================================================

        flights_df["weather_key"] = None

        # =====================================================
        # 3. CREATE FACT RECORDS
        # =====================================================

        print("\n4. Preparing fact records...")

        flights_df["flight_number"] = (
            flights_df["flight_number"]
            .astype(str)
            .str.strip()
        )

        fact_records = pd.DataFrame({
            "date_key": flights_df["date_key"],

            "airline_key": flights_df["airline_key"],

            "route_key": flights_df["route_key"],

            "weather_key": flights_df["weather_key"],

            "flight_number": flights_df["flight_number"],

            "departure_time": pd.to_datetime(
                flights_df["actual_departure"],
                errors="coerce",
                format="mixed"
            ),

            "arrival_time": pd.to_datetime(
                flights_df["actual_arrival"],
                errors="coerce",
                format="mixed"
            ),

            "delay_minutes": (
                flights_df["delay_minutes"]
            ),

            "flight_duration_minutes": (
                flights_df[
                    "flight_duration_minutes"
                ]
            ),

            "passenger_count": (
                flights_df["passenger_count"]
            ),

            "cancelled_flag": (
                flights_df["cancelled_flag"]
            )
        })

        # =====================================================
        # 4. DUPLICATE PROTECTION
        # =====================================================

        print(
            "\n5. Checking existing flight numbers..."
        )

        existing_flights = pd.read_sql(
            text("""
                SELECT flight_number
                FROM fact_flight
            """),
            dw_conn
        )

        existing_flights["flight_number"] = (
            existing_flights["flight_number"]
            .astype(str)
            .str.strip()
        )

        # -----------------------------------------------------
        # Remove flights already in DW
        # -----------------------------------------------------

        before_count = len(fact_records)

        fact_records = fact_records[
            ~fact_records["flight_number"].isin(
                existing_flights["flight_number"]
            )
        ].copy()

        skipped_existing = (
            before_count - len(fact_records)
        )

        print(
            f" -> Existing OLTP flights skipped: "
            f"{skipped_existing}"
        )

        # -----------------------------------------------------
        # Remove duplicate flight numbers inside OLTP
        # -----------------------------------------------------

        before_internal = len(fact_records)

        fact_records = (
            fact_records
            .drop_duplicates(
                subset=["flight_number"]
            )
            .copy()
        )

        skipped_internal = (
            before_internal - len(fact_records)
        )

        print(
            f" -> Duplicate OLTP flight numbers "
            f"skipped: {skipped_internal}"
        )

        # =====================================================
        # 5. LOAD FACT TABLE
        # =====================================================

        print(
            "\n6. Loading new records into fact_flight..."
        )

        if fact_records.empty:

            print(
                " -> No new OLTP flights to insert."
            )

        else:

            fact_records.to_sql(
                "fact_flight",
                dw_conn,
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
    print(" OLTP ETL COMPLETED SUCCESSFULLY")
    print("=" * 70)


if __name__ == "__main__":
    run_oltp_etl()