import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import execute_analytics_query

QUERY_AIRLINE_METRICS = """
SELECT 
    a.airline_name,
    COUNT(f.flight_key) AS total_flights,
    COALESCE(SUM(f.passenger_count), 0) AS recorded_passengers,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins,
    ROUND((SUM(CASE WHEN f.delay_minutes <= 15 AND f.cancelled_flag = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS on_time_rate_pct
FROM fact_flight f
JOIN dim_airline a ON f.airline_key = a.airline_key
GROUP BY a.airline_name
ORDER BY total_flights DESC;
"""

QUERY_CANCELLATION_RATE = """
SELECT
    a.airline_name,
    COUNT(f.flight_key) AS total_flights,
    SUM(CASE WHEN f.cancelled_flag = 1 THEN 1 ELSE 0 END) AS cancelled_flights,
    ROUND(
        (SUM(CASE WHEN f.cancelled_flag = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100,
        2
    ) AS cancellation_rate_pct
FROM fact_flight f
JOIN dim_airline a
    ON f.airline_key = a.airline_key
GROUP BY a.airline_name
ORDER BY cancellation_rate_pct DESC;
"""

QUERY_FLIGHTS_VS_PASSENGERS = """
SELECT
    a.airline_name,
    COUNT(f.flight_key) AS total_flights,
    COALESCE(SUM(f.passenger_count), 0) AS total_passengers
FROM fact_flight f
JOIN dim_airline a
    ON f.airline_key = a.airline_key
GROUP BY a.airline_name
ORDER BY total_flights DESC;
"""

QUERY_DELAY_TREND_QUARTERS = """
SELECT
    a.airline_name,
    d.year,
    d.quarter,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins
FROM fact_flight f
JOIN dim_airline a
    ON f.airline_key = a.airline_key
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY
    a.airline_name,
    d.year,
    d.quarter
ORDER BY
    d.year,
    d.quarter,
    a.airline_name;
"""


QUERY_DELAY_SEVERITY = """
SELECT
    a.airline_name,
    SUM(CASE
        WHEN f.delay_minutes <= 15 THEN 1
        ELSE 0
    END) AS on_time_flights,

    SUM(CASE
        WHEN f.delay_minutes > 15
             AND f.delay_minutes <= 30 THEN 1
        ELSE 0
    END) AS minor_delay_flights,

    SUM(CASE
        WHEN f.delay_minutes > 30
             AND f.delay_minutes <= 60 THEN 1
        ELSE 0
    END) AS moderate_delay_flights,

    SUM(CASE
        WHEN f.delay_minutes > 60 THEN 1
        ELSE 0
    END) AS severe_delay_flights

FROM fact_flight f
JOIN dim_airline a
    ON f.airline_key = a.airline_key
GROUP BY a.airline_name
ORDER BY a.airline_name;
"""


if __name__ == "__main__":
    execute_analytics_query(
        "Airline Performance & Passenger Volume", 
        QUERY_AIRLINE_METRICS
    )
    execute_analytics_query(
        "Airline Cancellation Rates",
        QUERY_CANCELLATION_RATE
    )
    execute_analytics_query(
        "Flights vs Passengers",
        QUERY_FLIGHTS_VS_PASSENGERS
    )
    execute_analytics_query(
        "Delay Trend by Quarters",
        QUERY_DELAY_TREND_QUARTERS
    )
    execute_analytics_query(
        "Delay Severity",
        QUERY_DELAY_SEVERITY
    )