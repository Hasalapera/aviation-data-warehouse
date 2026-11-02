import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import execute_analytics_query

QUERY_AIRLINE_METRICS = """
WITH airline_metrics AS (
    SELECT
        a.airline_name,
        COUNT(f.flight_key) AS total_flights,
        COALESCE(SUM(f.passenger_count), 0) AS recorded_passengers,
        ROUND(
            AVG(
                CASE
                    WHEN f.cancelled_flag = 0 THEN f.delay_minutes
                END
            ),
            2
        ) AS avg_delay_mins,
        ROUND(
            100.0 * SUM(
                CASE
                    WHEN f.cancelled_flag = 0
                         AND f.delay_minutes <= 15 THEN 1
                    ELSE 0
                END
            ) / NULLIF(
                SUM(
                    CASE
                        WHEN f.cancelled_flag = 0 THEN 1
                        ELSE 0
                    END
                ),
                0
            ),
            2
        ) AS on_time_rate_pct,
        ROUND(
            100.0 * SUM(
                CASE
                    WHEN f.cancelled_flag = 1 THEN 1
                    ELSE 0
                END
            ) / NULLIF(COUNT(f.flight_key), 0),
            2
        ) AS cancellation_rate_pct,
        SUM(
            CASE
                WHEN f.cancelled_flag = 0 THEN 1
                ELSE 0
            END
        ) AS operated_flights
    FROM fact_flight AS f
    JOIN dim_airline AS a
        ON f.airline_key = a.airline_key
    GROUP BY a.airline_name
)
SELECT
    m.airline_name,
    m.total_flights,
    m.recorded_passengers,
    m.avg_delay_mins,
    m.on_time_rate_pct,
    m.cancellation_rate_pct,
    RANK() OVER (ORDER BY m.total_flights DESC) AS flight_rank,
    RANK() OVER (ORDER BY m.recorded_passengers DESC) AS passenger_rank,
    CASE
        WHEN m.operated_flights = 0 THEN 'No Operated Flights'
        WHEN m.avg_delay_mins IS NULL THEN 'Insufficient Delay Data'
        WHEN m.on_time_rate_pct >= 85 AND m.avg_delay_mins <= 10 THEN 'Excellent'
        WHEN m.on_time_rate_pct >= 75 AND m.avg_delay_mins <= 15 THEN 'Good'
        WHEN m.on_time_rate_pct >= 70 AND m.avg_delay_mins <= 25 THEN 'Fair'
        ELSE 'Needs Improvement'
    END AS performance_category
FROM airline_metrics AS m
ORDER BY m.total_flights DESC, m.airline_name;
"""

QUERY_CANCELLATION_RATE = """
WITH airline_cancellations AS (
    SELECT
        a.airline_name,
        COUNT(f.flight_key) AS total_flights,
        SUM(
            CASE
                WHEN f.cancelled_flag = 1 THEN 1
                ELSE 0
            END
        ) AS cancelled_flights
    FROM fact_flight AS f
    JOIN dim_airline AS a
        ON f.airline_key = a.airline_key
    GROUP BY a.airline_name
),
airline_cancellation_rates AS (
    SELECT
        ac.airline_name,
        ac.total_flights,
        ac.cancelled_flights,
        100.0 * ac.cancelled_flights / NULLIF(ac.total_flights, 0) AS cancellation_rate_raw
    FROM airline_cancellations AS ac
)
SELECT
    acr.airline_name,
    acr.total_flights,
    acr.cancelled_flights,
    ROUND(acr.cancellation_rate_raw, 2) AS cancellation_rate_pct,
    RANK() OVER (
        ORDER BY acr.cancellation_rate_raw DESC
    ) AS cancellation_rank,
    COALESCE(
        ROUND(
            100.0 * acr.cancelled_flights / NULLIF(
                SUM(acr.cancelled_flights) OVER (),
                0
            ),
            2
        ),
        0
    ) AS share_of_all_cancellations_pct,
    ROUND(
        acr.cancellation_rate_raw - (
            100.0 * SUM(acr.cancelled_flights) OVER ()
            / NULLIF(SUM(acr.total_flights) OVER (), 0)
        ),
        2
    ) AS rate_vs_network_pp
FROM airline_cancellation_rates AS acr
ORDER BY cancellation_rank, acr.airline_name;
"""

QUERY_FLIGHTS_VS_PASSENGERS = """
WITH airline_passengers AS (
    SELECT
        a.airline_name,
        COUNT(f.flight_key) AS total_flights,
        COALESCE(SUM(f.passenger_count), 0) AS total_passengers
    FROM fact_flight AS f
    JOIN dim_airline AS a
        ON f.airline_key = a.airline_key
    GROUP BY a.airline_name
)
SELECT
    ap.airline_name,
    ap.total_flights,
    ap.total_passengers,
    ROUND(
        ap.total_passengers / NULLIF(ap.total_flights, 0),
        2
    ) AS passengers_per_flight,
    ROUND(
        100.0 * ap.total_passengers / NULLIF(SUM(ap.total_passengers) OVER (), 0),
        2
    ) AS passenger_share_pct
FROM airline_passengers AS ap
ORDER BY ap.total_flights DESC;
"""

QUERY_DELAY_TREND_QUARTERS = """
WITH airline_quarter_delays AS (
    SELECT
        a.airline_name,
        d.year,
        d.quarter,
        ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins
    FROM fact_flight AS f
    JOIN dim_airline AS a
        ON f.airline_key = a.airline_key
    JOIN dim_date AS d
        ON f.date_key = d.date_key
    WHERE f.cancelled_flag = 0
    GROUP BY
        a.airline_name,
        d.year,
        d.quarter
),
airline_quarter_trend AS (
    SELECT
        aqd.airline_name,
        aqd.year,
        aqd.quarter,
        aqd.avg_delay_mins,
        ROUND(
            LAG(aqd.avg_delay_mins) OVER (
                PARTITION BY aqd.airline_name
                ORDER BY aqd.year, aqd.quarter
            ),
            2
        ) AS previous_quarter_avg_delay_mins,
        ROUND(
            aqd.avg_delay_mins - LAG(aqd.avg_delay_mins) OVER (
                PARTITION BY aqd.airline_name
                ORDER BY aqd.year, aqd.quarter
            ),
            2
        ) AS qoq_delay_change_mins
    FROM airline_quarter_delays AS aqd
)
SELECT
    aqt.airline_name,
    aqt.year,
    aqt.quarter,
    aqt.avg_delay_mins,
    aqt.previous_quarter_avg_delay_mins,
    aqt.qoq_delay_change_mins
FROM airline_quarter_trend AS aqt
ORDER BY
    aqt.year,
    aqt.quarter,
    aqt.airline_name;
"""


QUERY_DELAY_SEVERITY = """
WITH airline_delay_counts AS (
    SELECT
        a.airline_name,
        COUNT(f.delay_minutes) AS classified_delay_flights,
        SUM(CASE
            WHEN f.delay_minutes IS NOT NULL
                 AND f.delay_minutes <= 15 THEN 1
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
    FROM fact_flight AS f
    JOIN dim_airline AS a
        ON f.airline_key = a.airline_key
    WHERE f.cancelled_flag = 0
    GROUP BY a.airline_name
),
airline_delay_severity AS (
    SELECT
        adc.airline_name,
        adc.classified_delay_flights,
        adc.on_time_flights,
        adc.minor_delay_flights,
        adc.moderate_delay_flights,
        adc.severe_delay_flights,
        ROUND(
            100.0 * adc.on_time_flights
            / NULLIF(adc.classified_delay_flights, 0),
            2
        ) AS on_time_pct,
        ROUND(
            100.0 * adc.minor_delay_flights
            / NULLIF(adc.classified_delay_flights, 0),
            2
        ) AS minor_delay_pct,
        ROUND(
            100.0 * adc.moderate_delay_flights
            / NULLIF(adc.classified_delay_flights, 0),
            2
        ) AS moderate_delay_pct,
        ROUND(
            100.0 * adc.severe_delay_flights
            / NULLIF(adc.classified_delay_flights, 0),
            2
        ) AS severe_delay_pct
    FROM airline_delay_counts AS adc
)
SELECT
    ads.airline_name,
    ads.classified_delay_flights,
    ads.on_time_flights,
    ads.minor_delay_flights,
    ads.moderate_delay_flights,
    ads.severe_delay_flights,
    ads.on_time_pct,
    ads.minor_delay_pct,
    ads.moderate_delay_pct,
    ads.severe_delay_pct,
    RANK() OVER (
        ORDER BY ads.severe_delay_pct DESC
    ) AS severity_rank
FROM airline_delay_severity AS ads
ORDER BY severity_rank, ads.airline_name;
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