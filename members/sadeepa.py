import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import execute_analytics_query


# Q11 - Average Delay by Weather Condition
QUERY_DELAY_BY_WEATHER = """
SELECT
    w.weather_condition,
    COUNT(f.flight_key) AS total_flights,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins
FROM fact_flight f
JOIN dim_weather w
    ON f.weather_key = w.weather_key
GROUP BY w.weather_condition
ORDER BY avg_delay_mins DESC;
"""


# Q12 - Cancellation Rate by Weather Condition
QUERY_CANCELLATION_BY_WEATHER = """
SELECT
    w.weather_condition,
    COUNT(f.flight_key) AS total_flights,
    SUM(CASE
        WHEN f.cancelled_flag = 1 THEN 1
        ELSE 0
    END) AS cancelled_flights,
    ROUND(
        SUM(CASE
            WHEN f.cancelled_flag = 1 THEN 1
            ELSE 0
        END) / COUNT(*) * 100,
        2
    ) AS cancellation_rate_pct
FROM fact_flight f
JOIN dim_weather w
    ON f.weather_key = w.weather_key
GROUP BY w.weather_condition
ORDER BY cancellation_rate_pct DESC;
"""


# Q13 - Rainfall vs Departure Delay
QUERY_RAINFALL_VS_DELAY = """
SELECT
    w.rainfall_mm,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins,
    COUNT(f.flight_key) AS total_flights
FROM fact_flight f
JOIN dim_weather w
    ON f.weather_key = w.weather_key
GROUP BY w.rainfall_mm
ORDER BY w.rainfall_mm;
"""


# Q14 - Visibility Level vs Average Delay
QUERY_VISIBILITY_VS_DELAY = """
SELECT
    CASE
        WHEN w.visibility_km < 2 THEN 'Low'
        WHEN w.visibility_km < 5 THEN 'Medium'
        ELSE 'High'
    END AS visibility_level,
    COUNT(f.flight_key) AS total_flights,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins
FROM fact_flight f
JOIN dim_weather w
    ON f.weather_key = w.weather_key
GROUP BY
    CASE
        WHEN w.visibility_km < 2 THEN 'Low'
        WHEN w.visibility_km < 5 THEN 'Medium'
        ELSE 'High'
    END
ORDER BY avg_delay_mins DESC;
"""


# Q15 - Temperature vs Flight Delay
QUERY_TEMPERATURE_VS_DELAY = """
SELECT
    w.temperature_c,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins,
    COUNT(f.flight_key) AS flight_volume
FROM fact_flight f
JOIN dim_weather w
    ON f.weather_key = w.weather_key
GROUP BY w.temperature_c
ORDER BY w.temperature_c;
"""


if __name__ == "__main__":

    execute_analytics_query(
        "Average Delay by Weather Condition",
        QUERY_DELAY_BY_WEATHER
    )

    execute_analytics_query(
        "Cancellation Rate by Weather Condition",
        QUERY_CANCELLATION_BY_WEATHER
    )

    execute_analytics_query(
        "Rainfall vs Departure Delay",
        QUERY_RAINFALL_VS_DELAY
    )

    execute_analytics_query(
        "Visibility Level vs Average Delay",
        QUERY_VISIBILITY_VS_DELAY
    )

    execute_analytics_query(
        "Temperature vs Flight Delay",
        QUERY_TEMPERATURE_VS_DELAY
    )