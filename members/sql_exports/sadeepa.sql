-- Exported from members/sadeepa.py
-- Query IDs: Q11-Q15

-- Q11 - Average Delay by Weather Condition
SELECT
    w.weather_condition,
    COUNT(f.flight_key) AS total_flights,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins
FROM fact_flight f
JOIN dim_weather w
    ON f.weather_key = w.weather_key
GROUP BY w.weather_condition
ORDER BY avg_delay_mins DESC;

-- Q12 - Cancellation Rate by Weather Condition
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

-- Q13 - Rainfall vs Departure Delay
SELECT
    w.rainfall AS rainfall_mm,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins,
    COUNT(f.flight_key) AS total_flights
FROM fact_flight f
JOIN dim_weather w
    ON f.weather_key = w.weather_key
GROUP BY w.rainfall
ORDER BY w.rainfall;

-- Q14 - Visibility Level vs Average Delay
SELECT
    CASE
        WHEN w.visibility < 2 THEN 'Low'
        WHEN w.visibility < 5 THEN 'Medium'
        ELSE 'High'
    END AS visibility_level,
    COUNT(f.flight_key) AS total_flights,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins
FROM fact_flight f
JOIN dim_weather w
    ON f.weather_key = w.weather_key
GROUP BY
    CASE
        WHEN w.visibility < 2 THEN 'Low'
        WHEN w.visibility < 5 THEN 'Medium'
        ELSE 'High'
    END
ORDER BY avg_delay_mins DESC;

-- Q15 - Temperature vs Flight Delay
SELECT
    w.temperature AS temperature_c,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins,
    COUNT(f.flight_key) AS flight_volume
FROM fact_flight f
JOIN dim_weather w
    ON f.weather_key = w.weather_key
GROUP BY w.temperature
ORDER BY w.temperature;
