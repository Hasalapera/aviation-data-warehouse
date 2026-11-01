-- Exported from members/navodya.py
-- Query IDs: Q16-Q20

-- Q16 - Monthly Flight Volume Trend
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(f.flight_key) AS total_flights
FROM fact_flight f
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY
    d.year,
    d.month,
    d.month_name
ORDER BY
    d.year,
    d.month;

-- Q17 - Average Delay by Day of Week
SELECT
    WEEKDAY(d.full_date) + 1 AS day_of_week,
    d.day_name,
    COUNT(f.flight_key) AS total_flights,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins
FROM fact_flight f
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY
    WEEKDAY(d.full_date) + 1,
    d.day_name
ORDER BY day_of_week;

-- Q18 - Quarterly On-time Rate Trend
SELECT
    d.year,
    d.quarter,
    COUNT(f.flight_key) AS total_flights,
    ROUND(
        SUM(CASE
            WHEN f.delay_minutes <= 15
                 AND f.cancelled_flag = 0
            THEN 1
            ELSE 0
        END) / COUNT(*) * 100,
        2
    ) AS on_time_rate_pct
FROM fact_flight f
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY
    d.year,
    d.quarter
ORDER BY
    d.year,
    d.quarter;

-- Q19 - Flights by Time of Day
SELECT
    CASE
        WHEN HOUR(f.scheduled_departure) < 6 THEN '00-06'
        WHEN HOUR(f.scheduled_departure) < 12 THEN '06-12'
        WHEN HOUR(f.scheduled_departure) < 18 THEN '12-18'
        ELSE '18-24'
    END AS time_bucket,
    COUNT(f.flight_key) AS total_flights
FROM fact_flight f
WHERE f.scheduled_departure IS NOT NULL
GROUP BY
    CASE
        WHEN HOUR(f.scheduled_departure) < 6 THEN '00-06'
        WHEN HOUR(f.scheduled_departure) < 12 THEN '06-12'
        WHEN HOUR(f.scheduled_departure) < 18 THEN '12-18'
        ELSE '18-24'
    END
ORDER BY time_bucket;

-- Q20 - Month x Weekday Delay Heatmap
SELECT
    d.month,
    d.month_name,
    WEEKDAY(d.full_date) + 1 AS day_of_week,
    d.day_name,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins
FROM fact_flight f
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY
    d.month,
    d.month_name,
    WEEKDAY(d.full_date) + 1,
    d.day_name
ORDER BY
    d.month,
    day_of_week;
