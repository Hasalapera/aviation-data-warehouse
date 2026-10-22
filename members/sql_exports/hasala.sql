-- Exported from members/hasala.py
-- Query IDs: Q1-Q5

-- Q1 - Airline Performance & Passenger Volume
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

-- Q2 - Airline Cancellation Rates
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

-- Q3 - Flights vs Passengers
SELECT
    a.airline_name,
    COUNT(f.flight_key) AS total_flights,
    COALESCE(SUM(f.passenger_count), 0) AS total_passengers
FROM fact_flight f
JOIN dim_airline a
    ON f.airline_key = a.airline_key
GROUP BY a.airline_name
ORDER BY total_flights DESC;

-- Q4 - Delay Trend by Quarters
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

-- Q5 - Delay Severity
SELECT
    a.airline_name,
    SUM(CASE WHEN f.delay_minutes <= 15 THEN 1 ELSE 0 END) AS on_time_flights,
    SUM(CASE WHEN f.delay_minutes > 15 AND f.delay_minutes <= 30 THEN 1 ELSE 0 END) AS minor_delay_flights,
    SUM(CASE WHEN f.delay_minutes > 30 AND f.delay_minutes <= 60 THEN 1 ELSE 0 END) AS moderate_delay_flights,
    SUM(CASE WHEN f.delay_minutes > 60 THEN 1 ELSE 0 END) AS severe_delay_flights
FROM fact_flight AS f
JOIN dim_airline AS a
    ON f.airline_key = a.airline_key
GROUP BY a.airline_name
ORDER BY a.airline_name;
