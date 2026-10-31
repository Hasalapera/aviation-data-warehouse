-- Exported from members/gihan.py
-- Query IDs: Q21-Q25

-- Q21 - Overall Flight KPIs
SELECT
    COUNT(f.flight_key) AS total_flights,
    SUM(CASE WHEN f.cancelled_flag = 1 THEN 1 ELSE 0 END) AS total_cancellations,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins,
    ROUND(
        SUM(
            CASE
                WHEN f.delay_minutes <= 15
                     AND f.cancelled_flag = 0
                THEN 1
                ELSE 0
            END
        ) / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS on_time_rate_pct
FROM fact_flight f;

-- Q22 - Passenger Volume by Airline
SELECT
    a.airline_name,
    COUNT(f.flight_key) AS total_flights,
    COALESCE(SUM(f.passenger_count), 0) AS total_passengers
FROM fact_flight f
JOIN dim_airline a
    ON f.airline_key = a.airline_key
GROUP BY a.airline_name
ORDER BY total_passengers DESC;

-- Q23 - Top Routes by Passenger Demand
SELECT
    o.airport_code AS origin_airport_code,
    d.airport_code AS destination_airport_code,
    COALESCE(SUM(f.passenger_count), 0) AS total_passengers,
    COUNT(f.flight_key) AS total_flights
FROM fact_flight f
JOIN dim_route r
    ON f.route_key = r.route_key
JOIN dim_airport o
    ON r.origin_airport_key = o.airport_key
JOIN dim_airport d
    ON r.destination_airport_key = d.airport_key
GROUP BY
    o.airport_code,
    d.airport_code
ORDER BY total_passengers DESC
LIMIT 10;

-- Q24 - Passenger Load vs Flight Delay
SELECT
    f.passenger_count,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins,
    COUNT(f.flight_key) AS flight_count
FROM fact_flight f
WHERE f.passenger_count IS NOT NULL
GROUP BY f.passenger_count
ORDER BY f.passenger_count;

-- Q25 - Cancelled vs Operated Flights by Month
SELECT
    d.`year`,
    d.`month`,
    d.month_name,
    SUM(
        CASE
            WHEN f.cancelled_flag = 1 THEN 1
            ELSE 0
        END
    ) AS cancelled_flights,
    SUM(
        CASE
            WHEN f.cancelled_flag = 0 THEN 1
            ELSE 0
        END
    ) AS operated_flights
FROM fact_flight AS f
JOIN dim_date AS d
    ON f.date_key = d.date_key
GROUP BY
    d.`year`,
    d.`month`,
    d.month_name
ORDER BY
    d.`year`,
    d.`month`;
