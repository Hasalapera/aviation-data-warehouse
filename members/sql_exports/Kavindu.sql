-- Exported from members/Kavindu.py
-- Query IDs: Q6-Q10

-- Q6 - Top Origin Airports by Departure Volume
SELECT
    a.airport_code,
    a.airport_name,
    COUNT(f.flight_key) AS departure_flights
FROM fact_flight f
JOIN dim_route r
    ON f.route_key = r.route_key
JOIN dim_airport a
    ON r.origin_airport_key = a.airport_key
GROUP BY
    a.airport_code,
    a.airport_name
ORDER BY departure_flights DESC
LIMIT 10;

-- Q7 - Busiest Routes by Flight Count
SELECT
    o.airport_code AS origin_airport_code,
    d.airport_code AS destination_airport_code,
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
ORDER BY total_flights DESC
LIMIT 10;

-- Q8 - Worst Routes by Average Delay
SELECT
    o.airport_code AS origin_airport_code,
    d.airport_code AS destination_airport_code,
    COUNT(f.flight_key) AS total_flights,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins
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
HAVING COUNT(f.flight_key) >= 5
ORDER BY avg_delay_mins DESC
LIMIT 15;

-- Q9 - Arrivals vs Departures by Airport
SELECT
    a.airport_code,
    a.airport_name,
    COUNT(DISTINCT CASE
        WHEN r.origin_airport_key = a.airport_key
        THEN f.flight_key
    END) AS departures,
    COUNT(DISTINCT CASE
        WHEN r.destination_airport_key = a.airport_key
        THEN f.flight_key
    END) AS arrivals
FROM dim_airport a
LEFT JOIN dim_route r
    ON r.origin_airport_key = a.airport_key
    OR r.destination_airport_key = a.airport_key
LEFT JOIN fact_flight f
    ON f.route_key = r.route_key
GROUP BY
    a.airport_code,
    a.airport_name
ORDER BY (departures + arrivals) DESC;

-- Q10 - Cancellations by Route
SELECT
    o.airport_code AS origin_airport_code,
    d.airport_code AS destination_airport_code,
    COUNT(f.flight_key) AS total_flights,
    SUM(CASE
        WHEN f.cancelled_flag = 1 THEN 1
        ELSE 0
    END) AS cancelled_flights
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
ORDER BY cancelled_flights DESC
LIMIT 10;
