import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import execute_analytics_query


# Q6 - Top Origin Airports by Departure Volume
QUERY_TOP_ORIGIN_AIRPORTS = """
SELECT
    a.airport_code,
    a.airport_name,
    COUNT(f.flight_key) AS departure_flights
FROM fact_flight f
JOIN dim_airport a
    ON f.origin_airport_key = a.airport_key
GROUP BY
    a.airport_code,
    a.airport_name
ORDER BY departure_flights DESC
LIMIT 10;
"""


# Q7 - Busiest Routes by Flight Count
QUERY_BUSIEST_ROUTES = """
SELECT
    r.origin_airport_code,
    r.destination_airport_code,
    COUNT(f.flight_key) AS total_flights
FROM fact_flight f
JOIN dim_route r
    ON f.route_key = r.route_key
GROUP BY
    r.origin_airport_code,
    r.destination_airport_code
ORDER BY total_flights DESC
LIMIT 10;
"""


# Q8 - Worst Routes by Average Delay
QUERY_WORST_ROUTES_DELAY = """
SELECT
    r.origin_airport_code,
    r.destination_airport_code,
    COUNT(f.flight_key) AS total_flights,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_mins
FROM fact_flight f
JOIN dim_route r
    ON f.route_key = r.route_key
GROUP BY
    r.origin_airport_code,
    r.destination_airport_code
HAVING COUNT(f.flight_key) >= 5
ORDER BY avg_delay_mins DESC
LIMIT 15;
"""


# Q9 - Arrivals vs Departures by Airport
QUERY_ARRIVALS_VS_DEPARTURES = """
SELECT
    a.airport_code,
    a.airport_name,
    COUNT(DISTINCT CASE
        WHEN f.origin_airport_key = a.airport_key
        THEN f.flight_key
    END) AS departures,
    COUNT(DISTINCT CASE
        WHEN f.destination_airport_key = a.airport_key
        THEN f.flight_key
    END) AS arrivals
FROM dim_airport a
LEFT JOIN fact_flight f
    ON f.origin_airport_key = a.airport_key
    OR f.destination_airport_key = a.airport_key
GROUP BY
    a.airport_code,
    a.airport_name
ORDER BY (departures + arrivals) DESC;
"""


# Q10 - Cancellations by Route
QUERY_CANCELLATIONS_BY_ROUTE = """
SELECT
    r.origin_airport_code,
    r.destination_airport_code,
    COUNT(f.flight_key) AS total_flights,
    SUM(CASE
        WHEN f.cancelled_flag = 1 THEN 1
        ELSE 0
    END) AS cancelled_flights
FROM fact_flight f
JOIN dim_route r
    ON f.route_key = r.route_key
GROUP BY
    r.origin_airport_code,
    r.destination_airport_code
ORDER BY cancelled_flights DESC
LIMIT 10;
"""


if __name__ == "__main__":

    execute_analytics_query(
        "Top Origin Airports by Departure Volume",
        QUERY_TOP_ORIGIN_AIRPORTS
    )

    execute_analytics_query(
        "Busiest Routes by Flight Count",
        QUERY_BUSIEST_ROUTES
    )

    execute_analytics_query(
        "Worst Routes by Average Delay",
        QUERY_WORST_ROUTES_DELAY
    )

    execute_analytics_query(
        "Arrivals vs Departures by Airport",
        QUERY_ARRIVALS_VS_DEPARTURES
    )

    execute_analytics_query(
        "Cancellations by Route",
        QUERY_CANCELLATIONS_BY_ROUTE
    )