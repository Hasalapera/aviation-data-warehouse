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

if __name__ == "__main__":
    execute_analytics_query("Airline Performance & Passenger Volume", QUERY_AIRLINE_METRICS)