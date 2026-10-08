CREATE DATABASE IF NOT EXISTS aviation_dw;

USE aviation_dw;


-- =========================================================
-- 1. DATE DIMENSION
-- =========================================================

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day INT,
    month INT,
    month_name VARCHAR(20),
    quarter INT,
    year INT,
    day_name VARCHAR(20)
);


-- =========================================================
-- 2. AIRLINE DIMENSION
-- =========================================================

CREATE TABLE IF NOT EXISTS dim_airline (
    airline_key INT PRIMARY KEY AUTO_INCREMENT,
    airline_code VARCHAR(10) NOT NULL,
    airline_name VARCHAR(100) NOT NULL,
    country VARCHAR(100),

    CONSTRAINT uq_airline_code
        UNIQUE (airline_code)
);


-- =========================================================
-- 3. AIRPORT DIMENSION
-- =========================================================

CREATE TABLE IF NOT EXISTS dim_airport (
    airport_key INT PRIMARY KEY AUTO_INCREMENT,
    airport_code VARCHAR(10) NOT NULL,
    airport_name VARCHAR(150) NOT NULL,
    city VARCHAR(100),
    country VARCHAR(100),

    CONSTRAINT uq_airport_code
        UNIQUE (airport_code)
);


-- =========================================================
-- 4. ROUTE DIMENSION
-- =========================================================

CREATE TABLE IF NOT EXISTS dim_route (
    route_key INT AUTO_INCREMENT PRIMARY KEY,

    origin_airport_key INT NOT NULL,
    destination_airport_key INT NOT NULL,

    CONSTRAINT fk_route_origin
        FOREIGN KEY (origin_airport_key)
        REFERENCES dim_airport(airport_key),

    CONSTRAINT fk_route_destination
        FOREIGN KEY (destination_airport_key)
        REFERENCES dim_airport(airport_key),

    CONSTRAINT uq_route
        UNIQUE (
            origin_airport_key,
            destination_airport_key
        )
);


-- =========================================================
-- 5. WEATHER DIMENSION
-- =========================================================

CREATE TABLE IF NOT EXISTS dim_weather (
    weather_key INT PRIMARY KEY AUTO_INCREMENT,

    temperature DECIMAL(5,2),
    rainfall DECIMAL(6,2),
    wind_speed DECIMAL(6,2),
    visibility DECIMAL(6,2),
    weather_condition VARCHAR(50)
);


-- =========================================================
-- 6. FLIGHT FACT TABLE
-- =========================================================

CREATE TABLE IF NOT EXISTS fact_flight (

    flight_key BIGINT AUTO_INCREMENT PRIMARY KEY,

    date_key INT NOT NULL,
    airline_key INT NOT NULL,
    route_key INT NOT NULL,
    weather_key INT,

    flight_number VARCHAR(20),

    departure_time DATETIME,
    arrival_time DATETIME,

    delay_minutes INT DEFAULT 0,

    arrival_delay_minutes INT DEFAULT 0,

    flight_duration_minutes INT,

    distance_km DECIMAL(8,2),

    passenger_count INT,

    cancelled_flag BOOLEAN DEFAULT FALSE,

    diverted_flag BOOLEAN DEFAULT FALSE,

    CONSTRAINT fk_fact_date
        FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    CONSTRAINT fk_fact_airline
        FOREIGN KEY (airline_key)
        REFERENCES dim_airline(airline_key),

    CONSTRAINT fk_fact_route
        FOREIGN KEY (route_key)
        REFERENCES dim_route(route_key),

    CONSTRAINT fk_fact_weather
        FOREIGN KEY (weather_key)
        REFERENCES dim_weather(weather_key),

    INDEX idx_fact_flight_number (flight_number),
    INDEX idx_fact_date (date_key),
    INDEX idx_fact_airline (airline_key),
    INDEX idx_fact_route (route_key)
);