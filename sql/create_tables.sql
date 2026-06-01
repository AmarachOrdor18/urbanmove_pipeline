-- UrbanMove Analytics Database Schema
-- Run this in pgAdmin to create all tables

-- Drop tables if they exist (for re-running)
DROP TABLE IF EXISTS ridership CASCADE;
DROP TABLE IF EXISTS vehicle_locations CASCADE;
DROP TABLE IF EXISTS weather CASCADE;

-- Table 1: Ridership data
CREATE TABLE ridership (
    id              SERIAL PRIMARY KEY,
    trip_id         VARCHAR(20) UNIQUE NOT NULL,
    route_id        VARCHAR(20) NOT NULL,
    stop_name       VARCHAR(50),
    passenger_count INTEGER,
    trip_date       DATE NOT NULL,
    trip_hour       INTEGER CHECK (trip_hour BETWEEN 0 AND 23),
    vehicle_id      VARCHAR(20),
    direction       VARCHAR(20),
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 2: Vehicle GPS locations
CREATE TABLE vehicle_locations (
    id              SERIAL PRIMARY KEY,
    location_id     VARCHAR(20) UNIQUE NOT NULL,
    vehicle_id      VARCHAR(20) NOT NULL,
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6),
    speed_kmh       DECIMAL(5,1),
    timestamp       TIMESTAMP,
    status          VARCHAR(20),
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 3: Weather conditions
CREATE TABLE weather (
    id              SERIAL PRIMARY KEY,
    weather_id      VARCHAR(20) UNIQUE NOT NULL,
    weather_date    DATE NOT NULL,
    temperature_c   DECIMAL(4,1),
    humidity_pct    INTEGER,
    rainfall_mm     DECIMAL(5,1),
    weather_condition VARCHAR(30),
    wind_speed_kmh  DECIMAL(4,1),
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Confirm tables created
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public';