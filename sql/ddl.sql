-- Drop tables if they exist
DROP TABLE IF EXISTS fact_weather;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS dim_station;
DROP TABLE IF EXISTS dim_source;

-- Station metadata
CREATE TABLE dim_station (
    station_id VARCHAR(20) PRIMARY KEY,
    station_name VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    elevation FLOAT
);

-- Time dimension
CREATE TABLE dim_time (
    time_id SERIAL PRIMARY KEY,
    date DATE UNIQUE,
    year INT,
    month INT,
    day INT
);

-- Source info with UNIQUE constraint on source_name
CREATE TABLE dim_source (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(255) UNIQUE,
    source_url VARCHAR(255)
);

-- Fact table
CREATE TABLE fact_weather (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(20) REFERENCES dim_station(station_id),
    time_id INT REFERENCES dim_time(time_id),
    source_id INT REFERENCES dim_source(source_id),
    prcp FLOAT,
    tavg FLOAT,
    tmax FLOAT,
    tmin FLOAT
);
