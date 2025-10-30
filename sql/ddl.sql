-- sql/ddl.sql
-- Create staging table to load CSV
CREATE TABLE IF NOT EXISTS staging_weather (
  station_id     TEXT,
  name            TEXT,
  latitude        DOUBLE PRECISION,
  longitude       DOUBLE PRECISION,
  elevation       DOUBLE PRECISION,
  ts              TIMESTAMP,   -- parsed from DATE column in CSV
  tavg            DOUBLE PRECISION,
  tmax            DOUBLE PRECISION,
  tmin            DOUBLE PRECISION,
  prcp            DOUBLE PRECISION
);

-- Dim: stations
CREATE TABLE IF NOT EXISTS dim_station (
  station_key     SERIAL PRIMARY KEY,
  station_id      TEXT UNIQUE,
  name            TEXT,
  latitude        DOUBLE PRECISION,
  longitude       DOUBLE PRECISION,
  elevation       DOUBLE PRECISION
);

-- Fact: daily aggregated observations
CREATE TABLE IF NOT EXISTS fact_daily_obs (
  fact_id         BIGSERIAL PRIMARY KEY,
  station_key     INT REFERENCES dim_station(station_key),
  day_date        DATE NOT NULL,
  temperature_avg DOUBLE PRECISION,
  prcp_sum        DOUBLE PRECISION,
  obs_count       INT,
  year            INT
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_staging_ts ON staging_weather(ts);
CREATE INDEX IF NOT EXISTS idx_fact_station_date ON fact_daily_obs(station_key, day_date);
CREATE INDEX IF NOT EXISTS idx_fact_year ON fact_daily_obs(year);
