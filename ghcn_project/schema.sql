CREATE TABLE IF NOT EXISTS dim_station (
  station_id TEXT PRIMARY KEY,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  elevation DOUBLE PRECISION,
  country_code TEXT,
  name TEXT
);

CREATE TABLE IF NOT EXISTS fact_temperature (
  id BIGSERIAL PRIMARY KEY,
  station_id TEXT REFERENCES dim_station(station_id),
  year INTEGER NOT NULL,
  month SMALLINT NOT NULL,
  temp_c REAL,
  UNIQUE(station_id, year, month)
);

CREATE INDEX IF NOT EXISTS idx_fact_temp_year ON fact_temperature(year);
CREATE INDEX IF NOT EXISTS idx_fact_temp_station_year ON fact_temperature(station_id, year);
CREATE INDEX IF NOT EXISTS idx_fact_temp_year_month ON fact_temperature(year, month);
