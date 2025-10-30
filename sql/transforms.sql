-- sql/transforms.sql (fixed)
-- 1. Upsert stations into dim_station
INSERT INTO dim_station (station_id, name, latitude, longitude, elevation)
SELECT DISTINCT station_id, name, latitude, longitude, elevation
FROM staging_weather
WHERE station_id IS NOT NULL
ON CONFLICT (station_id) DO UPDATE
  SET name = EXCLUDED.name,
      latitude = COALESCE(EXCLUDED.latitude, dim_station.latitude),
      longitude = COALESCE(EXCLUDED.longitude, dim_station.longitude),
      elevation = COALESCE(EXCLUDED.elevation, dim_station.elevation);

-- 2. Aggregate staging into daily facts and upsert into fact_daily_obs
WITH daily AS (
  SELECT
    s.station_id,
    date_trunc('day', s.ts)::date AS day_date,
    AVG(s.tavg) FILTER (WHERE s.tavg IS NOT NULL)    AS temperature_avg,
    SUM(s.prcp) FILTER (WHERE s.prcp IS NOT NULL)    AS prcp_sum,
    COUNT(*)                                          AS obs_count
  FROM staging_weather s
  WHERE s.station_id IS NOT NULL AND s.ts IS NOT NULL
  GROUP BY s.station_id, date_trunc('day', s.ts)::date
)
INSERT INTO fact_daily_obs (station_key, day_date, temperature_avg, prcp_sum, obs_count, year)
SELECT
  ds.station_key,
  d.day_date,
  d.temperature_avg,
  d.prcp_sum,
  d.obs_count,
  EXTRACT(YEAR FROM d.day_date)::int AS year
FROM daily d
JOIN dim_station ds ON ds.station_id = d.station_id
ON CONFLICT (station_key, day_date) DO UPDATE
  SET temperature_avg = EXCLUDED.temperature_avg,
      prcp_sum = EXCLUDED.prcp_sum,
      obs_count = EXCLUDED.obs_count,
      year = EXCLUDED.year;
