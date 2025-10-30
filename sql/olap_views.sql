-- sql/olap_views.sql
-- 1) Monthly averages per station (seasonal patterns)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_avg AS
SELECT
  f.station_key,
  f.year,
  EXTRACT(MONTH FROM f.day_date)::int AS month,
  AVG(f.temperature_avg) AS monthly_temp_avg
FROM fact_daily_obs f
GROUP BY f.station_key, f.year, EXTRACT(MONTH FROM f.day_date)
WITH NO DATA;

-- 2) 30-day rolling average per station (for smoothing)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_30d_rolling AS
SELECT
  f.station_key,
  f.day_date,
  AVG(f.temperature_avg) OVER (PARTITION BY f.station_key ORDER BY f.day_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30d_temp
FROM fact_daily_obs f
WITH NO DATA;

-- 3) Anomalies via z-score per station (flag days with |z|>2.5)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_anomalies AS
WITH stats AS (
  SELECT
    station_key,
    AVG(temperature_avg) AS mu,
    STDDEV(temperature_avg) AS sigma
  FROM fact_daily_obs
  GROUP BY station_key
)
SELECT
  f.station_key,
  f.day_date,
  f.temperature_avg,
  (f.temperature_avg - s.mu) / NULLIF(s.sigma, 0) AS zscore
FROM fact_daily_obs f
JOIN stats s USING (station_key)
WHERE ABS((f.temperature_avg - s.mu) / NULLIF(s.sigma,0)) > 2.5
WITH NO DATA;

-- 4) Long-term trend (slope) using regr_slope over epoch day (seconds)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trends AS
SELECT
  f.station_key,
  regr_slope(f.temperature_avg, EXTRACT(EPOCH FROM f.day_date)::double precision) AS slope
FROM fact_daily_obs f
WHERE f.temperature_avg IS NOT NULL
GROUP BY f.station_key
WITH NO DATA;
