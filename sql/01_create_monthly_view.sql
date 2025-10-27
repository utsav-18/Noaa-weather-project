-- Creates materialized monthly view
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_station AS
SELECT
  f.station_id,
  s.station_name,
  t.year,
  t.month,
  MAKE_DATE(t.year, t.month, 1) AS month_date,
  AVG(f.tavg) AS avg_tavg,
  AVG(f.tmax) AS avg_tmax,
  AVG(f.tmin) AS avg_tmin,
  SUM(f.prcp) AS total_prcp,
  COUNT(*) AS obs_count
FROM fact_weather f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_station s ON f.station_id = s.station_id
GROUP BY f.station_id, s.station_name, t.year, t.month
ORDER BY f.station_id, t.year, t.month;

REFRESH MATERIALIZED VIEW mv_monthly_station;
