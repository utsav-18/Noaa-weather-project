-- sql/example_reports.sql

-- A: Monthly seasonal patterns for a single station (example station_id 'IN009012800'):
SELECT ds.station_id, ds.name, m.year, m.month, m.monthly_temp_avg
FROM mv_monthly_avg m
JOIN dim_station ds ON ds.station_key = m.station_key
WHERE ds.station_id = 'IN009012800'
ORDER BY m.year, m.month;

-- B: Top anomalies (zscore) across stations
SELECT ds.station_id, ds.name, a.day_date, a.temperature_avg, a.zscore
FROM mv_anomalies a
JOIN dim_station ds ON ds.station_key = a.station_key
ORDER BY ABS(a.zscore) DESC
LIMIT 100;

-- C: Rolling average sample (last 180 days) for one station
SELECT ds.station_id, r.day_date, r.rolling_30d_temp
FROM mv_30d_rolling r
JOIN dim_station ds ON ds.station_key = r.station_key
WHERE ds.station_id = 'IN009012800' AND r.day_date >= current_date - INTERVAL '180 days'
ORDER BY r.day_date;

-- D: Trend slopes (which stations warming/cooling)
SELECT ds.station_id, ds.name, t.slope
FROM mv_trends t
JOIN dim_station ds ON ds.station_key = t.station_key
ORDER BY t.slope DESC;

-- E: IQR-based outliers (alternative anomaly method)
WITH q AS (
  SELECT station_key,
         percentile_cont(0.25) WITHIN GROUP (ORDER BY temperature_avg) AS q1,
         percentile_cont(0.75) WITHIN GROUP (ORDER BY temperature_avg) AS q3
  FROM fact_daily_obs
  GROUP BY station_key
)
SELECT ds.station_id, ds.name, f.day_date, f.temperature_avg
FROM fact_daily_obs f
JOIN q ON q.station_key = f.station_key
JOIN dim_station ds ON ds.station_key = f.station_key
WHERE f.temperature_avg < q.q1 - 1.5*(q.q3 - q.q1)
   OR f.temperature_avg > q.q3 + 1.5*(q.q3 - q.q1)
ORDER BY ds.station_id, f.day_date;
