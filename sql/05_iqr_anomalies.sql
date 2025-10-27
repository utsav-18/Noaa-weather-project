-- IQR-based anomaly detection
WITH per_station_month AS (
  SELECT f.station_id, t.month, f.tavg
  FROM fact_weather f
  JOIN dim_time t ON f.time_id = t.time_id
),
quant AS (
  SELECT station_id, month,
         percentile_cont(0.25) WITHIN GROUP (ORDER BY tavg) AS q1,
         percentile_cont(0.75) WITHIN GROUP (ORDER BY tavg) AS q3
  FROM per_station_month
  GROUP BY station_id, month
)
SELECT f.station_id, t.date, f.tavg,
       CASE WHEN f.tavg < (q1 - 1.5*(q3-q1)) OR f.tavg > (q3 + 1.5*(q3-q1)) THEN TRUE ELSE FALSE END AS is_iqr_outlier
FROM fact_weather f
JOIN dim_time t ON f.time_id = t.time_id
JOIN quant q ON q.station_id = f.station_id AND q.month = t.month
LIMIT 100;
