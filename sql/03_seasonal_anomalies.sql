-- Monthly z-score anomaly vs climatology
WITH stats AS (
  SELECT station_id, month,
         AVG(avg_tavg) AS mu,
         NULLIF(STDDEV(avg_tavg),0) AS sigma
  FROM mv_monthly_station
  GROUP BY station_id, month
)
SELECT m.station_id, m.station_name, m.year, m.month, m.month_date, m.avg_tavg,
       (m.avg_tavg - s.mu) / s.sigma AS z_monthly_tavg
FROM mv_monthly_station m
JOIN stats s ON m.station_id = s.station_id AND m.month = s.month
ORDER BY m.station_id, m.month_date;
