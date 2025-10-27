-- Count anomalies per year per station (z > 2.5)
WITH z AS (
  SELECT m.station_id, m.station_name, m.year, m.month,
         (m.avg_tavg - s.mu) / s.sigma AS z_monthly_tavg
  FROM mv_monthly_station m
  JOIN (
    SELECT station_id, month, AVG(avg_tavg) AS mu, NULLIF(STDDEV(avg_tavg),0) AS sigma
    FROM mv_monthly_station
    GROUP BY station_id, month
  ) s ON m.station_id = s.station_id AND m.month = s.month
)
SELECT station_id, year,
       COUNT(*) FILTER (WHERE ABS(z_monthly_tavg) > 2.5) AS num_anomalous_months
FROM z
GROUP BY station_id, year
ORDER BY station_id, year;
