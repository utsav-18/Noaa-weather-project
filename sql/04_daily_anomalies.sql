-- Daily-level z-score anomaly per station
WITH stats AS (
  SELECT station_id,
         AVG(tavg) AS mu,
         NULLIF(STDDEV(tavg),0) AS sigma
  FROM fact_weather
  GROUP BY station_id
)
SELECT f.station_id, d.date, f.tavg,
       (f.tavg - st.mu) / st.sigma AS z_daily_tavg
FROM fact_weather f
JOIN dim_time d ON f.time_id = d.time_id
JOIN stats st ON f.station_id = st.station_id
ORDER BY f.station_id, d.date;
