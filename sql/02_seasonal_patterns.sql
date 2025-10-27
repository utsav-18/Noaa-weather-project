-- Multi-year average per month (seasonality)
SELECT
  station_id,
  station_name,
  month,
  AVG(avg_tavg) AS climatology_tavg,
  STDDEV(avg_tavg) AS climatology_sd,
  AVG(total_prcp) AS climatology_prcp
FROM mv_monthly_station
GROUP BY station_id, station_name, month
ORDER BY station_id, month;
