-- 30-day rolling average per station
SELECT station_id, d.date,
       AVG(f.tavg) OVER (PARTITION BY f.station_id ORDER BY d.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30d_tavg
FROM fact_weather f
JOIN dim_time d ON f.time_id = d.time_id
ORDER BY station_id, d.date;

-- 3-month rolling average (monthly data)
SELECT station_id, month_date,
       AVG(avg_tavg) OVER (PARTITION BY station_id ORDER BY month_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3m
FROM mv_monthly_station
ORDER BY station_id, month_date;
