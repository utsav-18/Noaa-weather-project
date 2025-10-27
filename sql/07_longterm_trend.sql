-- Annual aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_annual_station AS
SELECT station_id, station_name, year, AVG(avg_tavg) AS annual_avg_tavg
FROM mv_monthly_station
GROUP BY station_id, station_name, year
ORDER BY station_id, year;

-- Linear regression trend per station
SELECT station_id, station_name,
       regr_slope(annual_avg_tavg, year) AS trend_slope_per_year,
       regr_intercept(annual_avg_tavg, year) AS intercept,
       corr(annual_avg_tavg, year) AS corr_year
FROM mv_annual_station
GROUP BY station_id, station_name
ORDER BY trend_slope_per_year DESC;
