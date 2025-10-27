-- Export monthly climatology for dashboards
\copy (
  SELECT * FROM mv_monthly_station 
  WHERE station_id = 'IN009012800'
  ORDER BY month_date
) TO 'export/monthly_IN009012800.csv' CSV HEADER;
