# load_postgres.py
import os
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

CSV = Path("etl/sample_transformed.csv")
DB = {
    'host': os.getenv('PGHOST','localhost'),
    'port': os.getenv('PGPORT','5432'),
    'dbname': os.getenv('PGDATABASE','weatherdb'),
    'user': os.getenv('PGUSER','etl_user'),
    'password': os.getenv('PGPASSWORD','ChangeMe123')
}

if not CSV.exists():
    raise SystemExit(f"{CSV} not found. Run ETL first.")

conn = psycopg2.connect(host=DB['host'], port=DB['port'], dbname=DB['dbname'],
                        user=DB['user'], password=DB['password'])
cur = conn.cursor()

# ensure NOAA source exists
cur.execute("INSERT INTO dim_source (source_name, source_url) VALUES (%s,%s) ON CONFLICT (source_name) DO NOTHING",
            ('NOAA','https://www.ncei.noaa.gov/'))
conn.commit()

# truncate staging then copy CSV (assumes header matches staging columns)
cur.execute("TRUNCATE TABLE staging_weather;")
with open(CSV, 'r', encoding='utf-8') as f:
    cur.copy_expert("COPY staging_weather(station_id, ts, year, month, day, hour, temperature_c, humidity, precipitation_mm, wind_speed_kmh, quality_flag) FROM STDIN WITH CSV HEADER", f)
conn.commit()

# insert missing stations
cur.execute("""
INSERT INTO dim_station (station_id, station_name)
SELECT DISTINCT station_id, 'Station ' || station_id
FROM staging_weather s
WHERE station_id IS NOT NULL
ON CONFLICT (station_id) DO NOTHING;
""")
conn.commit()

# ensure unique index for upsert exists (create once)
cur.execute("""
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                 WHERE c.relkind='i' AND c.relname='uniq_station_ts') THEN
    CREATE UNIQUE INDEX uniq_station_ts ON fact_weather (station_id, ts);
  END IF;
END$$;
""")
conn.commit()

# upsert from staging to fact_weather
cur.execute("""
INSERT INTO fact_weather (station_id, ts, source_id, temperature_c, humidity, precipitation_mm, wind_speed_kmh, quality_flag, year, month, day, hour)
SELECT s.station_id, s.ts, ds.source_id, s.temperature_c, s.humidity, s.precipitation_mm, s.wind_speed_kmh, s.quality_flag, s.year, s.month, s.day, s.hour
FROM staging_weather s CROSS JOIN (SELECT source_id FROM dim_source WHERE source_name='NOAA' LIMIT 1) ds
ON CONFLICT (station_id, ts) DO UPDATE
  SET temperature_c = EXCLUDED.temperature_c,
      humidity = EXCLUDED.humidity,
      precipitation_mm = EXCLUDED.precipitation_mm,
      wind_speed_kmh = EXCLUDED.wind_speed_kmh,
      quality_flag = EXCLUDED.quality_flag,
      year = EXCLUDED.year,
      month = EXCLUDED.month,
      day = EXCLUDED.day,
      hour = EXCLUDED.hour;
""")
conn.commit()
print("Load complete. Rows in fact_weather now:")
cur.execute("SELECT COUNT(*) FROM fact_weather;")
print(cur.fetchone()[0])

cur.close()
conn.close()
