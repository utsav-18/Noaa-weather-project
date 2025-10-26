import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np

# Path to CSV
file_path = "raw/noaa_weather_new.csv"

# ---------- Read CSV robustly ----------
dtype_map = {
    'STATION': 'string',
    'NAME': 'string',
    'LATITUDE': 'float',
    'LONGITUDE': 'float',
    'ELEVATION': 'float',
    'PRCP_ATTRIBUTES': 'string',
    'TAVG_ATTRIBUTES': 'string',
    'TMAX_ATTRIBUTES': 'string',
    'TMIN_ATTRIBUTES': 'string',
}

df = pd.read_csv(
    file_path,
    dtype=dtype_map,
    parse_dates=['DATE'],
    na_values=['', 'NA', 'NaN'],
    keep_default_na=True,
    low_memory=False
)

print("CSV columns:", df.columns.tolist())

# Coerce numeric measurement columns to floats; invalid -> NaN
for col in ['PRCP', 'TAVG', 'TMAX', 'TMIN']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Drop rows that have no DATE (NaT) because we cannot map them to dim_time
df = df[~df['DATE'].isna()].reset_index(drop=True)

# ---------- Connect to PostgreSQL ----------
conn = psycopg2.connect(
    host="localhost",
    database="weatherdb",
    user="postgres",
    password="Mutton@01"  
)
cur = conn.cursor()

# -----------------------------
# Insert source (idempotent)
# -----------------------------
cur.execute("""
INSERT INTO dim_source (source_name, source_url)
VALUES (%s, %s)
ON CONFLICT (source_name) DO NOTHING;
""", ("NOAA", "https://www.ncei.noaa.gov/"))
conn.commit()

# Get source_id
cur.execute("SELECT source_id FROM dim_source WHERE source_name = %s", ("NOAA",))
source_row = cur.fetchone()
if source_row is None:
    raise RuntimeError("Failed to insert / find source NOAA")
source_id = source_row[0]

# -----------------------------
# Insert stations
# -----------------------------
stations = df[['STATION', 'NAME', 'LATITUDE', 'LONGITUDE', 'ELEVATION']].drop_duplicates().reset_index(drop=True)

# Convert NaN -> None so psycopg2 inserts NULL
def row_to_tuple_for_db(row):
    return tuple(None if (isinstance(x, float) and np.isnan(x)) else (None if pd.isna(x) else x) for x in row)

station_values = [row_to_tuple_for_db(r) for r in stations.values]

sql_station = """
INSERT INTO dim_station (station_id, station_name, latitude, longitude, elevation)
VALUES %s
ON CONFLICT (station_id) DO NOTHING;
"""

if station_values:
    execute_values(cur, sql_station, station_values)
    conn.commit()
print(f"dim_station table updated with {len(station_values)} stations")

# -----------------------------
# Insert time dimension
# -----------------------------
# Build unique dates from df, drop duplicates
unique_dates = pd.to_datetime(df['DATE']).dt.date.drop_duplicates()

time_values = []
for d in unique_dates:
    # d is a datetime.date
    time_values.append((d, d.year, d.month, d.day))

sql_time = """
INSERT INTO dim_time (date, year, month, day)
VALUES %s
ON CONFLICT (date) DO NOTHING;
"""

if time_values:
    execute_values(cur, sql_time, time_values)
    conn.commit()
print(f"dim_time table updated with {len(time_values)} dates")

# -----------------------------
# Build a date -> time_id map
# -----------------------------
cur.execute("SELECT date, time_id FROM dim_time")
date_map = {row[0]: row[1] for row in cur.fetchall()}

# -----------------------------
# Insert fact table (batch)
# -----------------------------
fact_values = []
missing_dates = 0
for _, row in df.iterrows():
    date_obj = row['DATE'].date()
    time_id = date_map.get(date_obj)
    if time_id is None:
        missing_dates += 1
        continue

    # Convert NaN -> None for numeric fields
    prcp = None if pd.isna(row.get('PRCP')) else float(row.get('PRCP'))
    tavg = None if pd.isna(row.get('TAVG')) else float(row.get('TAVG'))
    tmax = None if pd.isna(row.get('TMAX')) else float(row.get('TMAX'))
    tmin = None if pd.isna(row.get('TMIN')) else float(row.get('TMIN'))

    fact_values.append((
        row['STATION'],
        time_id,
        source_id,
        prcp,
        tavg,
        tmax,
        tmin
    ))

if missing_dates:
    print(f"Skipped {missing_dates} rows because their date wasn't found in dim_time")

sql_fact = """
INSERT INTO fact_weather (station_id, time_id, source_id, prcp, tavg, tmax, tmin)
VALUES %s
"""
if fact_values:
    execute_values(cur, sql_fact, fact_values)
    conn.commit()
print(f"Inserted {len(fact_values)} rows into fact_weather")

# Finalize
cur.close()
conn.close()
