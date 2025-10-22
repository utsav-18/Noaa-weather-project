import pandas as pd
import mysql.connector

# -------------------------------
# MySQL connection settings
# -------------------------------
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',             # your MySQL username
    'password': 'Mutton@01', # your MySQL password
    'database': 'weatherdb',
    'charset': 'utf8mb4'
}

# -------------------------------
# Load cleaned CSV from ETL
# -------------------------------
csv_path = "etl/sample_transformed.csv"
df = pd.read_csv(csv_path)
print("CSV columns:", df.columns.tolist())

# Replace NaNs with None
df = df.where(pd.notnull(df), None)

# -------------------------------
# Connect to MySQL
# -------------------------------
try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # -------------------------------
    # Ensure NOAA source exists
    # -------------------------------
    cursor.execute("SELECT source_id FROM dim_source WHERE source_id = 1")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO dim_source (source_id, source_name, source_url) VALUES (%s, %s, %s)",
            (1, "NOAA", "https://www.ncei.noaa.gov/")
        )
        conn.commit()
        print("Inserted NOAA source into dim_source")

    # -------------------------------
    # Get existing station_ids
    # -------------------------------
    cursor.execute("SELECT station_id FROM dim_station")
    existing_stations = {row[0] for row in cursor.fetchall()}

    # -------------------------------
    # Insert missing stations
    # -------------------------------
    for _, r in df.iterrows():
        station_id = r['station_id']
        if not station_id:
            continue
        if station_id not in existing_stations:
            cursor.execute(
                "INSERT INTO dim_station (station_id, station_name) VALUES (%s, %s)",
                (station_id, f"Station {station_id}")
            )
            existing_stations.add(station_id)

    conn.commit()
    print(f"dim_station table updated with {len(existing_stations)} stations")

    # -------------------------------
    # Prepare rows for fact_weather
    # -------------------------------
    rows = []
    for _, r in df.iterrows():
        if not r['station_id'] or r['temperature_c'] is None:
            continue

        rows.append((
            r['station_id'],       # station_id
            None,                  # time_id
            1,                     # source_id = NOAA
            float(r['temperature_c']),
            None,                  # dew_point_c
            None,                  # wind_speed_kmh
            None,                  # precipitation_mm
            None,                  # quality_flag
            int(r['year']),
            int(r['month']),
            int(r['day']),
            int(r['hour'])
        ))

    # -------------------------------
    # Insert into fact_weather
    # -------------------------------
    insert_sql = """
    INSERT INTO fact_weather
    (station_id, time_id, source_id, temperature_c, dew_point_c, wind_speed_kmh, precipitation_mm, quality_flag, year, month, day, hour)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_sql, rows)
    conn.commit()
    print(f"Inserted {cursor.rowcount} rows into fact_weather")

except mysql.connector.Error as err:
    print("MySQL error:", err)
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals() and conn.is_connected():
        conn.close()
