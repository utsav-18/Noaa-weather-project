#!/usr/bin/env python3
"""
Robust ETL with staging + aggregate + upsert, and auto-insert of missing stations.

Behavior:
 - COPY into a TEMP staging table per chunk
 - INSERT any missing station_id into dim_station with NULL metadata
 - INSERT aggregated rows (one per station/year/month) using GROUP BY
 - ON CONFLICT DO UPDATE (replace temp_c)
 - Commits per chunk
"""

import os
import csv
import re
import tempfile
import psycopg2
from tqdm import tqdm

# ---------- CONFIG ----------
CONFIG = {
    "dat_path": "ghcnm.tavg.v4.0.1.20251028.qfe.dat",
    "inv_path": "ghcnm.tavg.v4.0.1.20251028.qfe.inv",
    "pg": {
        "host": os.environ.get("PG_HOST", "localhost"),
        "port": int(os.environ.get("PG_PORT", 5432)),
        "dbname": os.environ.get("PG_DB", "weather_dw"),
        "user": os.environ.get("PG_USER", "ghcn_user"),
        "password": os.environ.get("PG_PASS", "StrongPass123")
    },
    # chunk size (rows) before doing COPY+UPSERT; lower if you have less RAM
    "batch_size": int(os.environ.get("BATCH_SIZE", 300000)),
    # whether to load only India (set to None to load all)
    "FILTER_COUNTRY": None
}
# ---------- end config ----------

def pg_conn():
    c = CONFIG["pg"]
    conn = psycopg2.connect(
        host=c["host"],
        port=c["port"],
        dbname=c["dbname"],
        user=c["user"],
        password=c["password"]
    )
    # keep autocommit off so TEMP tables persist for session
    return conn

# ---- parsers (robust) ----
def parse_inv_line(line):
    parts = line.strip().split()
    if len(parts) < 6:
        return None
    station_id = parts[0]
    try:
        lat = float(parts[1]); lon = float(parts[2]); elev = float(parts[3])
    except:
        lat = lon = elev = None
    country = parts[4]
    name = " ".join(parts[5:])
    return station_id, lat, lon, elev, country, name

def parse_dat_line(line):
    station = line[:11].strip()
    if not station:
        m = re.match(r'(\S+)', line)
        station = m.group(1) if m else None
    year_match = re.search(r'\b(17[0-9]{2}|18[0-9]{2}|19[0-9]{2}|20[0-2][0-9]|2025)\b', line)
    if not year_match:
        return None
    year = int(year_match.group(0))
    if year < 1700 or year > 2025:
        return None
    tail = line[year_match.end():]
    nums = re.findall(r'(-?\d+)', tail)
    months = []
    for i in range(12):
        if i < len(nums):
            try:
                months.append(int(nums[i]))
            except:
                months.append(None)
        else:
            months.append(None)
    return station, year, months

def is_missing_value(v):
    return v is None or v == -9999

# -------- load stations (from .inv) ----------
def load_stations(inv_path):
    conn = pg_conn()
    cur = conn.cursor()
    upsert_sql = """
    INSERT INTO dim_station (station_id, latitude, longitude, elevation, country_code, name)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (station_id) DO UPDATE
      SET latitude = EXCLUDED.latitude,
          longitude = EXCLUDED.longitude,
          elevation = EXCLUDED.elevation,
          country_code = EXCLUDED.country_code,
          name = EXCLUDED.name;
    """
    print("Loading stations into dim_station...")
    inserted = 0
    with open(inv_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in tqdm(f):
            parsed = parse_inv_line(line)
            if not parsed:
                continue
            station_id, lat, lon, elev, country, name = parsed
            if CONFIG["FILTER_COUNTRY"] and country != CONFIG["FILTER_COUNTRY"]:
                continue
            cur.execute(upsert_sql, (station_id, lat, lon, elev, country, name))
            inserted += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"Stations loaded: {inserted}")

# -------- streaming with staging, ensure stations exist, aggregate & upsert ----------
def stream_dat_to_postgres_with_upsert(dat_path, batch_size):
    conn = pg_conn()
    cur = conn.cursor()
    header = ["station_id","year","month","temp_c"]

    # build allowed stations set for filter (if used)
    allowed_stations = None
    if CONFIG["FILTER_COUNTRY"]:
        allowed_stations = set()
        c2 = pg_conn(); cur2 = c2.cursor()
        cur2.execute("SELECT station_id FROM dim_station;")
        for r in cur2.fetchall(): allowed_stations.add(r[0])
        cur2.close(); c2.close()

    tmp_rows = 0
    csv_fh = None
    tmp_csv_path = None

    def start_new_csv():
        nonlocal csv_fh, tmp_csv_path
        fd, path = tempfile.mkstemp(prefix="ghcn_chunk_", suffix=".csv", text=True)
        os.close(fd)
        csv_fh = open(path, "w", newline='', encoding='utf-8')
        writer = csv.writer(csv_fh)
        writer.writerow(header)
        tmp_csv_path = path
        return writer

    import os
    writer = start_new_csv()
    total_rows = 0
    batch_no = 0
    print("Streaming .dat -> staging CSV chunks and performing COPY+AGGREGATE+UPSERT per chunk...")

    with open(dat_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in tqdm(f):
            parsed = parse_dat_line(line)
            if not parsed:
                continue
            station, year, months = parsed
            if allowed_stations is not None and station not in allowed_stations:
                continue
            for i, mv in enumerate(months, start=1):
                if is_missing_value(mv):
                    temp_c = ''
                else:
                    temp_c = float(mv)/100.0
                writer.writerow([station, year, i, temp_c])
                tmp_rows += 1
                total_rows += 1

                if tmp_rows >= batch_size:
                    csv_path = tmp_csv_path
                    csv_fh.flush(); csv_fh.close()
                    batch_no += 1
                    print(f"Processing chunk #{batch_no}, rows={tmp_rows} ...")
                    _copy_ensure_stations_aggregate_and_upsert(cur, conn, csv_path)
                    os.remove(csv_path)
                    writer = start_new_csv()
                    tmp_rows = 0

    # final chunk
    if tmp_rows > 0:
        csv_path = tmp_csv_path
        csv_fh.flush(); csv_fh.close()
        batch_no += 1
        print(f"Processing final chunk #{batch_no}, rows={tmp_rows} ...")
        _copy_ensure_stations_aggregate_and_upsert(cur, conn, csv_path)
        os.remove(csv_path)

    cur.close()
    conn.close()
    print(f"Total rows read: {total_rows}")

# helper: create staging table, copy into it, insert missing stations, aggregate to dedupe, then upsert into fact table
def _copy_ensure_stations_aggregate_and_upsert(cur, conn, csv_path):
    """
    Steps:
     - CREATE TEMP TABLE ghcn_temp_stage ...
     - COPY CSV into temp table
     - INSERT ANY MISSING station_ids INTO dim_station (NULL metadata) FROM the temp table
     - INSERT aggregated rows into fact_temperature with ON CONFLICT DO UPDATE
     - commit
    """
    cur.execute("""
    CREATE TEMP TABLE ghcn_temp_stage (
      station_id TEXT,
      year INT,
      month SMALLINT,
      temp_c REAL
    ) ON COMMIT DROP;
    """)
    # COPY into temp
    with open(csv_path, "r", encoding="utf-8") as copyfile:
        next(copyfile)
        cur.copy_expert("COPY ghcn_temp_stage (station_id, year, month, temp_c) FROM STDIN WITH CSV", copyfile)

    # insert missing stations (keep other metadata NULL)
    cur.execute("""
    INSERT INTO dim_station (station_id)
    SELECT DISTINCT station_id
    FROM ghcn_temp_stage s
    WHERE NOT EXISTS (SELECT 1 FROM dim_station d WHERE d.station_id = s.station_id);
    """)
    # now aggregate + upsert into fact_temperature
    cur.execute("""
    INSERT INTO fact_temperature (station_id, year, month, temp_c)
    SELECT station_id, year, month, agg_temp FROM (
      SELECT station_id, year, month, MAX(temp_c) AS agg_temp
      FROM ghcn_temp_stage
      GROUP BY station_id, year, month
    ) AS deduped
    ON CONFLICT (station_id, year, month) DO UPDATE
      SET temp_c = EXCLUDED.temp_c;
    """)
    conn.commit()

# -------- main ----------
if __name__ == "__main__":
    print("Starting GHCN monthly ETL -> Postgres (staging+agg+upsert + ensure stations)")
    load_stations(CONFIG["inv_path"])
    stream_dat_to_postgres_with_upsert(CONFIG["dat_path"], CONFIG["batch_size"])
    print("ETL finished.")
