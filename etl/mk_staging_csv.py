# etl/mk_staging_csv.py
import pandas as pd
from pathlib import Path


INPUT = Path("etl/noaa_weather_new.csv")  # or Path("raw/noaa_weather_new.csv")
OUTPUT = Path("etl/staging_weather_ready.csv")

print("Reading:", INPUT)
if not INPUT.exists():
    raise SystemExit(f"Input file not found: {INPUT}")

df = pd.read_csv(INPUT, low_memory=False)

# Standardize column names (case-insensitive mapping)
cols = {c.upper(): c for c in df.columns}
def col(name):
    return cols.get(name.upper())

# Common mappings (adapt if your file uses other names)
mappings = {
    'station_id': col('STATION') or col('station') or col('STATION_ID') or 'STATION',
    'name'      : col('NAME') or 'NAME',
    'latitude'  : col('LATITUDE') or 'LAT' or 'LATITUDE',
    'longitude' : col('LONGITUDE') or 'LON' or 'LONITUDE' or 'LONG',
    'elevation' : col('ELEVATION') or 'ELEV',
    'date'      : col('DATE') or col('ts') or 'DATE',
    'tavg'      : col('TAVG') or 'TAVG',
    'tmax'      : col('TMAX') or 'TMAX',
    'tmin'      : col('TMIN') or 'TMIN',
    'prcp'      : col('PRCP') or 'PRCP'
}

# Build a new DataFrame with desired columns (if a source column missing, fill NaN)
out = pd.DataFrame()
for k, src in mappings.items():
    if isinstance(src, str) and src in df.columns:
        out[k] = df[src]
    else:
        out[k] = pd.NA

# Parse and normalize types
out['date'] = pd.to_datetime(out['date'], errors='coerce')          # pandas Timestamp
out['ts'] = out['date']                                              # keep ts column name for DB
for numcol in ['latitude','longitude','elevation','tavg','tmax','tmin','prcp']:
    out[numcol] = pd.to_numeric(out[numcol], errors='coerce')

# Rename final columns to exactly match staging table
final = out.rename(columns={
    'station_id': 'station_id',
    'name': 'name',
    'latitude': 'latitude',
    'longitude': 'longitude',
    'elevation': 'elevation',
    'ts': 'ts',
    'tavg': 'tavg',
    'tmax': 'tmax',
    'tmin': 'tmin',
    'prcp': 'prcp'
})[
    ['station_id','name','latitude','longitude','elevation','ts','tavg','tmax','tmin','prcp']
]

# drop rows missing station_id or ts, because staging expects those
before = len(final)
final = final[final['station_id'].notna() & final['ts'].notna()].copy()
after = len(final)

OUTPUT.parent.mkdir(exist_ok=True)
final.to_csv(OUTPUT, index=False)
print(f"Wrote {OUTPUT} ({after} rows; dropped {before-after} rows missing id or ts)")
