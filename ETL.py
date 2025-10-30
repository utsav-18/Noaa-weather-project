
import pandas as pd
from pathlib import Path

# ---------- CONFIG ----------
FWF_PATH = Path("raw/bangalore_2024_weather.txt")   # input fixed-width file
OUT_ETL = Path("etl/sample_transformed.csv")
OUT_EXPORT = Path("exports/sample_transformed_for_tableau.csv")

# fixed-width column specs (adjust if your file differs)
colspecs = [
    (0, 6),    # USAF station ID
    (7, 12),   # WBAN station ID
    (12, 16),  # year
    (16, 18),  # month
    (18, 20),  # day
    (20, 22),  # hour
    (87, 92)   # temperature (tenths of °C) -- common in NOAA .dly style
]
names = ['usaf', 'wban', 'year', 'month', 'day', 'hour', 'temperature_raw']

# ---------- Helpers ----------
def clean_part(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if s.lower() in ("", "nan", "none"):
        return ""
    return s

def pad_numeric(x, width):
    try:
        return str(int(float(x))).zfill(width)
    except Exception:
        # if it's empty or cannot convert, return empty string
        return ""

# ---------- Read file ----------
if not FWF_PATH.exists():
    raise SystemExit(f"Input file not found: {FWF_PATH}")

# read fixed width formatted file
df = pd.read_fwf(FWF_PATH, colspecs=colspecs, names=names, dtype=str, na_values=["", "NA", "NaN"])

# clean station id parts and build station_id
df['usaf'] = df['usaf'].apply(clean_part)
df['wban'] = df['wban'].apply(clean_part)

df['usaf_z'] = df['usaf'].apply(lambda x: pad_numeric(x, 6))
df['wban_z'] = df['wban'].apply(lambda x: pad_numeric(x, 5))

# prefer full composite id if wban present, else usaf alone
df['station_id'] = df.apply(lambda r: (r['usaf_z'] + r['wban_z']) if r['wban_z'] else r['usaf_z'], axis=1)
df['station_id'] = df['station_id'].replace("", pd.NA)

# ---------- Parse numeric/date fields ----------
for c in ['year', 'month', 'day', 'hour']:
    df[c] = pd.to_numeric(df[c], errors='coerce')

# temperature: convert to numeric (tenths of °C -> °C)
df['temperature_raw'] = pd.to_numeric(df['temperature_raw'], errors='coerce')

# NOAA often uses 9999 or 99999 placeholders for missing temps; treat large abs as missing
df.loc[df['temperature_raw'].abs() >= 9999, 'temperature_raw'] = pd.NA
df['temperature_c'] = df['temperature_raw'] / 10.0

# ---------- Build timestamp and filter valid rows ----------
# require valid year/month/day/hour to create timestamp
mask_valid = (
    df['year'].notna() & (df['year'] > 0) &
    df['month'].between(1, 12) &
    df['day'].between(1, 31) &
    df['hour'].between(0, 23) &
    df['station_id'].notna()
)

df = df.loc[mask_valid].copy()

# create ts column
df['ts'] = pd.to_datetime(
    dict(year=df['year'].astype(int),
         month=df['month'].astype(int),
         day=df['day'].astype(int),
         hour=df['hour'].astype(int)),
    errors='coerce'
)

# drop rows where timestamp could not be built
df = df[df['ts'].notna()].copy()

# ---------- Final selection & formatting ----------
out = df[['station_id', 'ts', 'year', 'month', 'day', 'hour', 'temperature_c']].copy()
# format ts as ISO string for CSV compatibility (Postgres will parse this)
out['ts'] = out['ts'].dt.strftime('%Y-%m-%d %H:%M:%S')

# make directories
OUT_ETL.parent.mkdir(exist_ok=True)
OUT_EXPORT.parent.mkdir(exist_ok=True)

# write CSVs
out.to_csv(OUT_ETL, index=False)
out.to_csv(OUT_EXPORT, index=False)

# summary prints
print("✅ ETL complete")
print("Rows written:", len(out))
print("Unique stations:", out['station_id'].nunique())
