import pandas as pd

# -------------------------------
# Raw NOAA file path
# -------------------------------
file_path = "raw/bangalore_2024_weather.txt"

# -------------------------------
# Fixed-width column specs
# -------------------------------
colspecs = [
    (0, 6),    # USAF station ID
    (7, 12),   # WBAN station ID
    (12, 16),  # year
    (16, 18),  # month
    (18, 20),  # day
    (20, 22),  # hour
    (87, 92)   # temperature (tenths of °C)
]

names = ['usaf', 'wban', 'year', 'month', 'day', 'hour', 'temperature']

# -------------------------------
# Read the fixed-width file
# -------------------------------
df = pd.read_fwf(file_path, colspecs=colspecs, names=names)

# -------------------------------
# Create station_id as string
# -------------------------------
df['station_id'] = df['usaf'].astype(str).str.strip() + df['wban'].astype(str).str.strip()

# -------------------------------
# Convert temperature to °C
# -------------------------------
df['temperature_c'] = df['temperature'] / 10

# -------------------------------
# Clean year, month, day, hour
# Keep only numeric, fill missing or invalid with 0
# -------------------------------
for col in ['year', 'month', 'day', 'hour']:
    df[col] = pd.to_numeric(df[col], errors='coerce')  # non-numeric → NaN
    df[col] = df[col].fillna(0).astype(int)

# -------------------------------
# Keep only required columns
# -------------------------------
df = df[['station_id', 'year', 'month', 'day', 'hour', 'temperature_c']]

# -------------------------------
# Save cleaned CSV
# -------------------------------
df.to_csv("etl/sample_transformed.csv", index=False)
print("Cleaned CSV saved: etl/sample_transformed.csv (all non-numeric year/month/day/hour converted to 0)")
