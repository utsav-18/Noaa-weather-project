import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        dbname="weather_dw",
        user="postgres",
        password="Mutton@01"  # change this to your actual password
    )
    print("✅ Connected successfully!")
except Exception as e:
    print("❌ Connection failed:", e)
finally:
    if 'conn' in locals():
        conn.close()
