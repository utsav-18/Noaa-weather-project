import os
import psycopg2
import pandas as pd

# === Database connection info ===
DB_CONFIG = {
    "host": "localhost",
    "dbname": "weatherdb",
    "user": "postgres",
    "password": "Mutton@01"
}

EXPORT_DIR = "export"
os.makedirs(EXPORT_DIR, exist_ok=True)

# === Helper function ===
def export_query(name, query):
    """Run query and export to CSV"""
    print(f"▶ Running: {name} ...", end=" ")
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql_query(query, conn)
    filepath = os.path.join(EXPORT_DIR, f"{name}.csv")
    df.to_csv(filepath, index=False)
    print(f"✅ {len(df)} rows → {filepath}")

# === Queries ===
QUERIES = {
    "01_monthly_view": """
        SELECT * FROM mv_monthly_station ORDER BY station_id, year, month;
    """,
    "02_seasonal_patterns": """
        SELECT station_id, station_name, month,
               AVG(avg_tavg) AS climatology_tavg,
               STDDEV(avg_tavg) AS climatology_sd,
               AVG(total_prcp) AS climatology_prcp
        FROM mv_monthly_station
        GROUP BY station_id, station_name, month
        ORDER BY station_id, month;
    """,
    "03_monthly_anomalies": """
        WITH stats AS (
            SELECT station_id, month,
                   AVG(avg_tavg) AS mu,
                   NULLIF(STDDEV(avg_tavg),0) AS sigma
            FROM mv_monthly_station
            GROUP BY station_id, month
        )
        SELECT m.station_id, m.station_name, m.year, m.month, m.month_date, m.avg_tavg,
               (m.avg_tavg - s.mu) / s.sigma AS z_monthly_tavg
        FROM mv_monthly_station m
        JOIN stats s ON m.station_id = s.station_id AND m.month = s.month
        ORDER BY m.station_id, m.month_date;
    """,
    "04_daily_anomalies": """
        WITH stats AS (
            SELECT station_id, AVG(tavg) AS mu, NULLIF(STDDEV(tavg),0) AS sigma
            FROM fact_weather
            GROUP BY station_id
        )
        SELECT f.station_id, d.date, f.tavg,
               (f.tavg - s.mu) / s.sigma AS z_daily_tavg
        FROM fact_weather f
        JOIN dim_time d ON f.time_id = d.time_id
        JOIN stats s ON f.station_id = s.station_id
        ORDER BY f.station_id, d.date;
    """,
    "05_iqr_outliers": """
        WITH per_station_month AS (
            SELECT f.station_id, t.month, f.tavg
            FROM fact_weather f
            JOIN dim_time t ON f.time_id = t.time_id
        ),
        quant AS (
            SELECT station_id, month,
                   percentile_cont(0.25) WITHIN GROUP (ORDER BY tavg) AS q1,
                   percentile_cont(0.75) WITHIN GROUP (ORDER BY tavg) AS q3
            FROM per_station_month
            GROUP BY station_id, month
        )
        SELECT f.station_id, t.date, f.tavg,
               CASE WHEN f.tavg < (q1 - 1.5*(q3-q1)) OR f.tavg > (q3 + 1.5*(q3-q1))
                    THEN true ELSE false END AS is_iqr_outlier
        FROM fact_weather f
        JOIN dim_time t ON f.time_id = t.time_id
        JOIN quant q ON q.station_id = f.station_id AND q.month = t.month
        LIMIT 500;
    """,
    "06_rolling_avg": """
        SELECT station_id, month_date,
               AVG(avg_tavg) OVER (PARTITION BY station_id ORDER BY month_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3m_tavg
        FROM mv_monthly_station
        ORDER BY station_id, month_date;
    """,
    "07_trend_slopes": """
        SELECT station_id, station_name,
               regr_slope(annual_avg_tavg, year) AS trend_slope_per_year,
               regr_intercept(annual_avg_tavg, year) AS intercept,
               corr(annual_avg_tavg, year) AS corr_year
        FROM mv_annual_station
        GROUP BY station_id, station_name
        ORDER BY trend_slope_per_year DESC;
    """,
    "08_anomaly_summary": """
        WITH z AS (
            SELECT m.station_id, m.station_name, m.year, m.month,
                   (m.avg_tavg - s.mu) / s.sigma AS z_monthly_tavg
            FROM mv_monthly_station m
            JOIN (
                SELECT station_id, month, AVG(avg_tavg) AS mu, NULLIF(STDDEV(avg_tavg),0) AS sigma
                FROM mv_monthly_station
                GROUP BY station_id, month
            ) s ON m.station_id = s.station_id AND m.month = s.month
        )
        SELECT station_id, year,
               COUNT(*) FILTER (WHERE ABS(z_monthly_tavg) > 2.5) AS num_anomalous_months
        FROM z
        GROUP BY station_id, year
        ORDER BY station_id, year;
    """
}

# === Run all exports ===
if __name__ == "__main__":
    print("🚀 Starting full analytics export...\n")
    for name, query in QUERIES.items():
        try:
            export_query(name, query)
        except Exception as e:
            print(f"❌ {name} failed: {e}")
    print("\n🎉 All exports completed successfully!")
