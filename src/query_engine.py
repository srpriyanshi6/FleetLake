
import os

import duckdb

BASE = os.path.join(os.path.dirname(__file__), "..", "data_lake")
CURATED_DIR = os.path.join(BASE, "curated")
STAGING_DIR = os.path.join(BASE, "staging")

CURATED_TABLES = ["daily_fleet_utilization", "driver_performance", "delivery_sla_compliance"]
STAGING_TABLES = ["vehicle_telemetry", "driver_shift_logs", "delivery_completions"]


def connect_catalog(read_only_staging=True):
    """Return a DuckDB connection with every curated (and staging) table
    registered as a hive-partitioned view, mirroring Glue Data Catalog
    tables pointed at S3."""
    con = duckdb.connect()
    for name in CURATED_TABLES:
        glob_path = os.path.join(CURATED_DIR, name, "dt=*", "*.parquet").replace("\\", "/")
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{glob_path}', hive_partitioning=1)")
    if read_only_staging:
        for name in STAGING_TABLES:
            glob_path = os.path.join(STAGING_DIR, name, "dt=*", "*.parquet").replace("\\", "/")
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{glob_path}', hive_partitioning=1)")
    return con


QUERIES = {
    "Q1_top_underutilized_vehicles": """
        -- Vehicles with the lowest average active-time % over the whole window
        -- (candidates for fleet rightsizing)
        SELECT vehicle_id,
               ROUND(AVG(active_time_pct), 1) AS avg_active_pct,
               ROUND(AVG(distance_km), 1)      AS avg_daily_km
        FROM daily_fleet_utilization
        GROUP BY vehicle_id
        ORDER BY avg_active_pct ASC
        LIMIT 5
    """,
    "Q2_driver_leaderboard_last_7_days": """
        -- Top 5 drivers by on-time delivery rate over the most recent 7 days on file
        SELECT driver_id,
               SUM(deliveries_completed)                                  AS deliveries_completed,
               ROUND(SUM(on_time_deliveries) * 100.0 / NULLIF(SUM(deliveries_completed), 0), 1)
                                                                           AS on_time_rate_pct
        FROM driver_performance
        WHERE dt >= (SELECT CAST(MAX(dt) AS DATE) - INTERVAL 6 DAY FROM driver_performance)
        GROUP BY driver_id
        HAVING SUM(deliveries_completed) >= 5
        ORDER BY on_time_rate_pct DESC
        LIMIT 5
    """,
    "Q3_city_sla_trend": """
        -- Day-by-day SLA compliance % per city (trend line for a dashboard)
        SELECT dt, city, sla_compliance_pct, total_deliveries
        FROM delivery_sla_compliance
        ORDER BY city, dt
    """,
    "Q4_worst_sla_city_days": """
        -- Single worst (city, day) combinations for SLA breaches - ops triage list
        SELECT dt, city, breached_deliveries, avg_breach_overage_minutes, sla_compliance_pct
        FROM delivery_sla_compliance
        WHERE breached_deliveries > 0
        ORDER BY breached_deliveries DESC
        LIMIT 10
    """,
    "Q5_fleet_utilization_vs_driver_hours": """
        -- Joins two curated tables: does higher fleet active-time correlate
        -- with more driver hours logged that day, per vehicle-day?
        SELECT f.dt,
               f.vehicle_id,
               f.active_time_pct,
               ROUND(AVG(p.hours_worked), 2) AS avg_driver_hours_same_day
        FROM daily_fleet_utilization f
        LEFT JOIN driver_performance p ON f.dt = p.dt
        GROUP BY f.dt, f.vehicle_id, f.active_time_pct
        ORDER BY f.dt, f.vehicle_id
        LIMIT 20
    """,
}


def main():
    con = connect_catalog()
    for name, sql in QUERIES.items():
        print(f"\n=== {name} " + "=" * (60 - len(name)))
        print(sql.strip())
        print("-" * 60)
        df = con.execute(sql).fetchdf()
        print(df.to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
