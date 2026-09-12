# Silver -> Gold transform
#uses DuckDB


import os

import duckdb
import pandas as pd

BASE = os.path.join(os.path.dirname(__file__), "..", "data_lake")
STAGING_DIR = os.path.join(BASE, "staging")
CURATED_DIR = os.path.join(BASE, "curated")


def _glob(dataset):
    return os.path.join(STAGING_DIR, dataset, "dt=*", "*.parquet").replace("\\", "/")


def main():
    con = duckdb.connect()

    con.execute(f"""
        CREATE VIEW telemetry AS
        SELECT * FROM read_parquet('{_glob("vehicle_telemetry")}', hive_partitioning=1)
    """)
    con.execute(f"""
        CREATE VIEW shifts AS
        SELECT * FROM read_parquet('{_glob("driver_shift_logs")}', hive_partitioning=1)
    """)
    con.execute(f"""
        CREATE VIEW deliveries AS
        SELECT * FROM read_parquet('{_glob("delivery_completions")}', hive_partitioning=1)
    """)

    # 1. daily_fleet_utilization
    fleet_util = con.execute("""
        SELECT
            dt,
            vehicle_id,
            COUNT(*)                                        AS ping_count,
            ROUND(AVG(speed_kmph), 2)                        AS avg_speed_kmph,
            ROUND(MAX(odometer_km) - MIN(odometer_km), 2)    AS distance_km,
            ROUND(SUM(CASE WHEN engine_on THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
                                                              AS active_time_pct,
            ROUND((COUNT(*) - SUM(CASE WHEN engine_on THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 1)
                                                              AS idle_time_pct
        FROM telemetry
        GROUP BY dt, vehicle_id
        ORDER BY dt, vehicle_id
    """).fetchdf()

    # 2. driver_performance
    driver_perf = con.execute("""
        WITH shift_hours AS (
            SELECT
                dt,
                driver_id,
                ROUND(EPOCH(clock_out - clock_in) / 3600.0
                      - COALESCE(break_minutes, 0) / 60.0, 2) AS hours_worked
            FROM shifts
            WHERE clock_out IS NOT NULL
        ),
        delivery_stats AS (
            SELECT
                dt,
                driver_id,
                COUNT(*)                                                       AS deliveries_assigned,
                SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END)          AS deliveries_completed,
                SUM(CASE
                        WHEN status = 'COMPLETED'
                         AND EPOCH(completed_at - assigned_at) / 60.0 <= promised_minutes
                        THEN 1 ELSE 0 END)                                     AS on_time_deliveries,
                ROUND(AVG(CASE WHEN status = 'COMPLETED'
                                THEN EPOCH(completed_at - assigned_at) / 60.0 END), 2)
                                                                                AS avg_delivery_minutes
            FROM deliveries
            GROUP BY dt, driver_id
        )
        SELECT
            COALESCE(d.dt, s.dt)               AS dt,
            COALESCE(d.driver_id, s.driver_id) AS driver_id,
            s.hours_worked,
            COALESCE(d.deliveries_assigned, 0)   AS deliveries_assigned,
            COALESCE(d.deliveries_completed, 0)  AS deliveries_completed,
            COALESCE(d.on_time_deliveries, 0)    AS on_time_deliveries,
            CASE WHEN d.deliveries_completed > 0
                 THEN ROUND(d.on_time_deliveries * 100.0 / d.deliveries_completed, 1)
                 ELSE NULL END                    AS on_time_rate_pct,
            d.avg_delivery_minutes
        FROM delivery_stats d
        FULL OUTER JOIN shift_hours s
          ON d.dt = s.dt AND d.driver_id = s.driver_id
        ORDER BY dt, driver_id
    """).fetchdf()

    # 3. delivery_sla_compliance
    sla = con.execute("""
        SELECT
            dt,
            city,
            COUNT(*)                                                        AS total_deliveries,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)              AS failed_deliveries,
            SUM(CASE
                    WHEN status = 'COMPLETED'
                     AND EPOCH(completed_at - assigned_at) / 60.0 <= promised_minutes
                    THEN 1 ELSE 0 END)                                      AS on_time_deliveries,
            SUM(CASE
                    WHEN status = 'COMPLETED'
                     AND EPOCH(completed_at - assigned_at) / 60.0 > promised_minutes
                    THEN 1 ELSE 0 END)                                      AS breached_deliveries,
            ROUND(AVG(CASE
                    WHEN status = 'COMPLETED'
                     AND EPOCH(completed_at - assigned_at) / 60.0 > promised_minutes
                    THEN EPOCH(completed_at - assigned_at) / 60.0 - promised_minutes
                END), 2)                                                    AS avg_breach_overage_minutes,
            ROUND(SUM(CASE
                    WHEN status = 'COMPLETED'
                     AND EPOCH(completed_at - assigned_at) / 60.0 <= promised_minutes
                    THEN 1 ELSE 0 END) * 100.0
                  / NULLIF(SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END), 0), 1)
                                                                             AS sla_compliance_pct
        FROM deliveries
        GROUP BY dt, city
        ORDER BY dt, city
    """).fetchdf()

    tables = {
        "daily_fleet_utilization": fleet_util,
        "driver_performance": driver_perf,
        "delivery_sla_compliance": sla,
    }

    for name, df in tables.items():
        for dt_val, group in df.groupby("dt"):
            dt_str = pd.Timestamp(dt_val).strftime("%Y-%m-%d")
            out_dir = os.path.join(CURATED_DIR, name, f"dt={dt_str}")
            os.makedirs(out_dir, exist_ok=True)
            group.drop(columns=["dt"]).to_parquet(
                os.path.join(out_dir, "part-0000.parquet"),
                engine="pyarrow", index=False, compression="snappy",
            )
        print(f"  {name:24s} rows={len(df):5d}  partitions={df['dt'].nunique()}")

    print("\nCurated zone populated at:", os.path.abspath(CURATED_DIR))
    con.close()


if __name__ == "__main__":
    print("Silver -> Gold: building aggregated curated tables\n")
    main()
