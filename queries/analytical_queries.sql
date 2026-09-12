-- FleetLake analytical queries — run in Amazon Athena against the
-- fleetlake_curated database (see aws/athena_ddl.sql for table DDL)
--
-- Locally, the identical logic runs against DuckDB views in src/query_engine.py

-- Q1: Most underutilized vehicles (candidates for fleet rightsizing)
SELECT vehicle_id,
       ROUND(AVG(active_time_pct), 1) AS avg_active_pct,
       ROUND(AVG(distance_km), 1)     AS avg_daily_km
FROM fleetlake_curated.daily_fleet_utilization
GROUP BY vehicle_id
ORDER BY avg_active_pct ASC
LIMIT 5;


-- Q2: Driver leaderboard for the last 7 days on file, by on-time rate
SELECT driver_id,
       SUM(deliveries_completed) AS deliveries_completed,
       ROUND(SUM(on_time_deliveries) * 100.0 / NULLIF(SUM(deliveries_completed), 0), 1)
                                  AS on_time_rate_pct
FROM fleetlake_curated.driver_performance
WHERE dt >= date_add('day', -6, (SELECT MAX(dt) FROM fleetlake_curated.driver_performance))
GROUP BY driver_id
HAVING SUM(deliveries_completed) >= 5
ORDER BY on_time_rate_pct DESC
LIMIT 5;


-- Q3: Day-by-day SLA compliance % per city (dashboard trend line)
-- Note: filtering on dt here only scans the requested partitions.
SELECT dt, city, sla_compliance_pct, total_deliveries
FROM fleetlake_curated.delivery_sla_compliance
WHERE dt BETWEEN DATE '2026-08-25' AND DATE '2026-09-07'
ORDER BY city, dt;


-- Q4: Worst (city, day) combinations for SLA breaches — ops triage list
SELECT dt, city, breached_deliveries, avg_breach_overage_minutes, sla_compliance_pct
FROM fleetlake_curated.delivery_sla_compliance
WHERE breached_deliveries > 0
ORDER BY breached_deliveries DESC
LIMIT 10;


-- Q5: Fleet utilization vs. driver hours worked, same day (cross-table join)
SELECT f.dt,
       f.vehicle_id,
       f.active_time_pct,
       ROUND(AVG(p.hours_worked), 2) AS avg_driver_hours_same_day
FROM fleetlake_curated.daily_fleet_utilization f
LEFT JOIN fleetlake_curated.driver_performance p ON f.dt = p.dt
GROUP BY f.dt, f.vehicle_id, f.active_time_pct
ORDER BY f.dt, f.vehicle_id
LIMIT 20;


--the "before" query from the cost comparison 
-- what would have had to write (and pay full-scan price for) without a curated, partitioned layer. 
--Compare its data-scanned figure in the
-- Athena console against Q3/Q4 above.

SELECT city,
       COUNT(*) AS total_deliveries,
       ROUND(AVG(CASE WHEN status = 'COMPLETED'
                       AND date_diff('minute', assigned_at, completed_at) <= promised_minutes
                  THEN 100.0 ELSE 0.0 END), 1) AS sla_compliance_pct
FROM fleetlake_raw.delivery_completions   -- raw JSON, not partitioned by city, every file scanned
WHERE dt = '2026-09-01' AND city = 'Delhi'
GROUP BY city;
