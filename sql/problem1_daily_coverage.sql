-- Problem 1: How well do RWIS, WZDx, and Incident/Events line up by date? 
-- Daily coverage across all three curated sources. 

WITH daily_rwis AS (
    SELECT
        dt,
        count(DISTINCT station_id) AS rwis_stations,
        sum(obs_count) AS rwis_obs
    FROM tlms_curated_db.rwis_hourly_summary
    GROUP BY dt
),
daily_wzdx AS (
    SELECT
        dt,
        sum(work_zone_count) AS work_zones
    FROM tlms_curated_db.wzdx_daily_summary
    GROUP BY dt
),
daily_incidents AS (
    SELECT
        dt,
        sum(total_incidents) AS incidents,
        sum(alert_incidents) AS alert_incidents
    FROM tlms_curated_db.inc_daily_summary
    GROUP BY dt
)
SELECT
    r.dt,
    r.rwis_stations,
    r.rwis_obs,
    w.work_zones,
    i.incidents,
    i.alert_incidents
FROM daily_rwis r
JOIN daily_wzdx w ON w.dt = r.dt
JOIN daily_incidents i ON i.dt = r.dt
ORDER BY r.dt DESC
LIMIT 365
