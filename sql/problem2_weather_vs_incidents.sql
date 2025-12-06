-- Problem 2: How do weather conditions relate to incidents?

WITH rwis_daily AS (
    select
        dt,
        sum(cnt_icy) AS icy_obs,
        sum(cnt_wet) AS wet_obs,
        sum(cnt_dry) AS dry_obs,
        avg(avg_air_temp_f) AS avg_air_temp_f
    FROM tlms_curated_db.rwis_hourly_summary
    GROUP BY dt
),
inc_daily AS (
    SELECT
        dt, 
        sum(total_incidents) AS total_incidents,
        sum(open_incidents) AS open_incidents,
        sum(closed_incidents) AS closed_incidents,
        sum(alert_incidents) AS alert_incidents
    FROM tlms_curated_db.inc_daily_summary
    GROUP BY dt
)
SELECT 
    i.dt,
    i.total_incidents, 
    i.alert_incidents,
    i.open_incidents,
    r.icy_obs,
    r.wet_obs,
    r.dry_obs,
    r.avg_air_temp_f
FROM inc_daily i
JOIN rwis_daily r ON i.dt = r.dt
ORDER BY i.dt DESC
LIMIT 60