-- Problem 3: Incident performance by county. 
-- Aggregate incident activity by county and direction

SELECT
    dt,
    county,
    sum(total_incidents) AS total_incidents,
    sum(open_incidents) AS open_incidents,
    sum(closed_incidents) AS closed_incidents,
    sum(alert_incidents) AS alert_incidents,
    round(100.0 * sum(open_incidents) / nullif(sum(total_incidents), 0), 1) AS pct_open_incidents
FROM tlms_curated_db.inc_daily_summary
GROUP BY dt, county
HAVING sum(total_incidents) > 0
ORDER BY dt DESC, total_incidents DESC
LIMIT 500