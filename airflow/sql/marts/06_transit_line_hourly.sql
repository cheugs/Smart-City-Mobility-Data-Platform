-- Public transport performance per LINE and per hour (last 7 days), from the raw events.
--   on time   = between 1 minute early and 3 minutes late (same rule as Spark)
--   p90 delay = 90 % of the stops were served with less delay than this
DROP TABLE IF EXISTS analytics.transit_line_hourly;
CREATE TABLE analytics.transit_line_hourly AS
SELECT date_trunc('hour', e.event_time)                                   AS hour_start,
       e.line_id,
       l.name                                                             AS line_name,
       l.mode,
       count(*)                                                           AS stop_events,
       count(DISTINCT e.trip_id)                                          AS trips,
       round(avg(e.delay_s)::numeric, 0)                                  AS avg_delay_s,
       percentile_cont(0.9) WITHIN GROUP (ORDER BY e.delay_s)             AS p90_delay_s,
       round(100.0 * avg(CASE WHEN e.delay_s BETWEEN -60 AND 180 THEN 1 ELSE 0 END), 1) AS on_time_pct,
       round(avg(e.passengers)::numeric, 1)                               AS avg_passengers
FROM raw.transit_events e
JOIN city.transit_lines l USING (line_id)
WHERE e.event_time >= now() - interval '7 days'
GROUP BY 1, 2, 3, 4;

CREATE INDEX ON analytics.transit_line_hourly (hour_start);
