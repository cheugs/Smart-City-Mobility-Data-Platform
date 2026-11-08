-- Delay per STOP over the last 24 hours, next to the road speed measured by the
-- closest sensor (geospatial join from stop_context). Slow roads -> late buses.
DROP TABLE IF EXISTS analytics.transit_stop_delays;
CREATE TABLE analytics.transit_stop_delays AS
WITH delays AS (
    SELECT stop_id,
           count(*)                            AS stop_events,
           round(avg(delay_s)::numeric, 0)     AS avg_delay_s,
           round(100.0 * avg(CASE WHEN delay_s BETWEEN -60 AND 180 THEN 1 ELSE 0 END), 1) AS on_time_pct
    FROM raw.transit_events
    WHERE event_time >= now() - interval '24 hours'
    GROUP BY stop_id
),
road AS (
    SELECT t.sensor_id,
           round(avg(t.avg_speed_kmh / c.speed_limit_kmh)::numeric, 2) AS speed_ratio
    FROM stream.traffic_per_minute t
    JOIN analytics.sensor_context c USING (sensor_id)
    WHERE t.window_start >= now() - interval '24 hours'
    GROUP BY t.sensor_id
)
SELECT sc.stop_id, sc.line_id, sc.stop_sequence, sc.stop_name, sc.district_name,
       d.stop_events, d.avg_delay_s, d.on_time_pct,
       sc.nearest_sensor_id, sc.nearest_sensor_m, r.speed_ratio AS nearby_speed_ratio
FROM analytics.stop_context sc
LEFT JOIN delays d USING (stop_id)
LEFT JOIN road r ON r.sensor_id = sc.nearest_sensor_id;
