-- TRAFFIC PATTERN: average traffic by hour of the day (local time) and road class.
-- Shows the morning and evening rush hours.
DROP TABLE IF EXISTS analytics.traffic_hourly_profile;
CREATE TABLE analytics.traffic_hourly_profile AS
SELECT extract(hour FROM t.window_start AT TIME ZONE 'Europe/Paris')::int AS hour_of_day,
       c.road_class,
       round(avg(t.vehicle_count)::numeric, 1)                     AS avg_vehicles_per_min,
       round(avg(t.avg_speed_kmh)::numeric, 1)                     AS avg_speed_kmh,
       round(avg(t.avg_speed_kmh / c.speed_limit_kmh)::numeric, 2) AS speed_ratio,
       count(*)                                                    AS sensor_minutes
FROM stream.traffic_per_minute t
JOIN analytics.sensor_context c USING (sensor_id)
WHERE t.window_start >= now() - interval '7 days'
GROUP BY 1, 2;
