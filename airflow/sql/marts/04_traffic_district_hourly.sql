-- Traffic per DISTRICT and per hour (last 7 days). District = geospatial join (ST_Contains).
DROP TABLE IF EXISTS analytics.traffic_district_hourly;
CREATE TABLE analytics.traffic_district_hourly AS
SELECT date_trunc('hour', t.window_start)                          AS hour_start,
       c.district_id,
       c.district_name,
       sum(t.vehicle_count)                                        AS vehicle_count,
       round(avg(t.avg_speed_kmh)::numeric, 1)                     AS avg_speed_kmh,
       round(avg(t.avg_speed_kmh / c.speed_limit_kmh)::numeric, 2) AS speed_ratio
FROM stream.traffic_per_minute t
JOIN analytics.sensor_context c USING (sensor_id)
WHERE t.window_start >= now() - interval '7 days'
GROUP BY 1, 2, 3;

CREATE INDEX ON analytics.traffic_district_hourly (hour_start);
