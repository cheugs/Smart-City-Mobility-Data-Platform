-- Traffic per ROAD SEGMENT and per 15 minutes (last 7 days), with a congestion level.
--   speed_ratio = average speed / speed limit
--   free_flow  >= 0.70  |  dense  0.45 - 0.70  |  congested  < 0.45
-- The sensor -> road link comes from the geospatial join (sensor_context).
DROP TABLE IF EXISTS analytics.traffic_segment_15min;
CREATE TABLE analytics.traffic_segment_15min AS
WITH per_segment AS (
    SELECT date_bin('15 minutes', t.window_start, timestamptz '2000-01-01') AS period_start,
           c.segment_id,
           c.road_name,
           c.road_class,
           c.speed_limit_kmh,
           count(DISTINCT t.sensor_id)                  AS sensors,
           sum(t.vehicle_count)                         AS vehicle_count,
           round(avg(t.avg_speed_kmh)::numeric, 1)      AS avg_speed_kmh,
           round(avg(t.avg_occupancy_pct)::numeric, 1)  AS avg_occupancy_pct
    FROM stream.traffic_per_minute t
    JOIN analytics.sensor_context c USING (sensor_id)
    WHERE t.window_start >= now() - interval '7 days'
    GROUP BY 1, 2, 3, 4, 5
)
SELECT *,
       round(avg_speed_kmh / speed_limit_kmh, 2) AS speed_ratio,
       CASE WHEN avg_speed_kmh / speed_limit_kmh >= 0.70 THEN 'free_flow'
            WHEN avg_speed_kmh / speed_limit_kmh >= 0.45 THEN 'dense'
            ELSE 'congested' END                 AS congestion_level
FROM per_segment;

CREATE INDEX ON analytics.traffic_segment_15min (period_start);
