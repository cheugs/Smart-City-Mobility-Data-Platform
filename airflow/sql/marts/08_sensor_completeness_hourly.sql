-- Missing data per sensor and per hour: readings received vs readings expected
-- (one every 10 s = 360 per hour). A low percentage = the sensor was silent part of the hour.
-- Only complete hours of the last 48 h, after the pipeline started, are included.
DROP TABLE IF EXISTS analytics.sensor_completeness_hourly;
CREATE TABLE analytics.sensor_completeness_hourly AS
WITH hours AS (
    SELECT generate_series(date_trunc('hour', now() - interval '48 hours'),
                           date_trunc('hour', now()) - interval '1 hour',
                           interval '1 hour') AS hour_start
),
first_seen AS (   -- hours before the first reading are not counted as "missing"
    SELECT min(window_start) AS t0 FROM stream.traffic_per_minute
),
received AS (
    SELECT date_trunc('hour', window_start) AS hour_start, sensor_id, sum(reading_count) AS readings
    FROM stream.traffic_per_minute
    WHERE window_start >= now() - interval '49 hours'
    GROUP BY 1, 2
)
SELECT h.hour_start,
       s.sensor_id,
       coalesce(r.readings, 0)                                   AS readings_received,
       3600 / s.expected_interval_s                              AS readings_expected,
       round(100.0 * coalesce(r.readings, 0) / (3600 / s.expected_interval_s), 1) AS completeness_pct
FROM hours h
CROSS JOIN city.sensors s
CROSS JOIN first_seen f
LEFT JOIN received r ON r.hour_start = h.hour_start AND r.sensor_id = s.sensor_id
WHERE h.hour_start >= date_trunc('hour', f.t0) + interval '1 hour';

CREATE INDEX ON analytics.sensor_completeness_hourly (hour_start);
