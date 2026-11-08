-- Automatic data-quality checks, run by the Airflow DAG after the marts are built.
-- Each check is a query that returns ONE number: how many rows break the rule.
-- 0 = passed. Anything else fails the Airflow task (and is saved in monitoring.dq_results).
-- Format: a "-- check: <name>" line, then the query, ending with ";".

-- check: no_duplicate_traffic_event_ids
SELECT count(*) FROM (
    SELECT event_id FROM raw.traffic_readings GROUP BY event_id HAVING count(*) > 1
) d;

-- check: no_duplicate_transit_event_ids
SELECT count(*) FROM (
    SELECT event_id FROM raw.transit_events GROUP BY event_id HAVING count(*) > 1
) d;

-- check: windows_not_inflated_by_duplicates
-- A window can never count MORE readings than the distinct readings stored for
-- that sensor and minute (it can count fewer: events beyond the watermark).
SELECT count(*) FROM stream.traffic_per_minute w
WHERE w.window_start >= now() - interval '6 hours'
  AND w.reading_count > (
      SELECT count(*) FROM raw.traffic_readings r
      WHERE r.sensor_id = w.sensor_id
        AND r.event_time >= w.window_start AND r.event_time < w.window_end
  );

-- check: window_vehicle_totals_not_inflated
SELECT count(*) FROM stream.traffic_per_minute w
WHERE w.window_start >= now() - interval '6 hours'
  AND w.vehicle_count > (
      SELECT coalesce(sum(vehicle_count), 0) FROM raw.traffic_readings r
      WHERE r.sensor_id = w.sensor_id
        AND r.event_time >= w.window_start AND r.event_time < w.window_end
  );

-- check: no_future_events
SELECT (SELECT count(*) FROM raw.traffic_readings WHERE event_time > now() + interval '5 minutes')
     + (SELECT count(*) FROM raw.transit_events   WHERE event_time > now() + interval '5 minutes');

-- check: measurements_in_valid_range
SELECT count(*) FROM raw.traffic_readings
WHERE vehicle_count < 0 OR avg_speed_kmh NOT BETWEEN 0 AND 200 OR occupancy_pct NOT BETWEEN 0 AND 100;

-- check: transit_delay_matches_timestamps
SELECT count(*) FROM raw.transit_events
WHERE abs(delay_s - extract(epoch FROM event_time - scheduled_time)) > 1;

-- check: every_sensor_snapped_to_a_road_within_30m
SELECT count(*) FROM city.sensors s
LEFT JOIN analytics.sensor_context c USING (sensor_id)
WHERE c.segment_id IS NULL OR c.snap_distance_m > 30;

-- check: every_sensor_inside_a_district
SELECT count(*) FROM analytics.sensor_context WHERE district_id IS NULL;

-- check: v1_readings_have_no_v2_fields
-- Schema evolution: fields added in v2 must stay empty for v1 readings.
SELECT count(*) FROM raw.traffic_readings
WHERE schema_version = 1 AND (heavy_vehicle_count IS NOT NULL OR firmware IS NOT NULL);
