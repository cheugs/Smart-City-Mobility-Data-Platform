"""
Every SQL query used by the dashboard, grouped by page.

Each public function takes a database cursor and a time range in hours and
returns plain Python data (dicts / lists), ready to be sent as JSON.
The same functions are used:
  * live, by the FastAPI app (dashboard/app.py)
  * once, by scripts/export_snapshot.py, to build the demo file for the online version
"""

import json

RANGES = {1: "1 minute", 6: "5 minutes", 24: "15 minutes"}   # range (hours) -> chart bucket size
EXPECTED_READINGS_PER_MIN = 6                                # one reading every 10 s


def bucket(hours):
    """Chart resolution for a time range."""
    return RANGES.get(hours, "15 minutes")


def rows(cur, sql, params=None):
    """Run a query and return a list of dicts."""
    cur.execute(sql, params or {})
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def one(cur, sql, params=None):
    """Run a query and return the first row as a dict (or {})."""
    result = rows(cur, sql, params)
    return result[0] if result else {}


# ================================================================ shared pieces
def pipeline_health(cur):
    """
    Healthy     : a Spark batch finished in the last 60 s and the Kafka lag is small
    Catching up : batches are running but more than 10,000 messages are waiting in Kafka
    Stalled     : no batch for more than 60 s
    """
    r = one(cur, """
        WITH latest AS (
            SELECT DISTINCT ON (query_name) query_name, batch_timestamp, kafka_lag
            FROM monitoring.stream_progress
            WHERE query_name IN ('traffic_raw', 'transit_raw')
            ORDER BY query_name, batch_timestamp DESC
        )
        SELECT extract(epoch FROM now() - max(batch_timestamp)) AS seconds_since_batch,
               coalesce(sum(kafka_lag), 0) AS kafka_lag
        FROM latest
    """)
    age = r.get("seconds_since_batch")
    lag = int(r.get("kafka_lag") or 0)
    if age is None or age > 60:
        status = "stalled"
    elif lag > 10_000:
        status = "catching_up"
    else:
        status = "healthy"
    return {"status": status, "seconds_since_batch": round(age, 1) if age is not None else None,
            "kafka_lag": lag}


# ================================================================ overview
def overview(cur, hours):
    """Main KPIs of the city and of the pipeline, plus two small trend series."""
    p = {"h": hours, "b": bucket(hours)}
    kpi = one(cur, """
        SELECT
          (SELECT count(*) FROM raw.traffic_readings WHERE event_time >= now() - make_interval(hours => %(h)s)) AS readings,
          (SELECT count(*) FROM raw.transit_events  WHERE event_time >= now() - make_interval(hours => %(h)s)) AS transit_events,
          (SELECT coalesce(sum(vehicle_count), 0) FROM stream.traffic_per_minute
             WHERE window_start >= now() - make_interval(hours => %(h)s)) AS vehicles,
          (SELECT round(avg(t.avg_speed_kmh / c.speed_limit_kmh)::numeric, 2)
             FROM stream.traffic_per_minute t JOIN analytics.sensor_context c USING (sensor_id)
             WHERE t.window_start >= now() - make_interval(hours => %(h)s)) AS speed_ratio,
          (SELECT round(avg(t.avg_speed_kmh / c.speed_limit_kmh)::numeric, 2)
             FROM stream.traffic_per_minute t JOIN analytics.sensor_context c USING (sensor_id)
             WHERE t.window_start >= now() - interval '15 minutes') AS speed_ratio_now,
          (SELECT round(100.0 * avg(CASE WHEN delay_s BETWEEN -60 AND 180 THEN 1 ELSE 0 END), 1)
             FROM raw.transit_events WHERE event_time >= now() - make_interval(hours => %(h)s)) AS on_time_pct,
          (SELECT round(avg(delay_s)::numeric, 0)
             FROM raw.transit_events WHERE event_time >= now() - make_interval(hours => %(h)s)) AS avg_delay_s,
          (SELECT count(*) FROM monitoring.sensor_health WHERE status = 'OK') AS sensors_ok,
          (SELECT count(*) FROM monitoring.sensor_health WHERE status = 'SILENT') AS sensors_silent,
          (SELECT count(*) FROM city.sensors) AS sensors_total,
          (SELECT count(*) FROM monitoring.sensor_incidents WHERE closed_at IS NULL) AS open_incidents
    """, p)
    kpi["congested_segments"] = len([s for s in segment_status(cur) if s["congestion_level"] == "congested"])
    kpi["events_per_s"] = one(cur, """
        SELECT round(coalesce(sum(num_input_rows), 0) / 300.0, 1) AS v FROM monitoring.stream_progress
        WHERE query_name IN ('traffic_raw', 'transit_raw') AND batch_timestamp >= now() - interval '5 minutes'
    """).get("v")
    kpi["latency_p50_s"] = one(cur, """
        SELECT round(percentile_cont(0.5) WITHIN GROUP (
                 ORDER BY extract(epoch FROM processed_at - kafka_time))::numeric, 2) AS v
        FROM raw.traffic_readings WHERE processed_at >= now() - interval '5 minutes'
    """).get("v")

    trend = rows(cur, """
        SELECT date_bin(%(b)s::interval, t.window_start, timestamptz '2000-01-01') AS t,
               sum(t.vehicle_count) AS vehicles,
               round(avg(t.avg_speed_kmh / c.speed_limit_kmh)::numeric, 3) AS speed_ratio
        FROM stream.traffic_per_minute t JOIN analytics.sensor_context c USING (sensor_id)
        WHERE t.window_start >= now() - make_interval(hours => %(h)s)
        GROUP BY 1 ORDER BY 1
    """, p)
    transit = rows(cur, """
        SELECT date_bin(%(b)s::interval, window_start, timestamptz '2000-01-01') AS t,
               round(100.0 * sum(on_time_count) / nullif(sum(stop_events), 0), 1) AS on_time_pct,
               round((sum(avg_delay_s * stop_events) / nullif(sum(stop_events), 0))::numeric, 0) AS avg_delay_s
        FROM stream.transit_per_5min
        WHERE window_start >= now() - make_interval(hours => %(h)s)
        GROUP BY 1 ORDER BY 1
    """, p)
    return {"kpi": kpi, "health": pipeline_health(cur), "trend": trend, "transit_trend": transit}


# ================================================================ traffic
def segment_status(cur, minutes=15):
    """Congestion of every road segment over the last N minutes (live, from the Spark windows)."""
    return rows(cur, """
        SELECT c.segment_id, c.road_name, c.road_class, c.speed_limit_kmh,
               round(avg(t.avg_speed_kmh)::numeric, 1) AS avg_speed_kmh,
               sum(t.vehicle_count) AS vehicles,
               round(avg(t.avg_speed_kmh / c.speed_limit_kmh)::numeric, 2) AS speed_ratio,
               CASE WHEN avg(t.avg_speed_kmh / c.speed_limit_kmh) >= 0.70 THEN 'free_flow'
                    WHEN avg(t.avg_speed_kmh / c.speed_limit_kmh) >= 0.45 THEN 'dense'
                    ELSE 'congested' END AS congestion_level
        FROM stream.traffic_per_minute t JOIN analytics.sensor_context c USING (sensor_id)
        WHERE t.window_start >= now() - make_interval(mins => %(m)s)
        GROUP BY 1, 2, 3, 4
        ORDER BY speed_ratio
    """, {"m": minutes})


def traffic(cur, hours):
    """Traffic page: trends by road class, districts, hour-of-day pattern, slowest roads."""
    p = {"h": hours, "b": bucket(hours)}
    by_class = rows(cur, """
        SELECT date_bin(%(b)s::interval, t.window_start, timestamptz '2000-01-01') AS t,
               c.road_class,
               sum(t.vehicle_count) AS vehicles,
               round(avg(t.avg_speed_kmh)::numeric, 1) AS avg_speed_kmh
        FROM stream.traffic_per_minute t JOIN analytics.sensor_context c USING (sensor_id)
        WHERE t.window_start >= now() - make_interval(hours => %(h)s)
        GROUP BY 1, 2 ORDER BY 1, 2
    """, p)
    districts = rows(cur, """
        SELECT c.district_name AS district, sum(t.vehicle_count) AS vehicles,
               round(avg(t.avg_speed_kmh)::numeric, 1) AS avg_speed_kmh,
               round(avg(t.avg_speed_kmh / c.speed_limit_kmh)::numeric, 2) AS speed_ratio,
               count(DISTINCT t.sensor_id) AS sensors
        FROM stream.traffic_per_minute t JOIN analytics.sensor_context c USING (sensor_id)
        WHERE t.window_start >= now() - make_interval(hours => %(h)s)
        GROUP BY 1 ORDER BY speed_ratio
    """, p)
    profile = rows(cur, """
        SELECT hour_of_day, road_class, avg_vehicles_per_min, avg_speed_kmh, speed_ratio
        FROM analytics.traffic_hourly_profile ORDER BY hour_of_day, road_class
    """)
    return {"by_class": by_class, "districts": districts, "profile": profile,
            "slowest_segments": segment_status(cur)[:10]}


# ================================================================ map
def _collection(features):
    """Build a GeoJSON FeatureCollection from rows that have a 'geometry' JSON text column."""
    return {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": json.loads(f.pop("geometry")), "properties": f} for f in features]}


def city_map(cur):
    """All map layers with their current state (roads, sensors, stops, lines, districts)."""
    status = {s["segment_id"]: s for s in segment_status(cur)}
    roads = rows(cur, """SELECT segment_id, name, road_class, speed_limit_kmh, ST_AsGeoJSON(geom, 6) AS geometry
                         FROM city.road_segments""")
    for r in roads:
        s = status.get(r["segment_id"], {})
        r.update(speed_ratio=s.get("speed_ratio"), avg_speed_kmh=s.get("avg_speed_kmh"),
                 congestion_level=s.get("congestion_level", "no_data"))
    sensors = rows(cur, """
        SELECT h.sensor_id, h.status, h.seconds_since_last, c.road_name, c.district_name, c.snap_distance_m,
               ST_AsGeoJSON(s.geom, 6) AS geometry
        FROM monitoring.sensor_health h
        JOIN city.sensors s USING (sensor_id)
        LEFT JOIN analytics.sensor_context c USING (sensor_id)
    """)
    stops = rows(cur, """
        SELECT st.stop_id, st.line_id, st.name, st.stop_sequence,
               round(avg(e.delay_s)::numeric, 0) AS avg_delay_s, count(e.event_id) AS stop_events,
               ST_AsGeoJSON(st.geom, 6) AS geometry
        FROM city.transit_stops st
        LEFT JOIN raw.transit_events e ON e.stop_id = st.stop_id AND e.event_time >= now() - interval '1 hour'
        GROUP BY st.stop_id
    """)
    lines = rows(cur, """
        SELECT l.line_id, l.name, l.mode, l.color,
               ST_AsGeoJSON(ST_MakeLine(st.geom ORDER BY st.stop_sequence), 6) AS geometry
        FROM city.transit_lines l JOIN city.transit_stops st USING (line_id)
        GROUP BY l.line_id
    """)
    districts = rows(cur, """
        SELECT d.district_id, d.name,
               round(avg(t.avg_speed_kmh / c.speed_limit_kmh)::numeric, 2) AS speed_ratio,
               ST_AsGeoJSON(d.geom, 6) AS geometry
        FROM city.districts d
        LEFT JOIN analytics.sensor_context c ON c.district_id = d.district_id
        LEFT JOIN stream.traffic_per_minute t ON t.sensor_id = c.sensor_id
                                              AND t.window_start >= now() - interval '15 minutes'
        GROUP BY d.district_id
    """)
    return {"roads": _collection(roads), "sensors": _collection(sensors), "stops": _collection(stops),
            "lines": _collection(lines), "districts": _collection(districts)}


# ================================================================ public transport
def transit(cur, hours):
    """Transit page: KPIs per line, delay trend per line (Spark windows), worst stops."""
    p = {"h": hours, "b": bucket(hours)}
    lines = rows(cur, """
        SELECT e.line_id, l.name, l.mode, l.color,
               count(*) AS stop_events, count(DISTINCT e.trip_id) AS trips,
               round(avg(e.delay_s)::numeric, 0) AS avg_delay_s,
               round(percentile_cont(0.9) WITHIN GROUP (ORDER BY e.delay_s)::numeric, 0) AS p90_delay_s,
               round(100.0 * avg(CASE WHEN e.delay_s BETWEEN -60 AND 180 THEN 1 ELSE 0 END), 1) AS on_time_pct,
               round(avg(e.passengers)::numeric, 0) AS avg_passengers
        FROM raw.transit_events e JOIN city.transit_lines l USING (line_id)
        WHERE e.event_time >= now() - make_interval(hours => %(h)s)
        GROUP BY e.line_id, l.name, l.mode, l.color ORDER BY e.line_id
    """, p)
    trend = rows(cur, """
        SELECT date_bin(%(b)s::interval, window_start, timestamptz '2000-01-01') AS t, line_id,
               round((sum(avg_delay_s * stop_events) / nullif(sum(stop_events), 0))::numeric, 0) AS avg_delay_s,
               round(100.0 * sum(on_time_count) / nullif(sum(stop_events), 0), 1) AS on_time_pct
        FROM stream.transit_per_5min
        WHERE window_start >= now() - make_interval(hours => %(h)s)
        GROUP BY 1, 2 ORDER BY 1, 2
    """, {"h": hours, "b": "5 minutes" if hours == 1 else bucket(hours)})
    stops = rows(cur, """
        SELECT stop_id, line_id, stop_name, district_name, stop_events, avg_delay_s, on_time_pct,
               nearest_sensor_id, nearest_sensor_m, nearby_speed_ratio
        FROM analytics.transit_stop_delays
        WHERE stop_events IS NOT NULL
        ORDER BY avg_delay_s DESC NULLS LAST LIMIT 12
    """)
    return {"lines": lines, "trend": trend, "worst_stops": stops}


# ================================================================ pipeline
def pipeline(cur, hours):
    """Pipeline page: throughput, Kafka lag, batch duration, latency, restarts, Airflow checks."""
    p = {"h": hours, "b": bucket(hours)}
    series = rows(cur, """
        SELECT date_bin(%(b)s::interval, batch_timestamp, timestamptz '2000-01-01') AS t, query_name,
               round(sum(num_input_rows) / extract(epoch FROM %(b)s::interval), 2) AS events_per_s,
               max(kafka_lag) AS kafka_lag,
               round(avg(batch_duration_ms)) AS batch_ms
        FROM monitoring.stream_progress
        WHERE batch_timestamp >= now() - make_interval(hours => %(h)s)
        GROUP BY 1, 2 ORDER BY 1, 2
    """, p)
    latency = one(cur, """
        SELECT round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - kafka_time))::numeric, 2) AS p50_s,
               round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - kafka_time))::numeric, 2) AS p95_s,
               round(max(extract(epoch FROM processed_at - kafka_time))::numeric, 2) AS max_s,
               count(*) AS events
        FROM raw.traffic_readings WHERE processed_at >= now() - make_interval(hours => %(h)s)
    """, p)
    totals = one(cur, """
        SELECT count(*) AS batches,
               count(DISTINCT run_id) AS spark_runs,
               coalesce(sum(num_input_rows), 0) AS messages_read
        FROM monitoring.stream_progress WHERE batch_timestamp >= now() - make_interval(hours => %(h)s)
    """, p)
    events = rows(cur, """
        SELECT logged_at, event_type, query_name, details FROM monitoring.pipeline_events
        ORDER BY logged_at DESC LIMIT 15
    """)
    checks = rows(cur, """
        SELECT DISTINCT ON (check_name) check_name, failed_rows, passed, checked_at
        FROM monitoring.dq_results ORDER BY check_name, checked_at DESC
    """)
    return {"health": pipeline_health(cur), "series": series, "latency": latency, "totals": totals,
            "events": events, "checks": checks}


# ================================================================ data quality
def quality(cur, hours):
    """Data-quality page: invalid records, duplicates, late events, schema versions, sensors."""
    p = {"h": hours}
    invalid = rows(cur, """
        SELECT source, error_reason, count(*) AS records FROM raw.bad_events
        WHERE processed_at >= now() - make_interval(hours => %(h)s)
        GROUP BY 1, 2 ORDER BY records DESC
    """, p)
    batches = one(cur, """
        SELECT coalesce(sum(rows_received), 0) AS valid_received,
               coalesce(sum(rows_inserted), 0) AS stored,
               coalesce(sum(duplicates_skipped), 0) AS duplicates_skipped,
               coalesce(sum(bad_rows), 0) AS invalid
        FROM monitoring.batch_log
        WHERE query_name IN ('traffic_raw', 'transit_raw') AND logged_at >= now() - make_interval(hours => %(h)s)
    """, p)
    late = one(cur, """
        SELECT count(*) AS readings,
               sum(CASE WHEN kafka_time - event_time > interval '60 seconds' THEN 1 ELSE 0 END) AS late,
               sum(CASE WHEN kafka_time - event_time > interval '10 minutes' THEN 1 ELSE 0 END) AS beyond_watermark,
               round(avg(extract(epoch FROM kafka_time - event_time))::numeric, 2) AS avg_arrival_delay_s
        FROM raw.traffic_readings WHERE event_time >= now() - make_interval(hours => %(h)s)
    """, p)
    versions = rows(cur, """
        SELECT schema_version, count(*) AS readings FROM raw.traffic_readings
        WHERE event_time >= now() - make_interval(hours => %(h)s)
        GROUP BY 1 ORDER BY 1
    """, p)
    sensors = rows(cur, "SELECT status, count(*) AS sensors FROM monitoring.sensor_health GROUP BY 1 ORDER BY 1")
    incidents = rows(cur, """
        SELECT incident_id, sensor_id, opened_at, last_seen_at, closed_at,
               round(extract(epoch FROM coalesce(closed_at, now()) - coalesce(last_seen_at, opened_at)) / 60) AS minutes
        FROM monitoring.sensor_incidents
        ORDER BY closed_at IS NULL DESC, opened_at DESC LIMIT 15
    """)
    completeness = rows(cur, """
        SELECT hour_start AS t, round(avg(completeness_pct), 1) AS completeness_pct,
               sum(CASE WHEN completeness_pct < 90 THEN 1 ELSE 0 END) AS sensors_below_90
        FROM analytics.sensor_completeness_hourly
        WHERE hour_start >= now() - make_interval(hours => greatest(%(h)s, 6))
        GROUP BY 1 ORDER BY 1
    """, p)
    return {"invalid": invalid, "batches": batches, "late": late, "schema_versions": versions,
            "sensor_status": sensors, "incidents": incidents, "completeness": completeness}
