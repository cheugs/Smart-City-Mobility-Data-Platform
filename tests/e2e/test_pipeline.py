"""
End-to-end tests against the running Docker stack:
    docker compose up -d --build
    pytest tests/e2e -m "not slow"        (about 5 minutes)
    pytest tests/e2e -m slow              (Postgres outage + full replay: WIPES the output tables)

Each test uses its own unique event ids (the `tag` fixture), so the normal
producer can keep running in the background.
"""

import json
import os
import time
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from e2e_helpers import ROOT, compose, pg, run_dag, sql, wait_for


def iso(ts):
    """ISO-8601 UTC with milliseconds, like the producer."""
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def now():
    """Current time in UTC."""
    return datetime.now(timezone.utc)


def reading(event_id, ts=None, sensor_id="S001", vehicles=5, speed=42.0, occupancy=25.0, **extra):
    """A valid traffic reading."""
    e = {"event_id": event_id, "schema_version": 1, "sensor_id": sensor_id, "event_time": iso(ts or now()),
         "vehicle_count": vehicles, "avg_speed_kmh": speed, "occupancy_pct": occupancy}
    e.update(extra)
    return e


def stored(prefix, table="raw.traffic_readings"):
    """How many rows with this event_id prefix are stored."""
    return sql(f"select count(*) from {table} where event_id like %s", (prefix + "%",))[0][0]


def api(path):
    """Call the dashboard API of the running stack."""
    with urllib.request.urlopen(f"http://localhost:8000{path}", timeout=30) as r:
        return json.loads(r.read())


# ------------------------------------------------------------------ ingestion
def test_valid_readings_are_ingested_with_their_kafka_position(kafka, tag):
    kafka.traffic(*[reading(f"{tag}-{i}", sensor_id=f"S{(i % 60) + 1:03d}") for i in range(50)])
    wait_for(lambda: stored(tag) == 50, message="50 readings in raw.traffic_readings")
    rows = sql("""select count(distinct (kafka_partition, kafka_offset)),
                         bool_and(processed_at >= kafka_time), bool_and(kafka_time >= event_time - interval '5 seconds')
                  from raw.traffic_readings where event_id like %s""", (tag + "%",))
    assert rows[0] == (50, True, True)


def test_duplicates_are_stored_once_and_do_not_inflate_windows(kafka, tag):
    ts = now()
    e = reading(f"{tag}-dup", ts, sensor_id="S007", vehicles=11)
    kafka.traffic(e, e, e)
    wait_for(lambda: stored(tag) == 1, message="the duplicated reading")
    time.sleep(8)    # let the 3 copies go through
    assert stored(tag) == 1
    minute = ts.replace(second=0, microsecond=0)

    def window_matches_raw():
        """The window counts each distinct reading of that sensor and minute exactly once."""
        (window,) = sql("select reading_count from stream.traffic_per_minute where sensor_id = 'S007' and window_start = %s",
                        (minute,)) or [(None,)]
        (raw,) = sql("""select count(*) from raw.traffic_readings where sensor_id = 'S007'
                        and event_time >= %s and event_time < %s""", (minute, minute + timedelta(minutes=1)))
        return window is not None and window[0] == raw[0]
    wait_for(window_matches_raw, timeout=60, message="window count == distinct raw readings")


def test_invalid_records_are_isolated_with_their_reason(kafka, tag):
    kafka.traffic(
        reading(f"{tag}-neg", vehicles=-3),
        reading(f"{tag}-fast", speed=320.0),
        reading(f"{tag}-occ", occupancy=140.0),
        reading(f"{tag}-ghost", sensor_id="S999"),
        reading(f"{tag}-future", ts=now() + timedelta(days=1)),
        {**reading(f"{tag}-badtime"), "event_time": "31/02/2026 25:61"},
        '{"event_id": "' + tag + '-broken", "sensor_id": ',
    )
    expected = {f"{tag}-neg": "negative_vehicle_count", f"{tag}-fast": "speed_out_of_range",
                f"{tag}-occ": "occupancy_out_of_range", f"{tag}-ghost": "unknown_sensor",
                f"{tag}-future": "event_time_in_future", f"{tag}-badtime": "invalid_event_time"}
    wait_for(lambda: len(sql("select 1 from raw.bad_events where event_id like %s", (tag + "%",))) == 6,
             message="6 invalid readings in raw.bad_events")
    got = dict(sql("select event_id, error_reason from raw.bad_events where event_id like %s", (tag + "%",)))
    assert got == expected
    assert stored(tag) == 0
    assert sql("select count(*) from raw.bad_events where raw_value like %s and error_reason = 'malformed_json'",
               (f'%{tag}-broken%',))[0][0] == 1


def test_schema_versions_1_2_and_unknown_3_are_accepted(kafka, tag):
    kafka.traffic(
        reading(f"{tag}-v1"),
        reading(f"{tag}-v2", schema_version=2, heavy_vehicle_count=1, firmware="2.1.0"),
        {k: v for k, v in reading(f"{tag}-v0").items() if k != "schema_version"},
        reading(f"{tag}-v3", schema_version=3, air_quality_index=7),
    )
    wait_for(lambda: stored(tag) == 4, message="v0/v1/v2/v3 readings stored")
    rows = dict(sql("select event_id, schema_version from raw.traffic_readings where event_id like %s", (tag + "%",)))
    # no version = v1; the unknown v3 field is ignored (forward compatible) and the reading is kept
    assert rows == {f"{tag}-v1": 1, f"{tag}-v2": 2, f"{tag}-v0": 1, f"{tag}-v3": 3}
    v1_extra = sql("select heavy_vehicle_count, firmware from raw.traffic_readings where event_id = %s", (f"{tag}-v1",))
    assert v1_extra == [(None, None)]


def test_late_event_within_the_watermark_updates_its_window(kafka, tag):
    late_ts = (now() - timedelta(minutes=3)).replace(second=30, microsecond=0)
    kafka.traffic(reading(f"{tag}-late", late_ts, sensor_id="S011", vehicles=9))
    wait_for(lambda: stored(tag) == 1, message="the late reading")
    minute = late_ts.replace(second=0)

    def counted():
        """The 3-minute-old window now includes the late reading."""
        w = sql("select reading_count from stream.traffic_per_minute where sensor_id='S011' and window_start=%s", (minute,))
        raw = sql("""select count(*) from raw.traffic_readings where sensor_id='S011'
                     and event_time >= %s and event_time < %s""", (minute, minute + timedelta(minutes=1)))[0][0]
        return w and w[0][0] == raw
    wait_for(counted, timeout=60, message="late reading counted in its window")


def test_transit_delay_is_computed_and_stored(kafka, tag):
    scheduled = now().replace(microsecond=0) - timedelta(minutes=4)
    kafka.transit({"event_id": f"{tag}-bus", "schema_version": 1, "line_id": "L1", "vehicle_id": "L1-V99",
                   "trip_id": f"{tag}-trip", "stop_id": "L1-03", "event_type": "ARRIVAL",
                   "scheduled_time": iso(scheduled), "event_time": iso(scheduled + timedelta(seconds=135)),
                   "passengers": 42})
    wait_for(lambda: stored(tag, "raw.transit_events") == 1, message="the transit event")
    assert sql("select delay_s, passengers from raw.transit_events where event_id = %s", (f"{tag}-bus",))[0] == (135, 42)


# ------------------------------------------------------------------ monitoring
def test_monitoring_tables_show_progress_lag_and_starts():
    names = {r[0] for r in sql("select distinct query_name from monitoring.stream_progress")}
    assert names == {"traffic_raw", "traffic_windows", "transit_raw", "transit_windows"}
    assert sql("select count(*) from monitoring.stream_progress where kafka_lag is not null")[0][0] > 0
    assert sql("select count(*) from monitoring.batch_log where rows_received > 0")[0][0] > 0
    assert sql("""select count(*) from monitoring.pipeline_events
                  where event_type in ('fresh_start', 'restart_from_checkpoint')""")[0][0] >= 4


# ------------------------------------------------------------------ Airflow
def test_airflow_builds_geospatial_marts_and_all_checks_pass():
    run_dag("mobility_analytics")
    # Geospatial join: PostGIS finds the same road as the one the generator used
    city = json.load(open(os.path.join(ROOT, "data", "city", "city.json"), encoding="utf-8"))
    expected = {s["sensor_id"]: s["true_segment_id"] for s in city["sensors"]}
    found = dict(sql("select sensor_id, segment_id from analytics.sensor_context"))
    assert found == expected
    assert sql("select max(snap_distance_m) from analytics.sensor_context")[0][0] < 20
    assert sql("select count(*) from analytics.sensor_context where district_id is null")[0][0] == 0
    for table in ("traffic_segment_15min", "traffic_district_hourly", "traffic_hourly_profile",
                  "transit_line_hourly", "transit_stop_delays", "stop_context"):
        assert sql(f"select count(*) from analytics.{table}")[0][0] > 0, table
    latest = sql("""select distinct on (check_name) check_name, passed from monitoring.dq_results
                    order by check_name, checked_at desc""")
    assert len(latest) == 10 and all(passed for _, passed in latest), latest


def test_missing_sensor_detection_opens_and_closes_an_incident(tag):
    sid = f"TEST-{tag}"                  # a temporary sensor, removed at the end
    sql("insert into city.sensors (sensor_id, geom) values (%s, ST_SetSRID(ST_MakePoint(1.26, 45.83), 4326))", (sid,))
    try:
        # the sensor was last heard 10 minutes ago -> SILENT
        sql("insert into stream.sensor_last_seen values (%s, now() - interval '10 minutes', now() - interval '10 minutes')", (sid,))
        assert sql("select status from monitoring.sensor_health where sensor_id = %s", (sid,))[0][0] == "SILENT"
        run_dag("missing_sensor_detection")
        run_dag("missing_sensor_detection")        # a second run must not open a duplicate
        assert sql("select count(*) from monitoring.sensor_incidents where sensor_id = %s and closed_at is null", (sid,))[0][0] == 1
        # the sensor comes back -> the incident is closed
        sql("update stream.sensor_last_seen set last_event_time = now() where sensor_id = %s", (sid,))
        run_dag("missing_sensor_detection")
        assert sql("select count(*) from monitoring.sensor_incidents where sensor_id = %s and closed_at is not null", (sid,))[0][0] == 1
    finally:
        sql("delete from monitoring.sensor_incidents where sensor_id = %s", (sid,))
        sql("delete from stream.sensor_last_seen where sensor_id = %s", (sid,))
        sql("delete from city.sensors where sensor_id = %s", (sid,))


# ------------------------------------------------------------------ dashboard vs database
def test_dashboard_figures_match_the_database():
    import sys
    sys.path.insert(0, os.path.join(ROOT, "dashboard"))
    import queries

    # 1. The query functions, compared with independent SQL in the SAME database snapshot
    conn = pg()
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with conn.cursor() as cur:
            ov = queries.overview(cur, 1)["kpi"]
            q = queries.quality(cur, 1)
            cur.execute("select count(*) from raw.traffic_readings where event_time >= now() - interval '1 hour'")
            assert ov["readings"] == cur.fetchone()[0]
            cur.execute("select coalesce(sum(vehicle_count), 0) from stream.traffic_per_minute where window_start >= now() - interval '1 hour'")
            assert ov["vehicles"] == cur.fetchone()[0]
            cur.execute("select count(*) from raw.bad_events where processed_at >= now() - interval '1 hour'")
            assert sum(r["records"] for r in q["invalid"]) == cur.fetchone()[0]
            cur.execute("select count(*) from monitoring.sensor_health where status = 'OK'")
            assert ov["sensors_ok"] == cur.fetchone()[0]
    finally:
        conn.close()

    # 2. The HTTP API of the running dashboard returns the same figures (a few seconds apart)
    live = api("/api/overview?hours=1")["kpi"]
    (db_readings,) = sql("select count(*) from raw.traffic_readings where event_time >= now() - interval '1 hour'")[0]
    assert live["sensors_total"] == 60
    assert abs(live["readings"] - db_readings) <= max(200, 0.02 * db_readings)
    assert api("/api/map")["roads"]["features"].__len__() == 84


# ------------------------------------------------------------------ recovery
def test_spark_restart_loses_nothing_and_counts_nothing_twice(kafka, tag):
    kafka.traffic(*[reading(f"{tag}-a{i}", sensor_id="S020") for i in range(100)])
    compose("restart", "spark")                                  # crash in the middle of the stream
    kafka.traffic(*[reading(f"{tag}-b{i}", sensor_id="S021") for i in range(100)])
    wait_for(lambda: stored(tag) == 200, timeout=420, every=5, message="all 200 readings after the restart")
    time.sleep(10)
    assert stored(tag) == 200
    assert sql("select count(distinct event_id) from raw.traffic_readings where event_id like %s", (tag + "%",))[0][0] == 200
    assert sql("""select count(*) from monitoring.pipeline_events where event_type = 'restart_from_checkpoint'
                  and logged_at > now() - interval '10 minutes'""")[0][0] >= 4


@pytest.mark.slow
def test_postgres_outage_is_survived(kafka, tag):
    compose("stop", "postgres")
    kafka.traffic(*[reading(f"{tag}-{i}", sensor_id="S030") for i in range(50)])
    time.sleep(20)
    compose("start", "postgres")
    wait_for(lambda: stored(tag) == 50, timeout=480, every=5, message="readings sent while Postgres was down")


@pytest.mark.slow
def test_full_replay_from_kafka_gives_back_the_same_events():
    before = sql("select count(*), count(distinct event_id) from raw.traffic_readings")[0]
    sample = [r[0] for r in sql("select event_id from raw.traffic_readings order by random() limit 300")]
    script = ["powershell", "-File", "scripts/replay.ps1"] if os.name == "nt" else ["sh", "scripts/replay.sh"]
    import subprocess
    subprocess.run(script, cwd=ROOT, check=True, timeout=600)
    wait_for(lambda: sql("select count(*) from raw.traffic_readings where event_id = any(%s)", (sample,))[0][0] == len(sample),
             timeout=1200, every=10, message="the sampled events to come back after the replay")
    after = sql("select count(*), count(distinct event_id) from raw.traffic_readings")[0]
    assert after[0] == after[1]                 # still no duplicates
    assert after[0] >= before[0] * 0.99         # everything is back (the producer keeps adding new ones)
    assert sql("select count(*) from monitoring.pipeline_events where event_type = 'replay_requested'")[0][0] >= 1
