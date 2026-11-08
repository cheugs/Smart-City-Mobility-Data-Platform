"""
Unit tests of the window aggregations, on a static DataFrame
(the watermark has no effect on static data, the maths is the same).
"""

import pytest
from pyspark.sql import functions as F

from helpers import LINES, SENSORS, STOPS, kafka_df, reading, transit_event
from transformations import (parse_traffic, parse_transit, traffic_per_window, transit_per_window,
                             validate_traffic, validate_transit)


def traffic_windows(spark, events, kafka_time="2026-01-01 10:00:00"):
    valid = validate_traffic(parse_traffic(kafka_df(spark, events, kafka_time)), SENSORS)
    rows = traffic_per_window(valid.where(F.col("error_reason").isNull())).collect()
    return {(r.sensor_id, str(r.window_start)[11:16]): r for r in rows}


def test_counts_sums_and_averages_per_sensor_and_minute(spark):
    w = traffic_windows(spark, [
        reading("a", "2026-01-01T10:00:05Z", vehicles=4, speed=30.0, occupancy=10.0),
        reading("b", "2026-01-01T10:00:15Z", vehicles=6, speed=50.0, occupancy=30.0),
        reading("c", "2026-01-01T10:01:05Z", vehicles=1, speed=45.0),
        reading("d", "2026-01-01T10:00:20Z", sensor_id="S002", vehicles=9),
    ])
    first = w[("S001", "10:00")]
    assert first.reading_count == 2
    assert first.vehicle_count == 10
    assert first.avg_speed_kmh == pytest.approx(40.0)
    assert first.min_speed_kmh == pytest.approx(30.0)
    assert first.avg_occupancy_pct == pytest.approx(20.0)
    assert w[("S001", "10:01")].reading_count == 1
    assert w[("S002", "10:00")].vehicle_count == 9


def test_duplicate_event_ids_are_counted_once(spark):
    e = reading("dup", "2026-01-01T10:00:05Z", vehicles=7)
    w = traffic_windows(spark, [e, e, e, reading("other", "2026-01-01T10:00:30Z", vehicles=3)])
    assert w[("S001", "10:00")].reading_count == 2
    assert w[("S001", "10:00")].vehicle_count == 10


def test_invalid_readings_are_not_aggregated(spark):
    w = traffic_windows(spark, [
        reading("ok", "2026-01-01T10:00:05Z", vehicles=2),
        reading("bad", "2026-01-01T10:00:06Z", vehicles=-50),
    ])
    assert w[("S001", "10:00")].vehicle_count == 2


def test_arrival_delay_and_late_count(spark):
    # Kafka received everything at 10:05:00; one reading is from 10:04:30 (30 s), one from 10:03:00 (120 s)
    w = traffic_windows(spark, [
        reading("on-time", "2026-01-01T10:04:30Z"),
        reading("late", "2026-01-01T10:03:00Z"),
    ], kafka_time="2026-01-01 10:05:00")
    assert w[("S001", "10:04")].late_event_count == 0
    assert w[("S001", "10:03")].late_event_count == 1
    assert w[("S001", "10:03")].avg_arrival_delay_s == pytest.approx(120.0)


def test_transit_windows_delay_and_on_time(spark):
    events = [
        transit_event("a", "2026-01-01T10:01:00Z", "2026-01-01T10:00:00Z", passengers=10),   # +60 s, on time
        transit_event("b", "2026-01-01T10:04:00Z", "2026-01-01T09:59:00Z", passengers=30),   # +300 s, late
        transit_event("c", "2026-01-01T10:02:00Z", "2026-01-01T10:03:30Z", passengers=20),   # -90 s, too early
    ]
    valid = validate_transit(parse_transit(kafka_df(spark, events)), LINES, STOPS)
    rows = transit_per_window(valid.where(F.col("error_reason").isNull())).collect()
    assert len(rows) == 1
    r = rows[0]
    assert r.line_id == "T1" and str(r.window_start)[11:16] == "10:00"
    assert r.stop_events == 3
    assert r.on_time_count == 1
    assert r.avg_delay_s == pytest.approx(90.0)        # (60 + 300 - 90) / 3
    assert r.max_delay_s == 300
    assert r.avg_passengers == pytest.approx(20.0)
