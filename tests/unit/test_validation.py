"""Unit tests of the data-quality rules (spark/transformations.py)."""

import pytest

from helpers import LINES, SENSORS, STOPS, kafka_df, reading, transit_event
from transformations import parse_traffic, parse_transit, validate_traffic, validate_transit


def traffic_errors(spark, *events):
    """Run parse + validation and return {event_id or raw: error_reason}."""
    df = validate_traffic(parse_traffic(kafka_df(spark, list(events))), SENSORS)
    return [r.error_reason for r in df.orderBy("kafka_offset").collect()]


def transit_errors(spark, *events):
    df = validate_transit(parse_transit(kafka_df(spark, list(events))), LINES, STOPS)
    return [r.error_reason for r in df.orderBy("kafka_offset").collect()]


T = "2026-01-01T10:00:00Z"


def test_valid_reading_has_no_error(spark):
    assert traffic_errors(spark, reading("a", T)) == [None]


@pytest.mark.parametrize("change, reason", [
    ({"event_id": None}, "missing_event_id"),
    ({"sensor_id": None}, "missing_sensor_id"),
    ({"sensor_id": "S999"}, "unknown_sensor"),
    ({"event_time": None}, "missing_event_time"),
    ({"event_time": "31/02/2026 25:61"}, "invalid_event_time"),
    ({"event_time": "2099-01-01T00:00:00Z"}, "event_time_in_future"),
    ({"avg_speed_kmh": None}, "missing_measurement"),
    ({"vehicle_count": -3}, "negative_vehicle_count"),
    ({"vehicle_count": 999}, "vehicle_count_too_high"),
    ({"avg_speed_kmh": 260.0}, "speed_out_of_range"),
    ({"avg_speed_kmh": -1.0}, "speed_out_of_range"),
    ({"occupancy_pct": 120.0}, "occupancy_out_of_range"),
])
def test_each_traffic_rule(spark, change, reason):
    e = reading("x", T)
    for key, value in change.items():
        if value is None:
            e.pop(key)
        else:
            e[key] = value
    assert traffic_errors(spark, e) == [reason]


def test_malformed_json_is_detected(spark):
    assert traffic_errors(spark, '{"event_id": "broken", "sensor_id": ') == ["malformed_json"]


def test_missing_schema_version_means_v1(spark):
    e = reading("a", T)
    e.pop("schema_version")
    df = parse_traffic(kafka_df(spark, [e]))
    assert df.first().schema_version == 1


def test_schema_v2_fields_are_read_and_v1_fields_stay_null(spark):
    v1 = reading("v1", T)
    v2 = reading("v2", T, schema_version=2, heavy_vehicle_count=2, firmware="2.1.0")
    rows = {r.event_id: r for r in parse_traffic(kafka_df(spark, [v1, v2])).collect()}
    assert rows["v2"].heavy_vehicle_count == 2 and rows["v2"].firmware == "2.1.0"
    assert rows["v1"].heavy_vehicle_count is None and rows["v1"].firmware is None


def test_unknown_future_fields_are_ignored(spark):
    e = reading("v3", T, schema_version=3, air_quality_index=42)
    assert traffic_errors(spark, e) == [None]


def test_transit_delay_is_computed_from_the_timetable(spark):
    e = transit_event("t1", "2026-01-01T10:02:30Z", "2026-01-01T10:00:00Z")
    row = parse_transit(kafka_df(spark, [e])).first()
    assert row.delay_s == 150
    early = transit_event("t2", "2026-01-01T09:59:20Z", "2026-01-01T10:00:00Z")
    assert parse_transit(kafka_df(spark, [early])).first().delay_s == -40


@pytest.mark.parametrize("change, reason", [
    ({"line_id": "X9"}, "unknown_line"),
    ({"stop_id": "ZZ-99"}, "unknown_stop"),
    ({"scheduled_time": None}, "invalid_scheduled_time"),
    ({"event_type": "TELEPORT"}, "invalid_event_type"),
    ({"passengers": -5}, "passengers_out_of_range"),
    ({"passengers": 999}, "passengers_out_of_range"),
    ({"scheduled_time": "2026-01-01T06:00:00Z"}, "delay_out_of_range"),
])
def test_each_transit_rule(spark, change, reason):
    e = transit_event("x", "2026-01-01T10:00:00Z", "2026-01-01T10:00:00Z")
    for key, value in change.items():
        if value is None:
            e.pop(key)
        else:
            e[key] = value
    assert transit_errors(spark, e) == [reason]


def test_valid_transit_event_has_no_error(spark):
    assert transit_errors(spark, transit_event("ok", "2026-01-01T10:01:00Z", "2026-01-01T10:00:00Z")) == [None]
