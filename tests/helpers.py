"""Small helpers to build test events and Kafka-shaped DataFrames."""

import json
from datetime import datetime, timezone

# A few ids that exist in the sample city (sql/02_city_seed.sql)
SENSORS = ["S001", "S002", "S003"]
LINES = ["T1", "L1"]
STOPS = ["T1-01", "T1-02", "L1-01"]


def utc(text):
    """'2026-01-01 10:00:30' -> timezone-aware datetime"""
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def reading(event_id, event_time, sensor_id="S001", vehicles=5, speed=40.0, occupancy=20.0, **extra):
    """A small valid traffic reading; event_time like '2026-01-01T10:00:30Z'."""
    e = {"event_id": event_id, "schema_version": 1, "sensor_id": sensor_id, "event_time": event_time,
         "vehicle_count": vehicles, "avg_speed_kmh": speed, "occupancy_pct": occupancy}
    e.update(extra)
    return e


def transit_event(event_id, event_time, scheduled_time, line_id="T1", stop_id="T1-02", passengers=30, **extra):
    """A small valid bus/tram arrival."""
    e = {"event_id": event_id, "schema_version": 1, "line_id": line_id, "vehicle_id": f"{line_id}-V01",
         "trip_id": f"{line_id}-trip", "stop_id": stop_id, "event_type": "ARRIVAL",
         "scheduled_time": scheduled_time, "event_time": event_time, "passengers": passengers}
    e.update(extra)
    return e


def kafka_df(spark, events, kafka_time="2026-01-01 10:00:00"):
    """Build a DataFrame shaped like the Kafka input (raw_value = JSON text)."""
    rows = [
        (e if isinstance(e, str) else json.dumps(e), utc(kafka_time), 0, i)
        for i, e in enumerate(events)
    ]
    return spark.createDataFrame(
        rows, "raw_value string, kafka_time timestamp, kafka_partition int, kafka_offset long"
    )
