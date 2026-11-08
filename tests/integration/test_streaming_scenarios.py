"""
Integration tests: run the REAL streaming logic (parse -> validate -> watermark
-> dedup -> window) as Spark Structured Streaming queries, one micro-batch at a time.

Kafka is replaced by a folder of text files (one file = one micro-batch), so the
tests are fast and need no Docker. Scenarios: out-of-order events, late events,
events beyond the watermark, duplicates across batches, schema evolution,
malformed data, restarting from a checkpoint and the transit windows.
"""

import json
import os
import uuid

import pytest
from pyspark.sql import functions as F

from helpers import LINES, SENSORS, STOPS, reading, transit_event
from transformations import (parse_traffic, parse_transit, traffic_per_window, transit_per_window,
                             validate_traffic, validate_transit)


class StreamHarness:
    """Starts the raw + window queries on a folder and lets a test push batches."""

    def __init__(self, spark, tmp_path, kind="traffic", watermark="10 minutes"):
        """Create the folders and start the queries."""
        self.spark, self.kind, self.watermark = spark, kind, watermark
        self.input_dir = tmp_path / "input"
        self.staging = tmp_path / "staging"
        self.checkpoints = tmp_path / "checkpoints"
        for d in (self.input_dir, self.staging):
            d.mkdir(exist_ok=True)
        self.windows = {}      # (key, "HH:MM") -> latest row (like the Postgres upsert)
        self.valid_ids = []    # what the raw sink would store
        self.invalid = []      # (reason, raw_value)
        self.queries = []
        self.start()

    def start(self):
        """Build the same pipeline as the real job, reading from the input folder."""
        source = (
            self.spark.readStream.option("maxFilesPerTrigger", 1).text(str(self.input_dir))
            .select(
                F.col("value").alias("raw_value"),
                F.current_timestamp().alias("kafka_time"),
                F.lit(0).alias("kafka_partition"),
                F.lit(0).cast("long").alias("kafka_offset"),
            )
        )
        if self.kind == "traffic":
            events = validate_traffic(parse_traffic(source), SENSORS)
            windows, key = traffic_per_window(events.where(F.col("error_reason").isNull()), self.watermark), "sensor_id"
        else:
            events = validate_transit(parse_transit(source), LINES, STOPS)
            windows, key = transit_per_window(events.where(F.col("error_reason").isNull()), self.watermark), "line_id"

        def raw_sink(df, _):
            """Collect valid ids and invalid reasons, like the raw sink does."""
            for r in df.collect():
                if r.error_reason is None:
                    self.valid_ids.append(r.event_id)
                else:
                    self.invalid.append((r.error_reason, r.raw_value))

        def window_sink(df, _):
            """Keep the latest value of each window, like the Postgres upsert."""
            for r in df.collect():
                self.windows[(r[key], str(r.window_start)[11:16])] = r

        self.queries = [
            events.writeStream.foreachBatch(raw_sink)
            .option("checkpointLocation", str(self.checkpoints / "raw")).start(),
            windows.writeStream.outputMode("update").foreachBatch(window_sink)
            .option("checkpointLocation", str(self.checkpoints / "windows")).start(),
        ]

    def send(self, *events):
        """Write one file (= one micro-batch) and wait until it is processed."""
        name = f"{uuid.uuid4().hex}.txt"
        tmp = self.staging / name
        tmp.write_text("\n".join(e if isinstance(e, str) else json.dumps(e) for e in events) + "\n")
        os.replace(tmp, self.input_dir / name)   # atomic move so Spark never sees half a file
        for q in self.queries:
            q.processAllAvailable()

    def count(self, hhmm, key="S001"):
        """Readings counted for a sensor in the window starting at HH:MM."""
        row = self.windows.get((key, hhmm))
        return row.reading_count if row else 0

    def stop(self):
        """Stop both streaming queries."""
        for q in self.queries:
            q.stop()


@pytest.fixture
def stream(spark, tmp_path):
    """A running traffic stream harness, stopped after the test."""
    h = StreamHarness(spark, tmp_path)
    yield h
    h.stop()


def t(hhmmss):
    """'10:00:30' -> '2026-01-01T10:00:30Z'"""
    return f"2026-01-01T{hhmmss}Z"


def test_out_of_order_events_land_in_the_right_window(stream):
    stream.send(reading("c", t("10:02:10")))
    stream.send(reading("a", t("10:00:30")), reading("b", t("10:01:15")))   # older readings arrive later
    assert (stream.count("10:00"), stream.count("10:01"), stream.count("10:02")) == (1, 1, 1)


def test_late_event_within_watermark_updates_its_old_window(stream):
    stream.send(reading("a", t("10:00:10")))
    stream.send(reading("b", t("10:05:00")))      # watermark is now 09:55
    stream.send(reading("late", t("10:00:40")))   # 4m20s late, still inside the 10 min watermark
    assert stream.count("10:00") == 2


def test_event_beyond_watermark_is_dropped_from_windows_but_kept_raw(stream):
    stream.send(reading("a", t("10:00:10")))
    stream.send(reading("b", t("10:30:00")))         # watermark moves to 10:20
    stream.send(reading("too-late", t("10:05:00")))  # older than the watermark
    assert stream.count("10:05") == 0                 # not counted in the windows ...
    assert "too-late" in stream.valid_ids             # ... but stored in the raw table


def test_duplicates_across_batches_are_counted_once(stream):
    e = reading("dup", t("10:00:10"), vehicles=8)
    stream.send(e, e)                     # duplicate inside one batch
    stream.send(e)                        # and again in a later batch
    stream.send(reading("other", t("10:00:20"), vehicles=2))
    assert stream.count("10:00") == 2
    assert stream.windows[("S001", "10:00")].vehicle_count == 10


def test_schema_evolution_mixed_versions_in_one_stream(stream):
    stream.send(
        reading("v1", t("10:00:01")),
        reading("v2", t("10:00:02"), schema_version=2, heavy_vehicle_count=1, firmware="2.1.0"),
        reading("v3", t("10:00:03"), schema_version=3, air_quality_index=12),
    )
    assert stream.count("10:00") == 3
    assert sorted(stream.valid_ids) == ["v1", "v2", "v3"]


def test_malformed_and_invalid_records_are_isolated_and_stream_keeps_running(stream):
    stream.send(
        reading("good", t("10:00:01")),
        '{"event_id": "broken", "sensor_id": ',
        reading("neg", t("10:00:02"), vehicles=-4),
        reading("ghost", t("10:00:03"), sensor_id="S999"),
    )
    stream.send(reading("good2", t("10:00:04")))      # next batch still processed
    assert stream.count("10:00") == 2
    assert sorted(stream.valid_ids) == ["good", "good2"]
    assert sorted(r for r, _ in stream.invalid) == ["malformed_json", "negative_vehicle_count", "unknown_sensor"]


def test_sensors_are_aggregated_separately(stream):
    stream.send(reading("a", t("10:00:01"), sensor_id="S001"), reading("b", t("10:00:02"), sensor_id="S002"),
                reading("c", t("10:00:03"), sensor_id="S002"))
    assert stream.count("10:00", "S001") == 1
    assert stream.count("10:00", "S002") == 2


def test_restart_from_checkpoint_keeps_state(spark, tmp_path):
    # run 1: process one batch, then "crash"
    first = StreamHarness(spark, tmp_path)
    first.send(reading("a", t("10:00:10"), vehicles=4))
    first.stop()

    # run 2: same checkpoint + same input folder
    second = StreamHarness(spark, tmp_path)
    try:
        second.send(reading("a", t("10:00:10"), vehicles=4),   # duplicate of an event from run 1
                    reading("b", t("10:00:20"), vehicles=3))
        # the old file is NOT processed again (offsets were checkpointed) ...
        assert second.valid_ids.count("a") == 1
        # ... and the window state survived: a counted once, plus b
        assert second.count("10:00") == 2
        assert second.windows[("S001", "10:00")].vehicle_count == 7
    finally:
        second.stop()


def test_transit_stream_delays_and_duplicates(spark, tmp_path):
    h = StreamHarness(spark, tmp_path, kind="transit")
    try:
        a = transit_event("a", t("10:01:00"), t("10:00:00"))           # +60 s
        h.send(a, a)                                                    # duplicate
        h.send(transit_event("b", t("10:03:00"), t("09:58:00")))       # +300 s, late
        h.send(transit_event("bad", t("10:02:00"), t("10:00:00"), stop_id="ZZ-99"))
        row = h.windows[("T1", "10:00")]
        assert row.stop_events == 2
        assert row.avg_delay_s == pytest.approx(180.0)
        assert row.on_time_count == 1
        assert [r for r, _ in h.invalid] == ["unknown_stop"]
    finally:
        h.stop()
