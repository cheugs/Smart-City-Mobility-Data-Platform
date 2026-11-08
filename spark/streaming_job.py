"""
PySpark Structured Streaming job: Kafka -> PostGIS.

Reads two Kafka topics and runs four streaming queries at the same time:

  1. traffic_raw      - stores every valid sensor reading in raw.traffic_readings
                        (duplicates ignored thanks to the event_id primary key),
                        updates stream.sensor_last_seen, and puts invalid messages in bad_events
  2. traffic_windows  - per sensor, 1-minute EVENT-TIME windows (10-minute watermark)
                        upserted into stream.traffic_per_minute
  3. transit_raw      - same as 1 for bus / tram events -> raw.transit_events
  4. transit_windows  - per line, 5-minute windows -> stream.transit_per_5min

All queries use checkpoints, so if Spark restarts it carries on from the last
Kafka offset it finished. Because every sink is idempotent (INSERT ... ON
CONFLICT), re-processing a batch never creates duplicates.

While running, the main thread saves each query's progress (throughput, batch
duration, Kafka lag) into stream_progress, and start/failure events into
pipeline_events, so Grafana and the dashboard can monitor the pipeline.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transformations import (parse_traffic, parse_transit, traffic_per_window, transit_per_window,
                             validate_traffic, validate_transit)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29093")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic.readings")
TRANSIT_TOPIC = os.getenv("TRANSIT_TOPIC", "transit.events")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints")
WATERMARK = os.getenv("WATERMARK", "10 minutes")          # how long we wait for late events
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "5 seconds")
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "20000"))
QUERY_NAMES = ["traffic_raw", "traffic_windows", "transit_raw", "transit_windows"]

PG = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DB", "mobility"),
    user=os.getenv("PG_USER", "mobility"),
    password=os.getenv("PG_PASSWORD", ""),
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("py4j").setLevel(logging.WARNING)   # it prints too many messages at INFO level
log = logging.getLogger("streaming_job")


# ------------------------------------------------------------------ helpers
def with_retry(action, what, attempts=3, wait_s=2):
    """Retry a database action a few times (e.g. Postgres restarting).
    If it still fails, the error goes up to Spark, the query stops and the
    container restarts -> the same batch is processed again from the checkpoint."""
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except psycopg2.OperationalError as e:
            log.warning("%s failed (attempt %d/%d): %s", what, attempt, attempts, e)
            if attempt == attempts:
                raise
            time.sleep(wait_s * attempt)


def pg_connect():
    """Open a new connection to Postgres."""
    return psycopg2.connect(**PG)


def log_pipeline_event(event_type, query_name=None, run_id=None, details=None):
    """Write a row to pipeline_events (start, restart, failure...) and to the log."""
    log.info("pipeline event: %s %s %s", event_type, query_name or "", details or "")

    def insert():
        """The database work, wrapped so with_retry() can run it again."""
        with pg_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO monitoring.pipeline_events (event_type, query_name, run_id, details) VALUES (%s, %s, %s, %s)",
                (event_type, query_name, run_id, details),
            )
    try:
        with_retry(insert, "log pipeline event")
    except Exception as e:  # monitoring must never crash the pipeline
        log.error("could not save pipeline event: %s", e)


def load_reference_ids():
    """Read the known sensor, line and stop ids from PostGIS once, at start-up."""
    def read():
        """The database work, wrapped so with_retry() can run it again."""
        with pg_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT sensor_id FROM city.sensors")
            sensors = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT line_id FROM city.transit_lines")
            lines = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT stop_id FROM city.transit_stops")
            stops = [r[0] for r in cur.fetchall()]
            return sensors, lines, stops
    sensors, lines, stops = with_retry(read, "load reference data", attempts=10, wait_s=3)
    log.info("reference data: %d sensors, %d lines, %d stops", len(sensors), len(lines), len(stops))
    return sensors, lines, stops


def last_committed_batch(query_name):
    """Look in the checkpoint folder to see if we are resuming after a restart."""
    commits = os.path.join(CHECKPOINT_DIR, query_name, "commits")
    if not os.path.isdir(commits):
        return None
    ids = [int(f) for f in os.listdir(commits) if f.isdigit()]
    return max(ids) if ids else None


def log_batch(cur, query_name, batch_id, received, inserted, bad=0):
    """Save how many rows a micro-batch received and inserted."""
    # Same transaction as the data: if the batch is rolled back, so is this row.
    # ON CONFLICT DO NOTHING: if Spark re-runs a batch that was already committed,
    # we keep the original numbers.
    cur.execute(
        """INSERT INTO monitoring.batch_log (query_name, batch_id, rows_received, rows_inserted,
                                  duplicates_skipped, bad_rows)
           VALUES (%s, %s, %s, %s, %s, %s)
           ON CONFLICT (query_name, batch_id) DO NOTHING""",
        (query_name, batch_id, received, inserted, received - inserted, bad),
    )


def insert_bad(cur, source, rows, processed_at):
    """Invalid / malformed messages go to the dead-letter table, keyed by their Kafka position."""
    if rows:
        execute_values(
            cur,
            """INSERT INTO raw.bad_events (source, kafka_partition, kafka_offset, event_id, error_reason,
                   raw_value, processed_at)
               VALUES %s
               ON CONFLICT (source, kafka_partition, kafka_offset) DO NOTHING""",
            [(source, r.kafka_partition, r.kafka_offset, r.event_id, r.error_reason, r.raw_value, processed_at)
             for r in rows],
        )


# ------------------------------------------------------------------ sink 1: traffic readings
def write_traffic_raw(batch_df, batch_id):
    """Called by Spark once per micro-batch for the traffic_raw query."""
    processed_at = datetime.now(timezone.utc)
    rows = batch_df.collect()   # OK for a student-sized stream (see docs/PERFORMANCE.md)
    good = [r for r in rows if r.error_reason is None]
    bad = [r for r in rows if r.error_reason is not None]

    def write():
        """The database work, wrapped so with_retry() can run it again."""
        with pg_connect() as conn, conn.cursor() as cur:
            inserted = []
            if good:
                # ON CONFLICT DO NOTHING = duplicate events are skipped.
                # RETURNING tells us how many rows were really new.
                inserted = execute_values(
                    cur,
                    """INSERT INTO raw.traffic_readings (event_id, schema_version, sensor_id, vehicle_count,
                           avg_speed_kmh, occupancy_pct, heavy_vehicle_count, firmware,
                           event_time, kafka_time, processed_at, kafka_partition, kafka_offset)
                       VALUES %s
                       ON CONFLICT (event_id) DO NOTHING
                       RETURNING event_id""",
                    [(r.event_id, r.schema_version, r.sensor_id, r.vehicle_count, r.avg_speed_kmh,
                      r.occupancy_pct, r.heavy_vehicle_count, r.firmware, r.event_time, r.kafka_time,
                      processed_at, r.kafka_partition, r.kafka_offset) for r in good],
                    fetch=True, page_size=5000,
                )
                # Missing-sensor detection, step 1: remember when each sensor was last heard from.
                # GREATEST: a late event must never move last_event_time backwards.
                execute_values(
                    cur,
                    """INSERT INTO stream.sensor_last_seen AS s (sensor_id, last_event_time, last_kafka_time)
                       SELECT sensor_id, max(event_time), max(kafka_time)
                       FROM (VALUES %s) AS v(sensor_id, event_time, kafka_time)
                       GROUP BY sensor_id
                       ON CONFLICT (sensor_id) DO UPDATE SET
                           last_event_time = GREATEST(s.last_event_time, EXCLUDED.last_event_time),
                           last_kafka_time = GREATEST(s.last_kafka_time, EXCLUDED.last_kafka_time)""",
                    [(r.sensor_id, r.event_time, r.kafka_time) for r in good],
                    template="(%s, %s::timestamptz, %s::timestamptz)",
                )
            insert_bad(cur, "traffic", bad, processed_at)
            log_batch(cur, "traffic_raw", batch_id, len(good), len(inserted), len(bad))
            return len(inserted)

    new = with_retry(write, f"traffic_raw batch {batch_id}")
    log.info("[traffic_raw] batch %d: %d valid, %d new, %d duplicates, %d invalid",
             batch_id, len(good), new, len(good) - new, len(bad))


# ------------------------------------------------------------------ sink 2: traffic windows
def write_traffic_windows(batch_df, batch_id):
    """Upsert the per-sensor, per-minute aggregates (update mode sends changed windows only)."""
    rows = batch_df.collect()
    data = [(r.window_start, r.window_end, r.sensor_id, r.reading_count, r.vehicle_count, r.avg_speed_kmh,
             r.min_speed_kmh, r.avg_occupancy_pct, r.avg_arrival_delay_s, r.late_event_count) for r in rows]

    def write():
        """The database work, wrapped so with_retry() can run it again."""
        with pg_connect() as conn, conn.cursor() as cur:
            if data:
                execute_values(
                    cur,
                    """INSERT INTO stream.traffic_per_minute (window_start, window_end, sensor_id, reading_count,
                           vehicle_count, avg_speed_kmh, min_speed_kmh, avg_occupancy_pct,
                           avg_arrival_delay_s, late_event_count)
                       VALUES %s
                       ON CONFLICT (sensor_id, window_start) DO UPDATE SET
                           reading_count = EXCLUDED.reading_count,
                           vehicle_count = EXCLUDED.vehicle_count,
                           avg_speed_kmh = EXCLUDED.avg_speed_kmh,
                           min_speed_kmh = EXCLUDED.min_speed_kmh,
                           avg_occupancy_pct = EXCLUDED.avg_occupancy_pct,
                           avg_arrival_delay_s = EXCLUDED.avg_arrival_delay_s,
                           late_event_count = EXCLUDED.late_event_count,
                           updated_at = now()""",
                    data, page_size=5000,
                )
            log_batch(cur, "traffic_windows", batch_id, len(data), len(data))

    with_retry(write, f"traffic_windows batch {batch_id}")
    log.info("[traffic_windows] batch %d: %d windows updated", batch_id, len(data))


# ------------------------------------------------------------------ sink 3: transit events
def write_transit_raw(batch_df, batch_id):
    """Called by Spark once per micro-batch for the transit_raw query."""
    processed_at = datetime.now(timezone.utc)
    rows = batch_df.collect()
    good = [r for r in rows if r.error_reason is None]
    bad = [r for r in rows if r.error_reason is not None]

    def write():
        """The database work, wrapped so with_retry() can run it again."""
        with pg_connect() as conn, conn.cursor() as cur:
            inserted = []
            if good:
                inserted = execute_values(
                    cur,
                    """INSERT INTO raw.transit_events (event_id, schema_version, line_id, vehicle_id, trip_id,
                           stop_id, event_type, scheduled_time, event_time, delay_s, passengers,
                           kafka_time, processed_at, kafka_partition, kafka_offset)
                       VALUES %s
                       ON CONFLICT (event_id) DO NOTHING
                       RETURNING event_id""",
                    [(r.event_id, r.schema_version, r.line_id, r.vehicle_id, r.trip_id, r.stop_id, r.event_type,
                      r.scheduled_ts, r.event_time, r.delay_s, r.passengers, r.kafka_time, processed_at,
                      r.kafka_partition, r.kafka_offset) for r in good],
                    fetch=True, page_size=5000,
                )
            insert_bad(cur, "transit", bad, processed_at)
            log_batch(cur, "transit_raw", batch_id, len(good), len(inserted), len(bad))
            return len(inserted)

    new = with_retry(write, f"transit_raw batch {batch_id}")
    log.info("[transit_raw] batch %d: %d valid, %d new, %d duplicates, %d invalid",
             batch_id, len(good), new, len(good) - new, len(bad))


# ------------------------------------------------------------------ sink 4: transit windows
def write_transit_windows(batch_df, batch_id):
    """Upsert the per-line, 5-minute aggregates."""
    rows = batch_df.collect()
    data = [(r.window_start, r.window_end, r.line_id, r.stop_events, r.avg_delay_s, r.max_delay_s,
             r.on_time_count, r.avg_passengers, r.late_event_count) for r in rows]

    def write():
        """The database work, wrapped so with_retry() can run it again."""
        with pg_connect() as conn, conn.cursor() as cur:
            if data:
                execute_values(
                    cur,
                    """INSERT INTO stream.transit_per_5min (window_start, window_end, line_id, stop_events,
                           avg_delay_s, max_delay_s, on_time_count, avg_passengers, late_event_count)
                       VALUES %s
                       ON CONFLICT (line_id, window_start) DO UPDATE SET
                           stop_events = EXCLUDED.stop_events,
                           avg_delay_s = EXCLUDED.avg_delay_s,
                           max_delay_s = EXCLUDED.max_delay_s,
                           on_time_count = EXCLUDED.on_time_count,
                           avg_passengers = EXCLUDED.avg_passengers,
                           late_event_count = EXCLUDED.late_event_count,
                           updated_at = now()""",
                    data,
                )
            log_batch(cur, "transit_windows", batch_id, len(data), len(data))

    with_retry(write, f"transit_windows batch {batch_id}")
    log.info("[transit_windows] batch %d: %d windows updated", batch_id, len(data))


# ------------------------------------------------------------------ monitoring
def _offsets(value):
    """Kafka offsets in a progress report: {"traffic.readings": {"0": 123, "1": 456}}"""
    if isinstance(value, str):
        value = json.loads(value)
    return value or {}


def kafka_lag(progress):
    """Messages in Kafka that this query has not read yet = latest offset - read offset."""
    try:
        src = progress["sources"][0]
        latest, end = _offsets(src.get("latestOffset")), _offsets(src.get("endOffset"))
        return sum(
            int(latest[topic][p]) - int(end.get(topic, {}).get(p, 0))
            for topic in latest for p in latest[topic]
        )
    except Exception:
        return None


def save_progress(queries, seen):
    """Save new progress reports of each query to stream_progress."""
    rows = {}
    for q in queries:
        for p in q.recentProgress:
            if (p["name"], p["runId"], p["timestamp"]) in seen:
                continue
            seen.add((p["name"], p["runId"], p["timestamp"]))
            # When idle, Spark also sends reports for the next batch id, so the
            # same key can appear twice: keep the latest report.
            rows[(p["name"], p["runId"], p["batchId"])] = (
                p["name"], p["runId"], p["batchId"], p["timestamp"], p["numInputRows"],
                p.get("inputRowsPerSecond"), p.get("processedRowsPerSecond"),
                p.get("durationMs", {}).get("triggerExecution"), kafka_lag(p),
            )
    if not rows:
        return

    def write():
        """The database work, wrapped so with_retry() can run it again."""
        with pg_connect() as conn, conn.cursor() as cur:
            execute_values(
                cur,
                """INSERT INTO monitoring.stream_progress AS sp (query_name, run_id, batch_id, batch_timestamp,
                       num_input_rows, input_rows_per_second, processed_rows_per_second,
                       batch_duration_ms, kafka_lag)
                   VALUES %s
                   ON CONFLICT (query_name, run_id, batch_id) DO UPDATE SET
                       batch_timestamp = EXCLUDED.batch_timestamp,
                       num_input_rows = GREATEST(sp.num_input_rows, EXCLUDED.num_input_rows),
                       input_rows_per_second = EXCLUDED.input_rows_per_second,
                       processed_rows_per_second = EXCLUDED.processed_rows_per_second,
                       batch_duration_ms = EXCLUDED.batch_duration_ms,
                       kafka_lag = EXCLUDED.kafka_lag""",
                list(rows.values()),
            )
    try:
        with_retry(write, "save progress")
    except Exception as e:
        log.error("could not save progress: %s", e)


def read_topic(spark, topic):
    """
    Streaming DataFrame on one Kafka topic.
    startingOffsets=earliest is only used the very first time; after that Spark
    uses the offsets saved in the checkpoint.
    failOnDataLoss=true: if offsets we still needed were deleted from Kafka,
    stop with an error instead of silently skipping events.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "true")
        .load()
        .select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("timestamp").alias("kafka_time"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
    )


def start_query(df, name, writer, output_mode="append"):
    """Start one streaming query with its own checkpoint folder."""
    return (
        df.writeStream.queryName(name)
        .outputMode(output_mode)
        .foreachBatch(writer)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{name}")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


# ------------------------------------------------------------------ main
def main():
    """Start Spark, start the four streaming queries and monitor them."""
    spark = (
        SparkSession.builder.appName("smart-city-mobility-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")   # small data -> few partitions
        # RocksDB keeps the dedup/window state on disk instead of in memory
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    for name in QUERY_NAMES:
        last = last_committed_batch(name)
        if last is None:
            log_pipeline_event("fresh_start", name, details="no checkpoint found, reading Kafka from earliest offset")
        else:
            log_pipeline_event("restart_from_checkpoint", name, details=f"resuming after committed batch {last}")

    sensors, lines, stops = load_reference_ids()

    # Parse + data-quality rules (one DataFrame per topic)
    traffic = validate_traffic(parse_traffic(read_topic(spark, TRAFFIC_TOPIC)), sensors)
    transit = validate_transit(parse_transit(read_topic(spark, TRANSIT_TOPIC)), lines, stops)

    # Raw queries: no watermark, we keep EVERY valid event (even very late ones)
    # Window queries: event-time windows on valid events only, update mode = changed windows only
    queries = [
        start_query(traffic, "traffic_raw", write_traffic_raw),
        start_query(traffic_per_window(traffic.where(F.col("error_reason").isNull()), WATERMARK),
                    "traffic_windows", write_traffic_windows, "update"),
        start_query(transit, "transit_raw", write_transit_raw),
        start_query(transit_per_window(transit.where(F.col("error_reason").isNull()), WATERMARK),
                    "transit_windows", write_transit_windows, "update"),
    ]
    log.info("streaming queries started: %s", ", ".join(q.name for q in queries))

    # Every 10 s: save progress. If a query fails, log it and exit with an error
    # so Docker restarts the container (it then resumes from the checkpoint).
    seen = set()
    try:
        while not spark.streams.awaitAnyTermination(10):
            save_progress(spark.streams.active, seen)
    except Exception as e:
        log_pipeline_event("query_failed", details=str(e)[:2000])
        raise


if __name__ == "__main__":
    main()
