"""
The Spark logic of the pipeline, kept separate from the Kafka/Postgres code
so it can be unit-tested with small DataFrames.

  parse_traffic() / parse_transit()        JSON text -> typed columns
  validate_traffic() / validate_transit()  adds error_reason (NULL = valid)
  traffic_per_window()                     per sensor, 1-minute event-time windows
  transit_per_window()                     per line, 5-minute event-time windows

Both window functions use a watermark (for late events) and drop duplicate
event_ids, so a message sent twice is only counted once.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# ------------------------------------------------------------------ rules
MAX_FUTURE_MINUTES = 5        # small clock differences are OK, bigger ones are not
MAX_VEHICLES_PER_READING = 300
MAX_SPEED_KMH = 200
MAX_PASSENGERS = 250
MAX_DELAY_S = 2 * 3600        # a delay of more than 2 hours is treated as a data error
ON_TIME_MIN_S, ON_TIME_MAX_S = -60, 180   # "on time" = at most 1 min early and 3 min late
LATE_THRESHOLD_S = 60         # "late event" = reached Kafka more than 60 s after event_time
EVENT_TYPES = ["ARRIVAL", "DEPARTURE"]

# The schema contains the fields from ALL versions.
# v1 readings simply have null for the v2 fields -> backward compatible.
# Unknown extra fields (from a future v3) are ignored -> forward compatible.
TRAFFIC_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("schema_version", IntegerType()),
    StructField("sensor_id", StringType()),
    StructField("event_time", StringType()),
    StructField("vehicle_count", IntegerType()),
    StructField("avg_speed_kmh", DoubleType()),
    StructField("occupancy_pct", DoubleType()),
    # added in schema version 2 (optional)
    StructField("heavy_vehicle_count", IntegerType()),
    StructField("firmware", StringType()),
])

TRANSIT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("schema_version", IntegerType()),
    StructField("line_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("trip_id", StringType()),
    StructField("stop_id", StringType()),
    StructField("event_type", StringType()),
    StructField("scheduled_time", StringType()),
    StructField("event_time", StringType()),
    StructField("passengers", IntegerType()),
])

KAFKA_COLUMNS = ["raw_value", "kafka_time", "kafka_partition", "kafka_offset"]


def _parse(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Input columns : raw_value (JSON string), kafka_time, kafka_partition, kafka_offset
    Output        : the same columns + all event fields, event_time as a timestamp.
    Malformed JSON gives a row where every event field is null.
    """
    return (
        df.withColumn("data", F.from_json("raw_value", schema))
        .select(*KAFKA_COLUMNS, "data.*")
        .withColumnRenamed("event_time", "event_time_text")
        .withColumn("event_time", F.to_timestamp("event_time_text"))
        # events without a version number come from the oldest producers = v1
        .withColumn("schema_version", F.coalesce(F.col("schema_version"), F.lit(1)))
    )


def parse_traffic(df: DataFrame) -> DataFrame:
    """Parse traffic sensor readings."""
    return _parse(df, TRAFFIC_SCHEMA)


def parse_transit(df: DataFrame) -> DataFrame:
    """Parse public transport events, and compute the delay against the timetable."""
    return (
        _parse(df, TRANSIT_SCHEMA)
        .withColumn("scheduled_ts", F.to_timestamp("scheduled_time"))
        .withColumn("delay_s", F.col("event_time").cast("long") - F.col("scheduled_ts").cast("long"))
    )


def _time_rules(c, when: Column) -> Column:
    """Shared rules on event_time, chained after the rules given in `when`."""
    return (
        when.when(c("event_time_text").isNull(), "missing_event_time")
        .when(c("event_time").isNull(), "invalid_event_time")
        .when(c("event_time") > F.current_timestamp() + F.expr(f"INTERVAL {MAX_FUTURE_MINUTES} MINUTES"),
              "event_time_in_future")
    )


def validate_traffic(df: DataFrame, known_sensors: list) -> DataFrame:
    """
    Adds error_reason: why the reading is invalid, or NULL if it is valid.
    The FIRST rule that fails is reported. known_sensors comes from the
    `sensors` table in PostGIS.
    """
    c = F.col
    first = (
        F.when(c("event_id").isNull() & c("sensor_id").isNull() & c("event_time_text").isNull(), "malformed_json")
        .when(c("event_id").isNull(), "missing_event_id")
        .when(c("sensor_id").isNull(), "missing_sensor_id")
        .when(~c("sensor_id").isin(known_sensors), "unknown_sensor")
    )
    error = (
        _time_rules(c, first)
        .when(c("vehicle_count").isNull() | c("avg_speed_kmh").isNull() | c("occupancy_pct").isNull(),
              "missing_measurement")
        .when(c("vehicle_count") < 0, "negative_vehicle_count")
        .when(c("vehicle_count") > MAX_VEHICLES_PER_READING, "vehicle_count_too_high")
        .when((c("avg_speed_kmh") < 0) | (c("avg_speed_kmh") > MAX_SPEED_KMH), "speed_out_of_range")
        .when((c("occupancy_pct") < 0) | (c("occupancy_pct") > 100), "occupancy_out_of_range")
    )
    return df.withColumn("error_reason", error)


def validate_transit(df: DataFrame, known_lines: list, known_stops: list) -> DataFrame:
    """Same idea for transit events. known_lines / known_stops come from PostGIS."""
    c = F.col
    first = (
        F.when(c("event_id").isNull() & c("line_id").isNull() & c("event_time_text").isNull(), "malformed_json")
        .when(c("event_id").isNull(), "missing_event_id")
        .when(c("line_id").isNull() | ~c("line_id").isin(known_lines), "unknown_line")
        .when(c("stop_id").isNull() | ~c("stop_id").isin(known_stops), "unknown_stop")
    )
    error = (
        _time_rules(c, first)
        .when(c("scheduled_ts").isNull(), "invalid_scheduled_time")
        .when(c("event_type").isNull() | ~c("event_type").isin(EVENT_TYPES), "invalid_event_type")
        .when(c("passengers").isNull() | (c("passengers") < 0) | (c("passengers") > MAX_PASSENGERS),
              "passengers_out_of_range")
        .when(F.abs(c("delay_s")) > MAX_DELAY_S, "delay_out_of_range")
    )
    return df.withColumn("error_reason", error)


def _with_arrival_delay(df: DataFrame) -> DataFrame:
    """How late did the event reach Kafka compared to when it happened? (seconds)"""
    return df.withColumn("arrival_delay_s",
                         F.col("kafka_time").cast("double") - F.col("event_time").cast("double"))


def traffic_per_window(valid: DataFrame, watermark: str = "10 minutes", window: str = "1 minute") -> DataFrame:
    """
    Traffic per sensor and per EVENT-TIME window.

    withWatermark  -> Spark keeps windows open for late events up to `watermark`
                      behind the newest event_time seen; older events are dropped.
    dropDuplicates -> the same event_id is only counted once. Including event_time
                      lets Spark clean up the dedup state after the watermark.
    """
    return (
        _with_arrival_delay(valid)
        .withWatermark("event_time", watermark)
        .dropDuplicates(["event_id", "event_time"])
        .groupBy(F.window("event_time", window), "sensor_id")
        .agg(
            F.count("*").alias("reading_count"),
            F.sum("vehicle_count").alias("vehicle_count"),
            F.round(F.avg("avg_speed_kmh"), 2).alias("avg_speed_kmh"),
            F.round(F.min("avg_speed_kmh"), 2).alias("min_speed_kmh"),
            F.round(F.avg("occupancy_pct"), 2).alias("avg_occupancy_pct"),
            F.round(F.avg("arrival_delay_s"), 2).alias("avg_arrival_delay_s"),
            F.sum(F.when(F.col("arrival_delay_s") > LATE_THRESHOLD_S, 1).otherwise(0)).alias("late_event_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "sensor_id", "reading_count", "vehicle_count", "avg_speed_kmh", "min_speed_kmh",
            "avg_occupancy_pct", "avg_arrival_delay_s", "late_event_count",
        )
    )


def transit_per_window(valid: DataFrame, watermark: str = "10 minutes", window: str = "5 minutes") -> DataFrame:
    """Public transport performance per line and per EVENT-TIME window."""
    on_time = F.col("delay_s").between(ON_TIME_MIN_S, ON_TIME_MAX_S)
    return (
        _with_arrival_delay(valid)
        .withWatermark("event_time", watermark)
        .dropDuplicates(["event_id", "event_time"])
        .groupBy(F.window("event_time", window), "line_id")
        .agg(
            F.count("*").alias("stop_events"),
            F.round(F.avg("delay_s"), 1).alias("avg_delay_s"),
            F.max("delay_s").alias("max_delay_s"),
            F.sum(F.when(on_time, 1).otherwise(0)).alias("on_time_count"),
            F.round(F.avg("passengers"), 1).alias("avg_passengers"),
            F.sum(F.when(F.col("arrival_delay_s") > LATE_THRESHOLD_S, 1).otherwise(0)).alias("late_event_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "line_id", "stop_events", "avg_delay_s", "max_delay_s", "on_time_count",
            "avg_passengers", "late_event_count",
        )
    )
