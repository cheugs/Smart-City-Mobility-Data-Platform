-- Runs automatically the first time the PostGIS container starts.
--
-- One schema per layer, so raw and transformed data never mix:
--   city.*        reference data of the sample city (geometries, SRID 4326)
--   raw.*         every valid event exactly once + rejected events (written by Spark)
--   stream.*      windowed aggregates computed by Spark Structured Streaming
--   monitoring.*  pipeline health: batches, progress, restarts, incidents, data-quality checks
--   analytics.*   tables built by the Airflow DAG (geospatial joins + business marts)

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS city;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stream;
CREATE SCHEMA IF NOT EXISTS monitoring;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ================================================================ city (reference data)
CREATE TABLE IF NOT EXISTS city.districts (
    district_id  TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    geom         geometry(Polygon, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS city.road_segments (
    segment_id       TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    road_class       TEXT NOT NULL CHECK (road_class IN ('boulevard', 'arterial', 'local')),
    speed_limit_kmh  INT  NOT NULL CHECK (speed_limit_kmh > 0),
    lanes            INT  NOT NULL,
    length_m         INT  NOT NULL,
    geom             geometry(LineString, 4326) NOT NULL
);

-- A sensor only has a position. Which road it measures is found by PostGIS
-- (nearest road segment), see airflow/sql/marts/01_sensor_context.sql.
CREATE TABLE IF NOT EXISTS city.sensors (
    sensor_id            TEXT PRIMARY KEY,
    expected_interval_s  INT NOT NULL DEFAULT 10,   -- a reading is expected every 10 s
    geom                 geometry(Point, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS city.transit_lines (
    line_id      TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    mode         TEXT NOT NULL CHECK (mode IN ('bus', 'tram')),
    headway_min  INT  NOT NULL,
    color        TEXT
);

CREATE TABLE IF NOT EXISTS city.transit_stops (
    stop_id        TEXT PRIMARY KEY,
    line_id        TEXT NOT NULL REFERENCES city.transit_lines (line_id),
    stop_sequence  INT  NOT NULL,
    name           TEXT NOT NULL,
    geom           geometry(Point, 4326) NOT NULL
);

-- Spatial (GiST) indexes: used by the nearest-road search (<-> operator),
-- ST_Contains (sensor in district) and ST_DWithin (stop near a sensor).
CREATE INDEX IF NOT EXISTS idx_roads_geom     ON city.road_segments USING gist (geom);
CREATE INDEX IF NOT EXISTS idx_districts_geom ON city.districts     USING gist (geom);
CREATE INDEX IF NOT EXISTS idx_sensors_geom   ON city.sensors       USING gist (geom);
CREATE INDEX IF NOT EXISTS idx_stops_geom     ON city.transit_stops USING gist (geom);

-- ================================================================ raw (written by Spark)
-- Every valid sensor reading, exactly once (event_id is the primary key -> no duplicates).
CREATE TABLE IF NOT EXISTS raw.traffic_readings (
    event_id             TEXT PRIMARY KEY,
    schema_version       INT  NOT NULL,
    sensor_id            TEXT NOT NULL REFERENCES city.sensors (sensor_id),
    vehicle_count        INT  NOT NULL CHECK (vehicle_count >= 0),
    avg_speed_kmh        DOUBLE PRECISION NOT NULL CHECK (avg_speed_kmh BETWEEN 0 AND 200),
    occupancy_pct        DOUBLE PRECISION NOT NULL CHECK (occupancy_pct BETWEEN 0 AND 100),
    heavy_vehicle_count  INT,        -- added in schema v2 (NULL for v1 readings)
    firmware             TEXT,       -- added in schema v2 (NULL for v1 readings)
    event_time           TIMESTAMPTZ NOT NULL,   -- when the sensor measured
    kafka_time           TIMESTAMPTZ NOT NULL,   -- when it reached Kafka
    processed_at         TIMESTAMPTZ NOT NULL,   -- when Spark wrote it here
    kafka_partition      INT    NOT NULL,
    kafka_offset         BIGINT NOT NULL
);
-- Why these indexes:
--   event_time         : dashboard and marts filter on event-time ranges
--   sensor + time      : per-sensor history and the completeness checks
--   processed_at       : "latency over the last 5 minutes" only scans recent rows
CREATE INDEX IF NOT EXISTS idx_traffic_event_time   ON raw.traffic_readings (event_time);
CREATE INDEX IF NOT EXISTS idx_traffic_sensor_time  ON raw.traffic_readings (sensor_id, event_time);
CREATE INDEX IF NOT EXISTS idx_traffic_processed_at ON raw.traffic_readings (processed_at);

-- Every valid bus / tram stop event, exactly once.
CREATE TABLE IF NOT EXISTS raw.transit_events (
    event_id         TEXT PRIMARY KEY,
    schema_version   INT  NOT NULL,
    line_id          TEXT NOT NULL REFERENCES city.transit_lines (line_id),
    vehicle_id       TEXT,
    trip_id          TEXT,
    stop_id          TEXT NOT NULL REFERENCES city.transit_stops (stop_id),
    event_type       TEXT NOT NULL CHECK (event_type IN ('ARRIVAL', 'DEPARTURE')),
    scheduled_time   TIMESTAMPTZ NOT NULL,   -- timetable
    event_time       TIMESTAMPTZ NOT NULL,   -- real arrival time
    delay_s          INT NOT NULL,           -- event_time - scheduled_time (negative = early)
    passengers       INT NOT NULL CHECK (passengers BETWEEN 0 AND 250),
    kafka_time       TIMESTAMPTZ NOT NULL,
    processed_at     TIMESTAMPTZ NOT NULL,
    kafka_partition  INT    NOT NULL,
    kafka_offset     BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transit_event_time ON raw.transit_events (event_time);
CREATE INDEX IF NOT EXISTS idx_transit_line_time  ON raw.transit_events (line_id, event_time);
CREATE INDEX IF NOT EXISTS idx_transit_stop_time  ON raw.transit_events (stop_id, event_time);

-- Messages that failed parsing or a data-quality rule (a "dead letter" table).
-- Key = Kafka position, so a re-processed batch cannot insert them twice.
CREATE TABLE IF NOT EXISTS raw.bad_events (
    source           TEXT NOT NULL CHECK (source IN ('traffic', 'transit')),
    kafka_partition  INT,
    kafka_offset     BIGINT,
    event_id         TEXT,
    error_reason     TEXT NOT NULL,
    raw_value        TEXT,
    processed_at     TIMESTAMPTZ,
    PRIMARY KEY (source, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS idx_bad_processed_at ON raw.bad_events (processed_at);

-- ================================================================ stream (Spark windows)
-- Per sensor and per 1-minute EVENT-TIME window.
CREATE TABLE IF NOT EXISTS stream.traffic_per_minute (
    window_start         TIMESTAMPTZ NOT NULL,
    window_end           TIMESTAMPTZ NOT NULL,
    sensor_id            TEXT NOT NULL,
    reading_count        INT  NOT NULL,     -- 6 expected per minute (one every 10 s)
    vehicle_count        INT  NOT NULL,
    avg_speed_kmh        DOUBLE PRECISION,
    min_speed_kmh        DOUBLE PRECISION,
    avg_occupancy_pct    DOUBLE PRECISION,
    avg_arrival_delay_s  DOUBLE PRECISION,
    late_event_count     INT,
    updated_at           TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (sensor_id, window_start)
);
CREATE INDEX IF NOT EXISTS idx_tpm_window ON stream.traffic_per_minute (window_start);

-- Per line and per 5-minute EVENT-TIME window.
CREATE TABLE IF NOT EXISTS stream.transit_per_5min (
    window_start      TIMESTAMPTZ NOT NULL,
    window_end        TIMESTAMPTZ NOT NULL,
    line_id           TEXT NOT NULL,
    stop_events       INT  NOT NULL,
    avg_delay_s       DOUBLE PRECISION,
    max_delay_s       INT,
    on_time_count     INT,
    avg_passengers    DOUBLE PRECISION,
    late_event_count  INT,
    updated_at        TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (line_id, window_start)
);
CREATE INDEX IF NOT EXISTS idx_t5_window ON stream.transit_per_5min (window_start);

-- Missing-sensor detection, step 1: when did we last hear from each sensor?
CREATE TABLE IF NOT EXISTS stream.sensor_last_seen (
    sensor_id        TEXT PRIMARY KEY,
    last_event_time  TIMESTAMPTZ NOT NULL,
    last_kafka_time  TIMESTAMPTZ NOT NULL
);

-- ================================================================ monitoring
-- One row per Spark micro-batch, so we can SEE duplicates being skipped.
CREATE TABLE IF NOT EXISTS monitoring.batch_log (
    query_name          TEXT,
    batch_id            BIGINT,
    rows_received       INT,
    rows_inserted       INT,
    duplicates_skipped  INT,
    bad_rows            INT,
    logged_at           TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (query_name, batch_id)
);
CREATE INDEX IF NOT EXISTS idx_batch_log_time ON monitoring.batch_log (logged_at);

-- Spark progress reports: throughput, batch duration and Kafka consumer lag.
-- run_id changes every time the job restarts -> restarts are visible.
CREATE TABLE IF NOT EXISTS monitoring.stream_progress (
    query_name                 TEXT,
    run_id                     TEXT,
    batch_id                   BIGINT,
    batch_timestamp            TIMESTAMPTZ,
    num_input_rows             BIGINT,
    input_rows_per_second      DOUBLE PRECISION,
    processed_rows_per_second  DOUBLE PRECISION,
    batch_duration_ms          BIGINT,
    kafka_lag                  BIGINT,     -- messages in Kafka not read yet
    PRIMARY KEY (query_name, run_id, batch_id)
);
CREATE INDEX IF NOT EXISTS idx_progress_time ON monitoring.stream_progress (batch_timestamp);

-- Start / restart / failure / replay events of the pipeline.
CREATE TABLE IF NOT EXISTS monitoring.pipeline_events (
    id          SERIAL PRIMARY KEY,
    logged_at   TIMESTAMPTZ DEFAULT now(),
    event_type  TEXT NOT NULL,
    query_name  TEXT,
    run_id      TEXT,
    details     TEXT
);

-- Missing-sensor detection, step 2: one row per outage (opened and closed by Airflow).
CREATE TABLE IF NOT EXISTS monitoring.sensor_incidents (
    incident_id    SERIAL PRIMARY KEY,
    sensor_id      TEXT NOT NULL REFERENCES city.sensors (sensor_id),
    opened_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at   TIMESTAMPTZ,              -- last reading before the outage
    closed_at      TIMESTAMPTZ,              -- NULL = still silent
    details        TEXT
);
-- At most ONE open incident per sensor (the DAG can run twice without duplicates).
CREATE UNIQUE INDEX IF NOT EXISTS uq_open_incident ON monitoring.sensor_incidents (sensor_id) WHERE closed_at IS NULL;

-- Results of the automatic data-quality checks run by Airflow.
CREATE TABLE IF NOT EXISTS monitoring.dq_results (
    id           SERIAL PRIMARY KEY,
    checked_at   TIMESTAMPTZ DEFAULT now(),
    check_name   TEXT NOT NULL,
    failed_rows  BIGINT NOT NULL,
    passed       BOOLEAN NOT NULL
);

-- ================================================================ live sensor status
-- Missing-sensor detection, live view: compares the last reading with the clock.
--   OK          last reading less than 60 s ago
--   DELAYED     60 s - 5 min (a few readings missing)
--   SILENT      more than 5 min: the sensor is considered missing
--   NEVER_SEEN  no reading at all since the pipeline started
CREATE OR REPLACE VIEW monitoring.sensor_health AS
SELECT s.sensor_id,
       l.last_event_time,
       round(extract(epoch FROM now() - l.last_event_time)::numeric, 0) AS seconds_since_last,
       CASE
           WHEN l.last_event_time IS NULL THEN 'NEVER_SEEN'
           WHEN now() - l.last_event_time < interval '60 seconds' THEN 'OK'
           WHEN now() - l.last_event_time < interval '5 minutes' THEN 'DELAYED'
           ELSE 'SILENT'
       END AS status
FROM city.sensors s
LEFT JOIN stream.sensor_last_seen l USING (sensor_id);
