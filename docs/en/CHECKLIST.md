# Quality & Acceptance Checklist (DE02)

[Français](../CHECKLIST.md) · **English**

The original checklist talks about *financial transactions*. This project processes **mobility events**
(traffic sensor readings and bus and tram stop events): each criterion is applied to these events, and
section 8 ("financial analytics") is adapted to mobility analytics. Every line points to its proof: the
code, the automated test or the measurement.

<img src="../assets/icons/check.svg" width="14"> = verified by an automated test or a documented measurement ·
<img src="../assets/icons/partial.svg" width="14"> = covered, with an explained limitation

Abbreviations: **U** = unit test (`tests/unit`), **I** = integration test (`tests/integration`),
**E2E** = end-to-end test on the Docker stack (`tests/e2e`), **AF** = Airflow quality check
([data_quality_checks.sql](../../airflow/sql/checks/data_quality_checks.sql)).

## 1. Functional correctness

| | Criterion | Proof |
|---|---|---|
| <img src="../assets/icons/check.svg" width="14"> | Events are generated with realistic attributes | Rush hours, speed/density relation, incidents, bus delays ([generator.py](../../producer/generator.py)) · U `test_rush_hour_is_slower_and_busier_than_the_night`, `test_transit_vehicles_follow_the_timetable_with_a_delay` |
| <img src="../assets/icons/check.svg" width="14"> | Every event has a unique identifier | UUID v4 + primary key `event_id` · U `test_readings_have_unique_ids_and_valid_values` · AF `no_duplicate_*_event_ids` |
| <img src="../assets/icons/check.svg" width="14"> | Measurements, units and timestamps are valid | Validation rules + `CHECK` constraints · U `test_each_traffic_rule`, `test_each_transit_rule` · AF `measurements_in_valid_range` |
| <img src="../assets/icons/check.svg" width="14"> | Events are ingested through Kafka | 2 topics created by `kafka-init` · E2E `test_valid_readings_are_ingested_with_their_kafka_position` |
| <img src="../assets/icons/check.svg" width="14"> | Spark processes incoming events continuously | 4 Structured Streaming queries, trigger every 5 s ([streaming_job.py](../../spark/streaming_job.py)) · E2E `test_monitoring_tables_show_progress_lag_and_starts` |
| <img src="../assets/icons/check.svg" width="14"> | Aggregations are mathematically correct | U `test_counts_sums_and_averages_per_sensor_and_minute`, `test_transit_windows_delay_and_on_time` |
| <img src="../assets/icons/check.svg" width="14"> | PostgreSQL stores the expected results | E2E `test_duplicates_are_stored_once_and_do_not_inflate_windows`, `test_transit_delay_is_computed_and_stored` |
| <img src="../assets/icons/check.svg" width="14"> | Dashboard values match the database | E2E `test_dashboard_figures_match_the_database` (same database snapshot + HTTP API) |

## 2. Data quality and integrity

| | Criterion | Proof |
|---|---|---|
| <img src="../assets/icons/check.svg" width="14"> | Missing mandatory fields are detected | `missing_event_id`, `missing_sensor_id`, `missing_event_time`, `missing_measurement` · U `test_each_traffic_rule` |
| <img src="../assets/icons/check.svg" width="14"> | Invalid values are rejected (equivalent of invalid amounts) | Negative vehicles, speed out of range, occupancy > 100 %, passengers out of range · U + E2E `test_invalid_records_are_isolated_with_their_reason` |
| <img src="../assets/icons/check.svg" width="14"> | Invalid timestamps are handled | `invalid_event_time`, `event_time_in_future`, `invalid_scheduled_time` · U + E2E |
| <img src="../assets/icons/check.svg" width="14"> | Duplicate IDs are detected | `ON CONFLICT ... RETURNING` counts duplicates in `monitoring.batch_log.duplicates_skipped` · E2E |
| <img src="../assets/icons/check.svg" width="14"> | Duplicates do not inflate aggregates | Stateful `dropDuplicates` · U `test_duplicate_event_ids_are_counted_once` · I `test_duplicates_across_batches_are_counted_once` · AF `windows_not_inflated_by_duplicates` |
| <img src="../assets/icons/check.svg" width="14"> | Invalid records are isolated from valid records | Table `raw.bad_events` with reason and Kafka position · I `test_malformed_and_invalid_records_are_isolated_and_stream_keeps_running` |
| <img src="../assets/icons/check.svg" width="14"> | Transformations preserve integrity | Foreign keys to `city.*`, constraints, AF `transit_delay_matches_timestamps`, `window_vehicle_totals_not_inflated` |
| <img src="../assets/icons/check.svg" width="14"> | Data quality rules are tested automatically | 30+ unit validation cases + 10 Airflow checks every 10 min (results in `monitoring.dq_results`) |

## 3. Real-time processing

| | Criterion | Proof |
|---|---|---|
| <img src="../assets/icons/check.svg" width="14"> | Events are processed without batch execution | Structured Streaming micro-batches every 5 s; median latency measured (README section 9) |
| <img src="../assets/icons/check.svg" width="14"> | Event time is distinguished from processing time | 3 stored timestamps (`event_time`, `kafka_time`, `processed_at`); windows on `event_time` |
| <img src="../assets/icons/check.svg" width="14"> | Out-of-order events are handled correctly | I `test_out_of_order_events_land_in_the_right_window` |
| <img src="../assets/icons/check.svg" width="14"> | Late events are handled with watermarks | I `test_late_event_within_watermark_updates_its_old_window`, `test_event_beyond_watermark_is_dropped_from_windows_but_kept_raw` · E2E `test_late_event_within_the_watermark_updates_its_window` |
| <img src="../assets/icons/check.svg" width="14"> | Window-based aggregations are accurate | U `test_aggregation.py` · E2E (window = number of distinct readings) |
| <img src="../assets/icons/check.svg" width="14"> | Processing latency is measurable | `processed_at - kafka_time`: p50/p95 in Grafana, the dashboard and the benchmarks |
| <img src="../assets/icons/check.svg" width="14"> | Throughput is measurable | `monitoring.stream_progress.processed_rows_per_second` + benchmarks |
| <img src="../assets/icons/check.svg" width="14"> | Kafka consumer lag is observable | `kafka_lag` computed at every batch (latest offset - read offset); Grafana panel |

## 4. Reliability and fault tolerance

| | Criterion | Proof |
|---|---|---|
| <img src="../assets/icons/check.svg" width="14"> | The pipeline recovers after restarts | E2E `test_spark_restart_loses_nothing_and_counts_nothing_twice` |
| <img src="../assets/icons/check.svg" width="14"> | Checkpoints preserve streaming progress | I `test_restart_from_checkpoint_keeps_state`; `restart_from_checkpoint` event in `monitoring.pipeline_events` |
| <img src="../assets/icons/check.svg" width="14"> | Failed processing attempts can be retried | `with_retry` (3 attempts) in Spark; `retries: 2` on every Airflow task; `restart: unless-stopped` · slow E2E `test_postgres_outage_is_survived` |
| <img src="../assets/icons/check.svg" width="14"> | Duplicate processing does not corrupt results | Idempotent upserts; `batch_log` in the same transaction · I + restart E2E |
| <img src="../assets/icons/check.svg" width="14"> | Historical events can be replayed | 7-day Kafka retention + [scripts/replay.ps1](../../scripts/replay.ps1) · slow E2E `test_full_replay_from_kafka_gives_back_the_same_events` |
| <img src="../assets/icons/check.svg" width="14"> | Recovery does not silently lose events | `failOnDataLoss=true`; exact counts in the restart tests and benchmarks (0 lost) |
| <img src="../assets/icons/check.svg" width="14"> | Schema changes do not break processing | v1, v2, no version and unknown v3 in the same stream · U, I `test_schema_evolution_mixed_versions_in_one_stream`, E2E · AF `v1_readings_have_no_v2_fields` |
| <img src="../assets/icons/check.svg" width="14"> | Delivery guarantees are documented and tested | Table in README section 6; duplicate, restart and replay tests |

## 5. Data architecture

| | Criterion | Proof |
|---|---|---|
| <img src="../assets/icons/check.svg" width="14"> | Components have clear responsibilities | Component table (README section 4) |
| <img src="../assets/icons/check.svg" width="14"> | Kafka topics and partitions are configured appropriately | 2 topics, 3 partitions, key `sensor_id` / `line_id` (order per sensor), 7-day retention, auto-creation disabled |
| <img src="../assets/icons/check.svg" width="14"> | Data flows are traceable | `kafka_partition` + `kafka_offset` on every raw row and reject; `batch_log`, `stream_progress` (`run_id`), `pipeline_events` |
| <img src="../assets/icons/check.svg" width="14"> | Raw and transformed data are separated | Schemas `city`, `raw`, `stream`, `monitoring`, `analytics` ([01_schema.sql](../../sql/01_schema.sql)) |
| <img src="../assets/icons/check.svg" width="14"> | PostgreSQL tables are properly structured | Primary keys, foreign keys, `CHECK` constraints, geometry types with SRID |
| <img src="../assets/icons/check.svg" width="14"> | Indexes are justified | Comments in `01_schema.sql`; GiST indexes for spatial joins; partial unique index for incidents |
| <img src="../assets/icons/partial.svg" width="14"> | Transformations (dbt in the checklist) are modular | dbt is not part of the requested stack (Kafka, Python, Spark, PostGIS, Airflow, Grafana). Equivalent: one SQL file per mart, one Airflow task per file, explicit dependencies ([mobility_analytics.py](../../airflow/dags/mobility_analytics.py)) · U `test_every_mart_file_is_used_by_the_dag` |
| <img src="../assets/icons/check.svg" width="14"> | The architecture supports extensions | New topic = new query; new mart = one SQL file; new city = `generate_city.py`; optional schema fields |

## 6. Performance and scalability

| | Criterion | Proof |
|---|---|---|
| <img src="../assets/icons/check.svg" width="14"> | The pipeline handles at least 10,000 events | [benchmarks/benchmark_10000.json](../../benchmarks/benchmark_10000.json) |
| <img src="../assets/icons/check.svg" width="14"> | The pipeline is tested with 100,000 events | [benchmarks/benchmark_100000.json](../../benchmarks/benchmark_100000.json) |
| <img src="../assets/icons/check.svg" width="14"> | A 1-million-event benchmark is attempted | [benchmarks/benchmark_1000000.json](../../benchmarks/benchmark_1000000.json) |
| <img src="../assets/icons/check.svg" width="14"> | Throughput is measured in events per second | Producer, Spark (average per batch) and end to end ([PERFORMANCE.md](PERFORMANCE.md)) |
| <img src="../assets/icons/check.svg" width="14"> | Processing latency is measured | p50, p95 and max per benchmark and under normal load |
| <img src="../assets/icons/check.svg" width="14"> | Resource consumption is monitored | `docker stats` every 5 s during benchmarks (CPU, memory per container) |
| <img src="../assets/icons/check.svg" width="14"> | Bottlenecks are identified | [PERFORMANCE.md](PERFORMANCE.md), "Bottlenecks" section |
| <img src="../assets/icons/check.svg" width="14"> | Results are documented | README section 9 + [PERFORMANCE.md](PERFORMANCE.md) |

## 7. Monitoring and observability

| | Criterion | Proof |
|---|---|---|
| <img src="../assets/icons/check.svg" width="14"> | Ingestion is monitored | Grafana "Events per second", "Throughput per streaming query", "Readings received vs expected" |
| <img src="../assets/icons/check.svg" width="14"> | Processing failures are logged | Python logs of each container; `query_failed` in `monitoring.pipeline_events`; `FAIL` checks in `monitoring.dq_results` |
| <img src="../assets/icons/check.svg" width="14"> | Kafka lag is monitored | Grafana panels + Pipeline page of the dashboard + health badge |
| <img src="../assets/icons/check.svg" width="14"> | Processing delays are monitored | p50/p95 latency per minute (Grafana); late events per minute |
| <img src="../assets/icons/check.svg" width="14"> | Duplicate events are counted | `monitoring.batch_log.duplicates_skipped` (Grafana + Quality page) |
| <img src="../assets/icons/check.svg" width="14"> | Invalid events are counted | `raw.bad_events` per reason (Grafana + Quality page) |
| <img src="../assets/icons/check.svg" width="14"> | Recovery events are traceable | `fresh_start`, `restart_from_checkpoint`, `replay_requested` + new `run_id` |
| <img src="../assets/icons/check.svg" width="14"> | Logs give enough information to investigate | Each batch logs received / new / duplicates / invalid; each reject keeps its raw message and Kafka position |

## 8. Mobility analytics (adaptation of "financial analytics")

| | Original criterion → adapted criterion | Proof |
|---|---|---|
| <img src="../assets/icons/check.svg" width="14"> | Transaction volumes → vehicle and stop-event volumes | "Vehicles counted" KPI, charts per road type, stop events per line |
| <img src="../assets/icons/check.svg" width="14"> | Total amounts → totals per segment, district and line | `analytics.traffic_segment_15min`, `traffic_district_hourly`, `transit_line_hourly` |
| <img src="../assets/icons/check.svg" width="14"> | Distribution by status → by congestion level, sensor status, punctuality | free flow / dense / congested; OK / DELAYED / SILENT; on time / late |
| <img src="../assets/icons/check.svg" width="14"> | Volumes by time window | `stream.traffic_per_minute`, `stream.transit_per_5min`; hourly profile |
| <img src="../assets/icons/check.svg" width="14"> | Currency differences → differences between road types | **Relative speed** (speed / limit) to compare a 30 km/h street and a 70 km/h boulevard; never a raw average across classes |
| <img src="../assets/icons/check.svg" width="14"> | Simulated suspicious-activity indicators → simulated anomaly indicators | Simulated incidents (slowed road), silent sensors, impossible values, congested segments |
| <img src="../assets/icons/check.svg" width="14"> | Indicators have explainable rules | Documented thresholds (README section 7), visible in the SQL |
| <img src="../assets/icons/check.svg" width="14"> | No claim of real detection | Disclaimer at the top of the README and on the dashboard: synthetic data, no measurement or prediction of real traffic |

## 9. Testing and validation

| | Criterion | Proof |
|---|---|---|
| <img src="../assets/icons/check.svg" width="14"> | Unit tests pass | `docker compose run --rm tests` + CI |
| <img src="../assets/icons/check.svg" width="14"> | Integration tests pass | `tests/integration` (real streaming queries) + CI |
| <img src="../assets/icons/check.svg" width="14"> | End-to-end tests pass | `pytest tests/e2e -m "not slow"` + CI (full Docker stack) |
| <img src="../assets/icons/check.svg" width="14"> | Duplicate-event scenarios are tested | U, I, E2E |
| <img src="../assets/icons/check.svg" width="14"> | Late-event scenarios are tested | I (within and beyond the watermark), E2E |
| <img src="../assets/icons/check.svg" width="14"> | Out-of-order scenarios are tested | I |
| <img src="../assets/icons/check.svg" width="14"> | Application failure scenarios are tested | E2E Spark restart; slow E2E Postgres outage |
| <img src="../assets/icons/check.svg" width="14"> | Replay scenarios are tested | Slow E2E `test_full_replay_from_kafka_gives_back_the_same_events` |
| <img src="../assets/icons/check.svg" width="14"> | Schema evolution scenarios are tested | U, I, E2E |
| <img src="../assets/icons/check.svg" width="14"> | Dashboard figures are validated against the database | E2E `test_dashboard_figures_match_the_database` |

## 10. Engineering and professional quality

| | Criterion | Proof |
|---|---|---|
| <img src="../assets/icons/check.svg" width="14"> | Code is modular and maintainable | Spark logic separated from I/O; one SQL file per mart; dashboard queries shared between live and demo |
| <img src="../assets/icons/check.svg" width="14"> | Configuration is externalised | `.env` + environment variables ([.env.example](../../.env.example)) |
| <img src="../assets/icons/check.svg" width="14"> | No secrets on GitHub | `.env` in `.gitignore`; CI with random passwords; online demo with no database or password |
| <img src="../assets/icons/check.svg" width="14"> | Docker services are reproducible | Pinned image and package versions; one command |
| <img src="../assets/icons/check.svg" width="14"> | CI runs the relevant tests automatically | [.github/workflows/ci.yml](../../.github/workflows/ci.yml) |
| <img src="../assets/icons/check.svg" width="14"> | README explains the architecture | Section 4 (diagram, table, step-by-step flow) |
| <img src="../assets/icons/check.svg" width="14"> | Decisions and trade-offs are documented | README section 8 |
| <img src="../assets/icons/check.svg" width="14"> | Known limitations are acknowledged | README section 14 |
| <img src="../assets/icons/check.svg" width="14"> | Another developer can reproduce the project | README section 13; CI rebuilds everything from scratch on every push |
