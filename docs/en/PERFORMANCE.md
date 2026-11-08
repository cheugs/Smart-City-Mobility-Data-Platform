# Performance and load tests

[Français](../PERFORMANCE.md) · **English**

## How the measurements are made

`python scripts/run_benchmark.py N`:
1. stops the normal producer, so only benchmark events are measured;
2. sends N **valid** traffic readings as fast as possible (`producer/benchmark_load.py`), spread over the 60 sensors;
3. waits until all N readings are in `raw.traffic_readings`, then measures;
4. records `docker stats` every 5 seconds (CPU and memory of each container);
5. restarts the producer and saves everything in `benchmarks/benchmark_N.json`.

All four Spark queries run during the benchmark: each reading is read twice (raw table + windows) and goes
through the whole real pipeline (validation, deduplication, watermark, upserts).

**Machine:** laptop, Docker Desktop (WSL 2) with 12 CPUs and 8 GB of memory, Spark in `local[4]`,
`maxOffsetsPerTrigger = 20000`, trigger every 5 s. Airflow, Grafana and the dashboard run at the same time.

## Results

| | 10,000 readings | 100,000 readings | 1,000,000 readings |
|---|---|---|---|
| Stored / lost / duplicated | 10,000 / **0** / **0** | 100,000 / **0** / **0** | 1,000,000 / **0** / **0** |
| Producer → Kafka | 19,120 ev/s | 19,811 ev/s | 14,236 ev/s |
| End-to-end duration (first message in Kafka → last row stored) | 0.9 s | 55.8 s | 786.5 s (13 min) |
| End-to-end throughput | 11,142 ev/s (a single micro-batch) | **1,792 ev/s** | **1,271 ev/s** |
| Spark throughput (average / max per batch, `traffic_raw` query) | 1,744 / 1,744 ev/s | 1,697 / 2,760 ev/s | 1,783 / 4,821 ev/s |
| Micro-batches | 1 | 6 | 51 |
| Maximum Kafka lag | 0 | 66,025 messages | 920,007 messages |
| Kafka → PostGIS latency (p50 / p95 / max) | 0.66 / 0.86 / 0.90 s | 17.8 / 50.8 / 51.0 s | 406 / 696 / 716 s |

**Resources during the 1-million benchmark** (average / maximum):

| Container | Avg CPU | Max CPU | Max memory |
|-----------|---------|---------|------------|
| Spark | 117 % | 525 % | 1,691 MB |
| Kafka | 63 % | 282 % | 1,060 MB |
| PostGIS | 50 % | 257 % | 686 MB |
| Airflow | 16 % | 236 % (during a DAG run) | 1,292 MB |
| Grafana | 0.5 % | 6 % | 66 MB |
| Dashboard | 0.5 % | 5 % | 43 MB |

(100 % = one core. Per-benchmark details are in the JSON files.)

**Under normal load** (60 sensors, one reading every 10 s, about 6.5 events/s with buses and trams),
the latency from Kafka to PostGIS is about **4.3 s (median)** and 22 s (p95).

**24-hour history at start-up:** the 505,000 events simulated on the first start are processed by Spark
in 8 to 10 minutes, alongside the live data.

## Reading the results

- **No loss, no duplicates**, even with one million events and a Kafka lag of 920,000 messages.
- **10,000 readings** fit in a single micro-batch (under 20,000 messages): everything is stored in under a second.
- **Beyond that**, throughput settles around **1,300 to 1,800 events per second** end to end.
  The producer sends 10 times faster: messages wait in Kafka, which is its job (a durable buffer).
- During a spike, the high latency is almost entirely **waiting time in Kafka**, not processing time:
  each 20,000-message micro-batch takes 4 to 15 seconds.
- The pipeline has about **200 times** more capacity than the normal load of the city (6.5 events/s).

## Bottlenecks

1. **Each reading is read and parsed twice.** The raw query and the window query read the same topic
   independently (two consumers, two checkpoints). This is on purpose (the raw table keeps even events beyond
   the watermark), but it doubles the reading and JSON parsing work.
2. **Collecting on the driver.** `foreachBatch` does `collect()` then psycopg2 inserts (`execute_values`):
   everything goes through one Python process. PostGIS goes up to 2.5 cores during the inserts.
3. **Deduplication and window state.** `dropDuplicates` + windows keep state (RocksDB) per `event_id`
   and per (sensor, minute) during the watermark: expensive during a one-million-event spike.
4. **Sharing 4 cores.** The four queries and Spark's 4 threads share `local[4]`, on the same machine as
   Kafka, PostGIS and Airflow.
5. **Batch size.** `maxOffsetsPerTrigger = 20000` caps each batch, which keeps latency reasonable but limits
   throughput while catching up.

## Possible improvements

- Read the topic once, with one `foreachBatch` that writes the raw table and computes the aggregates (at the cost of handling the watermark by hand).
- Distributed writes: each partition writes into a staging table (JDBC or `COPY`), then a `MERGE` in PostGIS.
- More Kafka partitions and a real Spark cluster (several executors).
- Avro instead of JSON to reduce parsing cost.
- Raise `maxOffsetsPerTrigger` while catching up (higher throughput, longer batches).
