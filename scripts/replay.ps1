# Replay: wipe Spark's checkpoints and the tables Spark writes, then let Spark
# re-read both Kafka topics from the beginning (Kafka keeps 7 days of events).
# Run from the project folder:  .\scripts\replay.ps1
$ErrorActionPreference = "Stop"

Write-Host "Stopping Spark..."
docker compose stop spark

Write-Host "Clearing the tables written by Spark..."
$sql = "TRUNCATE raw.traffic_readings, raw.transit_events, raw.bad_events, " +
       "stream.traffic_per_minute, stream.transit_per_5min, stream.sensor_last_seen, monitoring.batch_log; " +
       "INSERT INTO monitoring.pipeline_events (event_type, details) VALUES ('replay_requested', 'tables and checkpoints cleared');"
$sql | docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U $POSTGRES_USER -d $POSTGRES_DB'

Write-Host "Deleting Spark checkpoints..."
docker compose run --rm --no-deps --entrypoint sh spark -c "rm -rf /opt/checkpoints/*"

Write-Host "Starting Spark again (it will read Kafka from the earliest offset)..."
docker compose up -d spark
