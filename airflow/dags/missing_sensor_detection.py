"""
DAG missing_sensor_detection - every 5 minutes.

Missing-sensor detection works in three steps:
  1. Spark updates stream.sensor_last_seen with every reading it stores
  2. the view monitoring.sensor_health compares that time with the clock (OK / DELAYED / SILENT)
  3. THIS DAG turns the status into incidents with a start and an end:
       - a SILENT sensor without an open incident  -> open an incident
       - an open incident whose sensor is back OK   -> close it
     The unique index on open incidents makes a re-run harmless (no duplicates).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from common import pg_connect

OPEN_INCIDENTS = """
    INSERT INTO monitoring.sensor_incidents (sensor_id, last_seen_at, details)
    SELECT h.sensor_id, h.last_event_time,
           'no reading for ' || h.seconds_since_last || ' s'
    FROM monitoring.sensor_health h
    WHERE h.status = 'SILENT'    -- NEVER_SEEN is ignored: right after start-up nothing has arrived yet
    ON CONFLICT (sensor_id) WHERE closed_at IS NULL DO NOTHING
"""

CLOSE_INCIDENTS = """
    UPDATE monitoring.sensor_incidents i
    SET closed_at = now()
    FROM monitoring.sensor_health h
    WHERE i.sensor_id = h.sensor_id
      AND i.closed_at IS NULL
      AND h.status = 'OK'
"""


def detect():
    """Open new incidents and close the ones that are resolved."""
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(OPEN_INCIDENTS)
        opened = cur.rowcount
        cur.execute(CLOSE_INCIDENTS)
        closed = cur.rowcount
        cur.execute("SELECT count(*) FROM monitoring.sensor_incidents WHERE closed_at IS NULL")
        still_open = cur.fetchone()[0]
    print(f"incidents opened: {opened}, closed: {closed}, still open: {still_open}")


with DAG(
    dag_id="missing_sensor_detection",
    description="Open and close incidents for sensors that stopped sending data",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["mobility", "monitoring"],
) as dag:
    PythonOperator(task_id="detect_missing_sensors", python_callable=detect)
