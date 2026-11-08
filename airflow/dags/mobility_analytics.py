"""
DAG mobility_analytics - every 10 minutes:

  1. geospatial joins   sensor -> nearest road + district, stop -> district + nearest sensor
  2. business marts     traffic per road / district / hour of day, transit per line / stop,
                        sensor completeness
  3. data-quality checks (results saved in monitoring.dq_results; any failure fails the run)

Spark does the real-time work; this DAG builds the slower "analytics" tables on top.
Each task is one SQL file in airflow/sql/, run in its own transaction, so a failed
task can simply be retried (the files use DROP + CREATE, they are idempotent).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from common import pg_connect, read_checks, run_sql_file

default_args = {
    "owner": "data-engineering",
    "retries": 2,                          # failed attempts are retried automatically
    "retry_delay": timedelta(seconds=30),
}


def run_quality_checks():
    """Run every check, save the results, and fail if at least one check failed."""
    failed = []
    with pg_connect() as conn, conn.cursor() as cur:
        for name, query in read_checks():
            cur.execute(query)
            bad_rows = cur.fetchone()[0]
            cur.execute("INSERT INTO monitoring.dq_results (check_name, failed_rows, passed) VALUES (%s, %s, %s)",
                        (name, bad_rows, bad_rows == 0))
            print(f"{'PASS' if bad_rows == 0 else 'FAIL'}  {name}: {bad_rows} failing rows")
            if bad_rows:
                failed.append(name)
    if failed:
        raise ValueError(f"data-quality checks failed: {', '.join(failed)}")


with DAG(
    dag_id="mobility_analytics",
    description="Geospatial joins, analytics marts and data-quality checks",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=10),
    catchup=False,                # only the latest state matters, no back-filling of old runs
    max_active_runs=1,
    default_args=default_args,
    tags=["mobility", "analytics"],
) as dag:

    def sql_task(file_name):
        """One task per SQL file, named after the file."""
        return PythonOperator(task_id=file_name.split("/")[-1].replace(".sql", ""),
                              python_callable=run_sql_file, op_args=[file_name])

    sensor_context = sql_task("marts/01_sensor_context.sql")
    stop_context = sql_task("marts/02_stop_context.sql")
    segment = sql_task("marts/03_traffic_segment_15min.sql")
    district = sql_task("marts/04_traffic_district_hourly.sql")
    profile = sql_task("marts/05_traffic_hourly_profile.sql")
    transit_line = sql_task("marts/06_transit_line_hourly.sql")
    stop_delays = sql_task("marts/07_transit_stop_delays.sql")
    completeness = sql_task("marts/08_sensor_completeness_hourly.sql")
    checks = PythonOperator(task_id="data_quality_checks", python_callable=run_quality_checks)

    # The geospatial joins come first: the traffic marts need the sensor -> road link.
    sensor_context >> [segment, district, profile, stop_context]
    stop_context >> stop_delays
    [segment, district, profile, transit_line, stop_delays, completeness] >> checks
