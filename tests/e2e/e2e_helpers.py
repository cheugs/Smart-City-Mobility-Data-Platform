"""
Helpers for the end-to-end tests. These tests need the Docker stack running:
    docker compose up -d --build
They send events to Kafka on localhost:29093 and check PostGIS on localhost:5433.
"""

import json
import os
import subprocess
import time

import psycopg2
import pytest
from confluent_kafka import Producer

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def load_env():
    """Read the .env file so the tests use the same password as Docker."""
    path = os.path.join(ROOT, ".env")
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key, value)
    os.environ.setdefault("PG_HOST", "localhost")
    os.environ.setdefault("PG_PORT", "5433")          # host port of the PostGIS container
    os.environ.setdefault("PG_DB", os.environ.get("POSTGRES_DB", "mobility"))
    os.environ.setdefault("PG_USER", os.environ.get("POSTGRES_USER", "mobility"))
    os.environ.setdefault("PG_PASSWORD", os.environ.get("POSTGRES_PASSWORD", ""))


load_env()


def pg():
    """Open a connection to the PostGIS of the running stack."""
    return psycopg2.connect(host=os.environ["PG_HOST"], port=int(os.environ["PG_PORT"]), dbname=os.environ["PG_DB"],
                            user=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"])


def sql(query, params=None):
    """Run a query and return all rows (new connection each time)."""
    conn = pg()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if cur.description else None
    finally:
        conn.close()


def wait_for(check, timeout=180, every=2, message="condition"):
    """Call check() until it returns something truthy, or fail after timeout seconds."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = check()
            if last:
                return last
        except psycopg2.Error as e:   # e.g. Postgres restarting, table being rebuilt
            last = e
        time.sleep(every)
    pytest.fail(f"timed out after {timeout}s waiting for {message} (last value: {last})")


def compose(*args, timeout=300):
    """Run a docker compose command in the project folder."""
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, capture_output=True,
                          text=True, timeout=timeout)


def run_dag(dag_id, timeout=600):
    """Run one Airflow DAG synchronously inside the airflow container (airflow dags test)."""
    result = compose("exec", "-T", "airflow", "airflow", "dags", "test", dag_id, timeout=timeout)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    return result.stdout


class KafkaSender:
    """Sends test events to the traffic / transit topics."""

    def __init__(self):
        """Create the Kafka producer."""
        self.producer = Producer({"bootstrap.servers": "localhost:29093", "acks": "all",
                                  "enable.idempotence": True})

    def send(self, topic, *events):
        """Send events (dicts or raw strings) and wait until Kafka has them all."""
        for e in events:
            value = e if isinstance(e, str) else json.dumps(e)
            key = (e.get("sensor_id") or e.get("line_id") or "x") if isinstance(e, dict) else "x"
            self.producer.produce(topic, key=key.encode(), value=value.encode())
        assert self.producer.flush(30) == 0, "some messages were not delivered to Kafka"

    def traffic(self, *events):
        """Send to traffic.readings."""
        self.send("traffic.readings", *events)

    def transit(self, *events):
        """Send to transit.events."""
        self.send("transit.events", *events)
