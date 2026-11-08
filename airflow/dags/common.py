"""
Small helpers shared by the DAGs: open a Postgres connection and run SQL files.

The connection settings come from environment variables (set in docker-compose.yml
from the .env file), so no password is written in the code.
"""

import logging
import os
import re

import psycopg2

SQL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sql")
log = logging.getLogger(__name__)


def pg_connect():
    """Connection to the mobility database (not Airflow's own metadata database)."""
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "mobility"),
        user=os.getenv("PG_USER", "mobility"),
        password=os.getenv("PG_PASSWORD", ""),
    )


def run_sql_file(relative_path):
    """Run one .sql file in a single transaction (all or nothing)."""
    path = os.path.join(SQL_DIR, relative_path)
    with open(path, encoding="utf-8") as f:
        sql = f.read()
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(sql)
    log.info("ran %s", relative_path)


def read_checks(relative_path="checks/data_quality_checks.sql"):
    """Split the checks file into (name, query) pairs using the '-- check: name' markers."""
    with open(os.path.join(SQL_DIR, relative_path), encoding="utf-8") as f:
        text = f.read()
    parts = re.split(r"^-- check: (\w+)\s*$", text, flags=re.MULTILINE)
    # parts = [header, name1, body1, name2, body2, ...]
    return [(parts[i], parts[i + 1].strip().rstrip(";")) for i in range(1, len(parts), 2)]
