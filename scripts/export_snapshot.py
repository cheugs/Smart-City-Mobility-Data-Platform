"""
Export a snapshot of the running pipeline for the free online dashboard.

The full stack (Kafka + Spark + Airflow + Grafana) needs about 6 GB of memory,
more than free hosting offers. So the online version (Render) runs the SAME
dashboard in demo mode: it reads dashboard/data/snapshot.json, made by this
script from a real local run. Every API answer is computed by the same SQL
functions as in live mode (dashboard/queries.py), for each time range.

    python scripts/export_snapshot.py            (stack running, PostGIS on localhost:5433)

Then commit dashboard/data/snapshot.json and push: Render redeploys by itself.
"""

import json
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal

import psycopg2

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "dashboard"))
import queries  # noqa: E402  (needs the path above)

OUT = os.path.join(ROOT, "dashboard", "data", "snapshot.json")


def load_env():
    """Read .env so the script uses the same password as Docker."""
    path = os.path.join(ROOT, ".env")
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key, value)


def to_json(value):
    """JSON encoder for dates and decimals."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(type(value))


def main():
    """Run every dashboard query in one consistent database snapshot and save the answers."""
    load_env()
    conn = psycopg2.connect(host=os.getenv("PG_HOST", "localhost"), port=int(os.getenv("PG_PORT", "5433")),
                            dbname=os.getenv("POSTGRES_DB", "mobility"), user=os.getenv("POSTGRES_USER", "mobility"),
                            password=os.getenv("POSTGRES_PASSWORD", ""))
    # REPEATABLE READ: all queries see exactly the same data, even while Spark keeps writing
    conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
    pages = {"overview": queries.overview, "traffic": queries.traffic, "transit": queries.transit,
             "pipeline": queries.pipeline, "quality": queries.quality}
    with conn.cursor() as cur:
        snapshot = {
            "snapshot_at": datetime.now(timezone.utc).isoformat(),
            "health": queries.pipeline_health(cur),
            "map": queries.city_map(cur),
            "pages": {name: {str(h): func(cur, h) for h in queries.RANGES} for name, func in pages.items()},
        }
    conn.close()
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, default=to_json, ensure_ascii=False, separators=(",", ":"))
    print(f"saved {OUT} ({os.path.getsize(OUT) / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
