"""
Dashboard backend (FastAPI): a small JSON API + the static web page.

Two modes:
  * LIVE (default, Docker)  - every request runs the SQL queries of queries.py on PostGIS
  * DEMO (DEMO_MODE=1)      - answers come from data/snapshot.json, a copy of a real
                              pipeline run made by scripts/export_snapshot.py.
                              Used for the free online version (no database needed).

Run locally without Docker:
    uvicorn app:app --reload --port 8000       (from the dashboard/ folder)
"""

import json
import os
from datetime import date, datetime
from decimal import Decimal

import psycopg2
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

import queries

HERE = os.path.dirname(os.path.abspath(__file__))
DEMO_MODE = os.getenv("DEMO_MODE", "0") == "1"
SNAPSHOT_FILE = os.getenv("SNAPSHOT_FILE", os.path.join(HERE, "data", "snapshot.json"))
PAGES = {"overview": queries.overview, "traffic": queries.traffic, "transit": queries.transit,
         "pipeline": queries.pipeline, "quality": queries.quality}

app = FastAPI(title="Smart City Mobility API", docs_url="/api/docs")


@app.middleware("http")
async def revalidate_static_files(request, call_next):
    """Browsers must check for a newer version of the page, CSS and JS (no stale design after an update)."""
    response = await call_next(request)
    if request.url.path == "/" or request.url.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-cache"
    return response


def to_json(value):
    """JSON encoder for the types returned by psycopg2 (dates and decimals)."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"cannot serialise {type(value)}")


def json_response(data):
    """Return data as JSON (no browser caching: the data changes every few seconds)."""
    return Response(json.dumps(data, default=to_json), media_type="application/json",
                    headers={"Cache-Control": "no-store"})


def load_snapshot():
    """Read the demo snapshot once (it never changes while the app runs)."""
    with open(SNAPSHOT_FILE, encoding="utf-8") as f:
        return json.load(f)


SNAPSHOT = load_snapshot() if DEMO_MODE else None


def run_live(func, *args):
    """Open a read-only connection, run one query function, close the connection."""
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"), port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "mobility"), user=os.getenv("PG_USER", "mobility"),
        password=os.getenv("PG_PASSWORD", ""), sslmode=os.getenv("PG_SSLMODE", "prefer"),
        connect_timeout=5,
    )
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            return func(cur, *args)
    except psycopg2.errors.UndefinedTable:
        # analytics.* tables are created by the first Airflow run (a minute after start-up)
        raise HTTPException(503, "analytics tables not built yet: waiting for the first Airflow run")
    finally:
        conn.close()


# ------------------------------------------------------------------ API
@app.get("/api/meta")
def meta():
    """Which mode the dashboard runs in, and when the demo data was captured."""
    if DEMO_MODE:
        return json_response({"mode": "demo", "snapshot_at": SNAPSHOT["snapshot_at"],
                              "ranges": list(queries.RANGES)})
    return json_response({"mode": "live", "snapshot_at": None, "ranges": list(queries.RANGES)})


@app.get("/api/health")
def health():
    """Pipeline status shown in the sidebar (healthy / catching up / stalled)."""
    if DEMO_MODE:
        return json_response(SNAPSHOT["health"])
    return json_response(run_live(queries.pipeline_health))


@app.get("/api/map")
def city_map():
    """GeoJSON layers of the city with their current state."""
    if DEMO_MODE:
        return json_response(SNAPSHOT["map"])
    return json_response(run_live(queries.city_map))


@app.get("/api/{page}")
def page_data(page: str, hours: int = Query(1)):
    """Data of one dashboard page for the last `hours` hours (1, 6 or 24)."""
    if page not in PAGES:
        raise HTTPException(404, f"unknown page {page}")
    if hours not in queries.RANGES:
        raise HTTPException(400, f"hours must be one of {list(queries.RANGES)}")
    if DEMO_MODE:
        return json_response(SNAPSHOT["pages"][page][str(hours)])
    return json_response(run_live(PAGES[page], hours))


@app.get("/healthz")
def healthz():
    """Used by Docker / Render to know the app is up."""
    return {"ok": True, "mode": "demo" if DEMO_MODE else "live"}


# ------------------------------------------------------------------ web page
app.mount("/static", StaticFiles(directory=os.path.join(HERE, "static")), name="static")


@app.get("/")
def index():
    """The single-page dashboard."""
    return FileResponse(os.path.join(HERE, "static", "index.html"))
