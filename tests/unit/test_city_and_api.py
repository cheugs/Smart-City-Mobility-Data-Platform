"""
Unit tests of the reference data, the Airflow SQL files and the dashboard API (demo mode).
No database is needed.
"""

import importlib
import json
import math
import os
import re

import pytest

from conftest import ROOT

CITY = json.load(open(os.path.join(ROOT, "data", "city", "city.json"), encoding="utf-8"))
SEED = open(os.path.join(ROOT, "sql", "02_city_seed.sql"), encoding="utf-8").read()


# ------------------------------------------------------------------ reference data
def test_seed_file_matches_the_city_model():
    sensor_ids = re.findall(r"\('(S\d{3})', 10,", SEED)
    assert sorted(sensor_ids) == sorted(s["sensor_id"] for s in CITY["sensors"])
    for stop in CITY["stops"]:
        assert f"'{stop['stop_id']}'" in SEED
    assert SEED.count("ST_GeomFromText('LINESTRING") == 84


def point_segment_distance_m(p, a, b):
    """Distance from point p to segment ab in metres (flat approximation, fine at city scale)."""
    kx = 111_320 * math.cos(math.radians(45.83))
    ky = 111_320
    px, py = p[1] * kx, p[0] * ky
    ax, ay, bx, by = a[1] * kx, a[0] * ky, b[1] * kx, b[0] * ky
    dx, dy = bx - ax, by - ay
    f = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    return math.hypot(px - (ax + f * dx), py - (ay + f * dy))


def test_sensors_are_close_to_their_road_but_not_on_it():
    roads = {f["properties"]["segment_id"]: f["geometry"]["coordinates"]
             for f in json.load(open(os.path.join(ROOT, "data", "city", "roads.geojson")))["features"]}
    for s in CITY["sensors"]:
        (lon1, lat1), (lon2, lat2) = roads[s["true_segment_id"]]
        d = point_segment_distance_m((s["lat"], s["lon"]), (lat1, lon1), (lat2, lon2))
        # 5-15 m away from the centre line: PostGIS has to snap it (geospatial join)
        assert 3 < d < 20, (s["sensor_id"], d)


def test_every_sensor_and_stop_is_inside_one_district():
    districts = json.load(open(os.path.join(ROOT, "data", "city", "districts.geojson")))["features"]

    def inside(lon, lat):
        hits = 0
        for d in districts:
            ring = d["geometry"]["coordinates"][0]
            lons, lats = [p[0] for p in ring], [p[1] for p in ring]
            hits += min(lons) <= lon < max(lons) and min(lats) <= lat < max(lats)
        return hits
    for item in CITY["sensors"] + CITY["stops"]:
        assert inside(item["lon"], item["lat"]) == 1


# ------------------------------------------------------------------ Airflow SQL
def test_quality_checks_file_is_parsed_into_named_queries():
    from common import read_checks
    checks = read_checks()
    names = [n for n, _ in checks]
    assert len(checks) == 10
    assert len(set(names)) == len(names)
    assert "windows_not_inflated_by_duplicates" in names
    assert all(q.lstrip().upper().startswith(("SELECT", "--")) for _, q in checks)


def test_every_mart_file_is_used_by_the_dag():
    dag_code = open(os.path.join(ROOT, "airflow", "dags", "mobility_analytics.py"), encoding="utf-8").read()
    for name in os.listdir(os.path.join(ROOT, "airflow", "sql", "marts")):
        assert f"marts/{name}" in dag_code, name


# ------------------------------------------------------------------ dashboard API (demo mode)
@pytest.fixture
def demo_client(tmp_path, monkeypatch):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    snapshot = {
        "snapshot_at": "2026-11-08T12:00:00+00:00",
        "health": {"status": "healthy", "kafka_lag": 0, "seconds_since_batch": 3},
        "map": {"roads": {"type": "FeatureCollection", "features": []}},
        "pages": {page: {str(h): {"page": page, "hours": h} for h in (1, 6, 24)}
                  for page in ("overview", "traffic", "transit", "pipeline", "quality")},
    }
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(snapshot))
    monkeypatch.setenv("DEMO_MODE", "1")
    monkeypatch.setenv("SNAPSHOT_FILE", str(path))
    import app
    importlib.reload(app)
    yield TestClient(app.app)
    monkeypatch.setenv("DEMO_MODE", "0")
    importlib.reload(app)


def test_demo_mode_serves_the_snapshot(demo_client):
    assert demo_client.get("/api/meta").json() == {"mode": "demo", "snapshot_at": "2026-11-08T12:00:00+00:00",
                                                    "ranges": [1, 6, 24]}
    assert demo_client.get("/api/traffic?hours=6").json() == {"page": "traffic", "hours": 6}
    assert demo_client.get("/api/health").json()["status"] == "healthy"
    assert demo_client.get("/api/map").json()["roads"]["type"] == "FeatureCollection"


def test_api_rejects_unknown_pages_and_ranges(demo_client):
    assert demo_client.get("/api/nothing").status_code == 404
    assert demo_client.get("/api/overview?hours=5").status_code == 400


def test_index_page_and_static_files_are_served(demo_client):
    page = demo_client.get("/")
    assert page.status_code == 200 and "MOBILITÉ" in page.text
    assert demo_client.get("/static/app.js").status_code == 200
    assert demo_client.get("/healthz").json() == {"ok": True, "mode": "demo"}
