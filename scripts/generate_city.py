"""
Generates the reference data of the sample city (road network, districts,
traffic sensors, public transport lines and stops).

The city is SYNTHETIC: a slightly irregular 7 x 7 street grid placed around
the coordinates of Limoges (France) so it can be drawn on a real map.

Outputs (all deterministic thanks to a fixed random seed):
  data/city/city.json          - compact model used by the producer
  data/city/*.geojson          - layers drawn on the dashboard map
  sql/02_city_seed.sql         - INSERT statements loaded into PostGIS at start-up

Run it again only if you want to change the city:
    python scripts/generate_city.py
"""

import json
import math
import os
import random

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(ROOT, "data", "city")
SEED_SQL = os.path.join(ROOT, "sql", "02_city_seed.sql")

rng = random.Random(7)          # fixed seed -> same city every time

# ------------------------------------------------------------------ grid
CENTER_LAT, CENTER_LON = 45.8336, 1.2611
N = 7                            # 7 x 7 intersections
LAT_STEP, LON_STEP = 0.0045, 0.0064   # about 500 m between intersections

# Street names (fictional, French style). Rows go west -> east, columns north -> south.
ROW_NAMES = ["Boulevard du Nord", "Rue des Tilleuls", "Avenue de la Liberté", "Cours de la République",
             "Rue du Pont-Neuf", "Avenue des Platanes", "Boulevard du Sud"]
COL_NAMES = ["Boulevard de l'Ouest", "Rue des Écoles", "Rue Victor Hugo", "Avenue Centrale",
             "Rue de la Gare", "Rue des Arts", "Boulevard de l'Est"]

# Road classes: (speed limit km/h, lanes per direction, free-flow vehicles per minute)
ROAD_CLASSES = {
    "boulevard": (70, 2, 40),
    "arterial": (50, 2, 30),
    "local": (30, 1, 12),
}


def road_class(is_row, index):
    """Outer ring = boulevard, middle row/column = arterial, the rest = local streets."""
    if index in (0, N - 1):
        return "boulevard"
    if index == 3:
        return "arterial"
    return "local"


def build_nodes():
    """Intersections of the grid with a small random shift so it looks less artificial."""
    top_lat = CENTER_LAT + LAT_STEP * (N - 1) / 2
    left_lon = CENTER_LON - LON_STEP * (N - 1) / 2
    nodes = {}
    for r in range(N):
        for c in range(N):
            jitter = 0.0 if (r in (0, N - 1) or c in (0, N - 1)) else 0.0007
            nodes[(r, c)] = (
                round(top_lat - r * LAT_STEP + rng.uniform(-jitter, jitter), 6),
                round(left_lon + c * LON_STEP + rng.uniform(-jitter, jitter), 6),
            )
    return nodes


def distance_m(a, b):
    """Approximate distance in metres between two (lat, lon) points (good enough at city scale)."""
    dlat = (a[0] - b[0]) * 111_320
    dlon = (a[1] - b[1]) * 111_320 * math.cos(math.radians(CENTER_LAT))
    return math.hypot(dlat, dlon)


def build_segments(nodes):
    """One road segment between every pair of neighbouring intersections."""
    segments = []
    for r in range(N):
        for c in range(N - 1):          # horizontal segments (along a row)
            cls = road_class(True, r)
            segments.append(dict(segment_id=f"R{r}{c}-{c + 1}", name=ROW_NAMES[r], road_class=cls,
                                 start=nodes[(r, c)], end=nodes[(r, c + 1)]))
    for c in range(N):
        for r in range(N - 1):          # vertical segments (along a column)
            cls = road_class(False, c)
            segments.append(dict(segment_id=f"C{c}{r}-{r + 1}", name=COL_NAMES[c], road_class=cls,
                                 start=nodes[(r, c)], end=nodes[(r + 1, c)]))
    for s in segments:
        speed, lanes, capacity = ROAD_CLASSES[s["road_class"]]
        s.update(speed_limit_kmh=speed, lanes=lanes, free_flow_vpm=capacity,
                 length_m=round(distance_m(s["start"], s["end"])))
    return segments


def build_districts(nodes):
    """Six districts: the city box cut into 3 columns x 2 rows."""
    lats = [p[0] for p in nodes.values()]
    lons = [p[1] for p in nodes.values()]
    north, south = max(lats) + 0.0015, min(lats) - 0.0015
    west, east = min(lons) - 0.002, max(lons) + 0.002
    mid_lat = (north + south) / 2
    col_edges = [round(west + (east - west) * k / 3, 6) for k in range(4)]
    north, south, mid_lat = round(north, 6), round(south, 6), round(mid_lat, 6)
    names = [["Campus", "Centre-Ville", "Gare"], ["Rives", "Vieux-Quartier", "Zone Est"]]
    districts = []
    for row, (top, bottom) in enumerate([(north, mid_lat), (mid_lat, south)]):
        for col in range(3):
            left, right = col_edges[col], col_edges[col + 1]
            districts.append(dict(
                district_id=f"D{row * 3 + col + 1}", name=names[row][col],
                ring=[(top, left), (top, right), (bottom, right), (bottom, left), (top, left)],
            ))
    return districts


def build_sensors(segments, count=60):
    """
    Place traffic sensors along road segments. Main roads get more sensors.
    Each sensor is moved 5-15 m away from the centre line (like a real roadside
    sensor), so PostGIS has to SNAP it to the nearest road (geospatial join).
    """
    weights = [{"boulevard": 3, "arterial": 3, "local": 1}[s["road_class"]] for s in segments]
    chosen = []
    pool = list(range(len(segments)))
    while len(chosen) < count:
        i = rng.choices(pool, weights=[weights[k] for k in pool])[0]
        pool.remove(i)
        chosen.append(segments[i])
    sensors = []
    for k, seg in enumerate(chosen, start=1):
        f = rng.uniform(0.25, 0.75)
        lat = seg["start"][0] + f * (seg["end"][0] - seg["start"][0])
        lon = seg["start"][1] + f * (seg["end"][1] - seg["start"][1])
        # small perpendicular offset (about 5-15 m)
        dx, dy = seg["end"][1] - seg["start"][1], seg["end"][0] - seg["start"][0]
        length = math.hypot(dx, dy)
        offset = rng.uniform(0.00005, 0.00013) * rng.choice([-1, 1])
        lat += -dx / length * offset
        lon += dy / length * offset
        sensors.append(dict(
            sensor_id=f"S{k:03d}", lat=round(lat, 6), lon=round(lon, 6),
            # the producer needs the true road to simulate realistic values;
            # PostGIS never sees this field, it finds the road by itself
            true_segment_id=seg["segment_id"], road_class=seg["road_class"],
            speed_limit_kmh=seg["speed_limit_kmh"], free_flow_vpm=seg["free_flow_vpm"],
        ))
    return sensors


# Public transport: each line follows one row or column of the grid.
LINES = [
    dict(line_id="T1", name="Tram T1", mode="tram", headway_min=8, color="#1B4D3E", axis=("row", 3), speed_kmh=20),
    dict(line_id="L1", name="Bus L1", mode="bus", headway_min=10, color="#B5562E", axis=("col", 3), speed_kmh=16),
    dict(line_id="L2", name="Bus L2", mode="bus", headway_min=12, color="#2F3A44", axis=("row", 1), speed_kmh=16),
    dict(line_id="L3", name="Bus L3", mode="bus", headway_min=15, color="#7A6A3A", axis=("col", 5), speed_kmh=15),
    dict(line_id="L4", name="Bus L4", mode="bus", headway_min=15, color="#5B4B6B", axis=("row", 6), speed_kmh=18),
]


def build_transit(nodes):
    """Stops at every intersection of the line's row/column, 15-25 m from the crossing."""
    lines, stops = [], []
    for line in LINES:
        kind, idx = line["axis"]
        points = [nodes[(idx, k)] if kind == "row" else nodes[(k, idx)] for k in range(N)]
        line_stops = []
        for seq, p in enumerate(points, start=1):
            cross = COL_NAMES[seq - 1] if kind == "row" else ROW_NAMES[seq - 1]
            stop = dict(
                stop_id=f"{line['line_id']}-{seq:02d}", line_id=line["line_id"], stop_sequence=seq,
                name=cross.replace("Boulevard", "Bd").replace("Avenue", "Av."),
                lat=round(p[0] + rng.uniform(-0.00015, 0.00015), 6),
                lon=round(p[1] + rng.uniform(-0.00015, 0.00015), 6),
            )
            line_stops.append(stop)
        # scheduled travel time between consecutive stops (+30 s dwell time)
        for a, b in zip(line_stops, line_stops[1:]):
            a["minutes_to_next"] = round(distance_m((a["lat"], a["lon"]), (b["lat"], b["lon"]))
                                         / (line["speed_kmh"] * 1000 / 60) + 0.5, 1)
        line_stops[-1]["minutes_to_next"] = None
        stops.extend(line_stops)
        lines.append({k: v for k, v in line.items() if k != "axis"} | {
            "stops": [s["stop_id"] for s in line_stops],
            "path": [[s["lon"], s["lat"]] for s in line_stops],
        })
    return lines, stops


# ------------------------------------------------------------------ writers
def feature(geometry, props):
    """One GeoJSON feature."""
    return {"type": "Feature", "geometry": geometry, "properties": props}


def write_geojson(name, features):
    """Write a GeoJSON FeatureCollection to data/city/<name>.geojson."""
    with open(os.path.join(OUT_DIR, f"{name}.geojson"), "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False)


def wkt_line(a, b):
    """WKT uses (lon lat) order."""
    return f"LINESTRING({a[1]} {a[0]}, {b[1]} {b[0]})"


def sql_text(value):
    """Quote a value for SQL."""
    return "'" + str(value).replace("'", "''") + "'"


def write_seed_sql(segments, districts, sensors, lines, stops):
    """INSERT statements for the reference tables (loaded by Postgres on first start)."""
    out = ["-- GENERATED by scripts/generate_city.py - do not edit by hand.",
           "-- Reference data of the synthetic sample city (SRID 4326 = GPS coordinates).", ""]
    out.append("INSERT INTO city.districts (district_id, name, geom) VALUES")
    out.append(",\n".join(
        f"  ({sql_text(d['district_id'])}, {sql_text(d['name'])}, ST_GeomFromText('POLYGON(("
        + ", ".join(f"{lon} {lat}" for lat, lon in d["ring"]) + "))', 4326))"
        for d in districts) + "\nON CONFLICT DO NOTHING;\n")
    out.append("INSERT INTO city.road_segments (segment_id, name, road_class, speed_limit_kmh, lanes, length_m, geom) VALUES")
    out.append(",\n".join(
        f"  ({sql_text(s['segment_id'])}, {sql_text(s['name'])}, {sql_text(s['road_class'])}, "
        f"{s['speed_limit_kmh']}, {s['lanes']}, {s['length_m']}, "
        f"ST_GeomFromText('{wkt_line(s['start'], s['end'])}', 4326))"
        for s in segments) + "\nON CONFLICT DO NOTHING;\n")
    out.append("INSERT INTO city.sensors (sensor_id, expected_interval_s, geom) VALUES")
    out.append(",\n".join(
        f"  ({sql_text(s['sensor_id'])}, 10, ST_SetSRID(ST_MakePoint({s['lon']}, {s['lat']}), 4326))"
        for s in sensors) + "\nON CONFLICT DO NOTHING;\n")
    out.append("INSERT INTO city.transit_lines (line_id, name, mode, headway_min, color) VALUES")
    out.append(",\n".join(
        f"  ({sql_text(l['line_id'])}, {sql_text(l['name'])}, {sql_text(l['mode'])}, "
        f"{l['headway_min']}, {sql_text(l['color'])})" for l in lines) + "\nON CONFLICT DO NOTHING;\n")
    out.append("INSERT INTO city.transit_stops (stop_id, line_id, stop_sequence, name, geom) VALUES")
    out.append(",\n".join(
        f"  ({sql_text(s['stop_id'])}, {sql_text(s['line_id'])}, {s['stop_sequence']}, {sql_text(s['name'])}, "
        f"ST_SetSRID(ST_MakePoint({s['lon']}, {s['lat']}), 4326))" for s in stops) + "\nON CONFLICT DO NOTHING;\n")
    with open(SEED_SQL, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(out))


def main():
    """Build every layer and write all output files."""
    os.makedirs(OUT_DIR, exist_ok=True)
    nodes = build_nodes()
    segments = build_segments(nodes)
    districts = build_districts(nodes)
    sensors = build_sensors(segments)
    lines, stops = build_transit(nodes)

    write_geojson("roads", [feature(
        {"type": "LineString", "coordinates": [[s["start"][1], s["start"][0]], [s["end"][1], s["end"][0]]]},
        {k: s[k] for k in ("segment_id", "name", "road_class", "speed_limit_kmh", "lanes", "length_m")})
        for s in segments])
    write_geojson("districts", [feature(
        {"type": "Polygon", "coordinates": [[[lon, lat] for lat, lon in d["ring"]]]},
        {"district_id": d["district_id"], "name": d["name"]}) for d in districts])
    write_geojson("sensors", [feature(
        {"type": "Point", "coordinates": [s["lon"], s["lat"]]}, {"sensor_id": s["sensor_id"]}) for s in sensors])
    write_geojson("stops", [feature(
        {"type": "Point", "coordinates": [s["lon"], s["lat"]]},
        {k: s[k] for k in ("stop_id", "line_id", "stop_sequence", "name")}) for s in stops])
    write_geojson("lines", [feature(
        {"type": "LineString", "coordinates": l["path"]},
        {k: l[k] for k in ("line_id", "name", "mode", "headway_min", "color")}) for l in lines])

    with open(os.path.join(OUT_DIR, "city.json"), "w", encoding="utf-8") as f:
        json.dump({"sensors": sensors, "lines": [{k: v for k, v in l.items() if k != "path"} for l in lines],
                   "stops": stops}, f, ensure_ascii=False, indent=1)

    write_seed_sql(segments, districts, sensors, lines, stops)
    print(f"{len(segments)} road segments, {len(districts)} districts, {len(sensors)} sensors, "
          f"{len(lines)} lines, {len(stops)} stops -> {OUT_DIR}")


if __name__ == "__main__":
    main()
