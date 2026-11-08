"""Unit tests of the synthetic data generator (no Kafka, no Spark)."""

import uuid
from datetime import datetime, timedelta, timezone

from generator import (READING_INTERVAL_S, TrafficSimulator, TransitSimulator, demand_factor, load_city,
                       make_invalid_reading, make_invalid_transit, make_late, traffic_values)

CITY = load_city()
T0 = datetime(2026, 3, 10, 6, 0, tzinfo=timezone.utc)   # a Tuesday, 7:00 local time


def test_city_has_expected_size():
    assert len(CITY["sensors"]) == 60
    assert len(CITY["lines"]) == 5
    assert len({s["stop_id"] for s in CITY["stops"]}) == len(CITY["stops"])


def test_every_sensor_sends_one_reading_every_10_seconds():
    sim = TrafficSimulator(CITY, start=T0, seed=1)
    sim.silent_until = {}                       # no outage for this test
    import generator
    old = generator.OUTAGE_START_RATE
    generator.OUTAGE_START_RATE = 0
    try:
        events = sim.step(T0 + timedelta(seconds=60))
    finally:
        generator.OUTAGE_START_RATE = old
    per_sensor = {}
    for e in events:
        per_sensor[e["sensor_id"]] = per_sensor.get(e["sensor_id"], 0) + 1
    assert len(per_sensor) == 60
    assert all(n in (6, 7) for n in per_sensor.values())   # 60 s / 10 s (+1 depending on the first offset)


def test_readings_have_unique_ids_and_valid_values():
    sim = TrafficSimulator(CITY, start=T0, seed=2)
    events = sim.step(T0 + timedelta(minutes=5))
    ids = [e["event_id"] for e in events]
    assert len(ids) == len(set(ids))
    for e in events:
        uuid.UUID(e["event_id"])                       # a real UUID
        assert e["vehicle_count"] >= 0
        assert 0 < e["avg_speed_kmh"] <= 200
        assert 0 <= e["occupancy_pct"] <= 100
        assert e["event_time"].endswith("Z")
        datetime.fromisoformat(e["event_time"].replace("Z", "+00:00"))


def test_schema_v2_readings_have_the_extra_fields():
    sim = TrafficSimulator(CITY, start=T0, seed=3)
    events = sim.step(T0 + timedelta(minutes=2))
    v1 = [e for e in events if e["schema_version"] == 1]
    v2 = [e for e in events if e["schema_version"] == 2]
    assert v1 and v2
    assert all("firmware" not in e for e in v1)
    assert all("heavy_vehicle_count" in e and e["heavy_vehicle_count"] <= e["vehicle_count"] for e in v2)


def test_rush_hour_is_slower_and_busier_than_the_night():
    night = datetime(2026, 3, 10, 2, 0, tzinfo=timezone.utc)      # 3:00 local
    rush = datetime(2026, 3, 10, 7, 0, tzinfo=timezone.utc)       # 8:00 local
    assert demand_factor(rush) > 0.9 > 0.3 > demand_factor(night)
    sensor = next(s for s in CITY["sensors"] if s["road_class"] == "arterial")
    night_values = [traffic_values(sensor, night) for _ in range(200)]
    rush_values = [traffic_values(sensor, rush) for _ in range(200)]
    avg = lambda vals, i: sum(v[i] for v in vals) / len(vals)
    assert avg(rush_values, 0) > avg(night_values, 0)          # more vehicles
    assert avg(rush_values, 1) < avg(night_values, 1)          # lower speed


def test_sensor_outage_means_no_readings():
    sim = TrafficSimulator(CITY, start=T0, seed=4)
    sim.silent_until["S001"] = T0 + timedelta(minutes=10)
    events = sim.step(T0 + timedelta(minutes=5))
    assert not [e for e in events if e["sensor_id"] == "S001"]


def test_late_events_are_moved_back_in_time():
    class AlwaysLate:
        def random(self): return 0.01          # between VERY_LATE_RATE and VERY_LATE + LATE
        def uniform(self, a, b): return a
    e = {"event_time": "2026-03-10T06:00:00.000Z"}
    make_late(e, T0, AlwaysLate())
    assert e["event_time"] < "2026-03-10T06:00:00.000Z"


def test_invalid_readings_break_one_rule():
    sim = TrafficSimulator(CITY, start=T0, seed=5)
    for _ in range(40):
        e = make_invalid_reading(sim.make_reading(CITY["sensors"][0], T0))
        broken = (
            "sensor_id" not in e or e["sensor_id"] == "S999" or "avg_speed_kmh" not in e
            or e.get("vehicle_count", 0) < 0 or e.get("avg_speed_kmh", 0) > 200
            or e.get("occupancy_pct", 0) > 100 or not e["event_time"].startswith("20") or e["event_time"] > "2026-03-11"
        )
        assert broken


def test_transit_vehicles_follow_the_timetable_with_a_delay():
    sim = TransitSimulator(CITY, start=T0, seed=6)
    events = sim.step(T0 + timedelta(hours=1))
    assert {e["line_id"] for e in events} == {"T1", "L1", "L2", "L3", "L4"}
    stop_ids = {s["stop_id"] for s in CITY["stops"]}
    for e in events:
        assert e["stop_id"] in stop_ids
        assert e["event_type"] in ("ARRIVAL", "DEPARTURE")
        assert 0 <= e["passengers"] <= 200
        actual = datetime.fromisoformat(e["event_time"].replace("Z", "+00:00"))
        scheduled = datetime.fromisoformat(e["scheduled_time"].replace("Z", "+00:00"))
        assert abs((actual - scheduled).total_seconds()) < 3600
    # stops of one trip arrive in order
    trip = [e for e in events if e["trip_id"] == events[0]["trip_id"]]
    assert [e["stop_id"] for e in trip] == sorted(e["stop_id"] for e in trip)


def test_invalid_transit_events_break_one_rule():
    sim = TransitSimulator(CITY, start=T0, seed=7)
    base = sim.step(T0 + timedelta(minutes=30))[0]
    for _ in range(20):
        e = make_invalid_transit(dict(base))
        assert (e["line_id"] == "X9" or e["stop_id"] == "ZZ-99" or e["event_type"] == "TELEPORT"
                or e["passengers"] < 0 or "scheduled_time" not in e)


def test_reading_interval_matches_the_database_setting():
    # city.sensors.expected_interval_s = 10 in the seed file
    assert READING_INTERVAL_S == 10
