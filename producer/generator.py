"""
Builds synthetic mobility events (pure Python, no Kafka here so it is easy to test).

Two kinds of events:

  traffic readings  - every sensor sends one reading every 10 s:
                      vehicle_count, avg_speed_kmh, occupancy_pct
  transit events    - a bus or tram arrives at a stop, with its scheduled
                      time, so the pipeline can compute the delay

The values follow a daily pattern (morning and evening rush hours), and
random "incidents" slow down one road for a while.

Besides normal events it can create "messy" data on purpose, to test the pipeline:
  * late events       - event_time 30 s to 5 min in the past
  * very late events  - older than the Spark watermark (20-40 min)
  * invalid events    - negative counts, impossible speeds, unknown sensor, bad timestamps...
  * sensor outages    - a sensor stops sending for a while (missing-sensor detection)
  * schema v1 / v2    - v2 traffic readings add two optional fields
"""

import json
import math
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

# ------------------------------------------------------------------ settings
LATE_RATE = 0.05          # 5 % of readings arrive 30 s - 5 min late
VERY_LATE_RATE = 0.005    # 0.5 % arrive after the 10-minute watermark
INVALID_RATE = 0.01       # 1 % break a data-quality rule
SCHEMA_V2_RATE = 0.5      # half of the sensors already run the new firmware
OUTAGE_START_RATE = 0.0004  # chance per reading that a sensor goes silent
INCIDENT_RATE = 0.00005   # chance per reading that an incident starts on its road
READING_INTERVAL_S = 10   # one reading per sensor every 10 s

CITY_FILE = os.getenv("CITY_FILE", os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "city", "city.json"))


def load_city(path=CITY_FILE):
    """Read the sample city produced by scripts/generate_city.py."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def now_utc():
    """Current time in UTC."""
    return datetime.now(timezone.utc)


def iso(ts):
    """Format a datetime as ISO-8601 UTC, e.g. 2026-11-01T08:00:00.123Z"""
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


# ------------------------------------------------------------------ traffic model
def demand_factor(ts):
    """
    How busy the city is at this time, between about 0.15 (night) and 1.0 (rush hour).
    Two bumps: around 8:00 and around 17:30 (local time ~ UTC+1). Weekends are calmer.
    """
    hour = (ts.hour + 1 + ts.minute / 60) % 24    # simple local time (UTC+1)
    morning = math.exp(-((hour - 8.0) ** 2) / 2.0)
    evening = math.exp(-((hour - 17.5) ** 2) / 3.0)
    daytime = 0.45 if 7 <= hour <= 20 else 0.0
    base = 0.15 + 0.85 * max(morning, evening, daytime)
    if ts.weekday() >= 5:
        base *= 0.9           # weekends are a little calmer
    return min(base, 1.0)


def traffic_values(sensor, ts, incident=False):
    """
    Realistic values for one 10-second reading.
    More demand -> more vehicles, higher occupancy and lower speed.
    An incident cuts the speed by more than half.
    """
    demand = demand_factor(ts)
    if incident:
        demand = min(demand + 0.6, 1.3)
    # vehicles counted during the 10 s interval
    expected = sensor["free_flow_vpm"] * demand * READING_INTERVAL_S / 60
    vehicle_count = max(0, int(random.gauss(expected, max(expected * 0.25, 0.5))))
    occupancy = min(98.0, max(0.0, random.gauss(70 * demand ** 1.6, 4)))
    # speed drops when the road gets full (simple speed/density curve)
    limit = sensor["speed_limit_kmh"]
    speed = limit * (1.05 - 0.75 * demand ** 2)
    if incident:
        speed *= 0.4
    speed = max(3.0, random.gauss(speed, limit * 0.06))
    return vehicle_count, round(speed, 1), round(occupancy, 1)


class TrafficSimulator:
    """Keeps the state of every sensor: next reading time, outages, incidents."""

    def __init__(self, city, start=None, seed=None):
        """Create the state for every sensor in the city."""
        self.rnd = random.Random(seed)
        self.sensors = city["sensors"]
        start = start or now_utc()
        # spread the first readings over 10 s so they don't all arrive at once
        self.next_time = {s["sensor_id"]: start + timedelta(seconds=self.rnd.uniform(0, READING_INTERVAL_S))
                          for s in self.sensors}
        self.silent_until = {}      # sensor_id -> end of its outage
        self.incident_until = {}    # segment_id -> end of the incident
        # firmware version decides the schema version (stable per sensor)
        self.version = {s["sensor_id"]: 2 if self.rnd.random() < SCHEMA_V2_RATE else 1 for s in self.sensors}

    def make_reading(self, sensor, ts):
        """One valid reading as a Python dict."""
        incident = self.incident_until.get(sensor["true_segment_id"], ts) > ts
        count, speed, occupancy = traffic_values(sensor, ts, incident)
        event = {
            "event_id": str(uuid.uuid4()),
            "schema_version": self.version[sensor["sensor_id"]],
            "sensor_id": sensor["sensor_id"],
            "event_time": iso(ts),
            "vehicle_count": count,
            "avg_speed_kmh": speed,
            "occupancy_pct": occupancy,
        }
        # Schema evolution: version 2 adds two optional fields.
        if event["schema_version"] == 2:
            event["heavy_vehicle_count"] = sum(1 for _ in range(count) if random.random() < 0.08)
            event["firmware"] = "2.1.0"
        return event

    def step(self, now):
        """Return every reading that is due at time `now`."""
        events = []
        for s in self.sensors:
            sid = s["sensor_id"]
            while self.next_time[sid] <= now:
                ts = self.next_time[sid]
                self.next_time[sid] = ts + timedelta(seconds=READING_INTERVAL_S)
                # sensor outage: the sensor sends NOTHING (that is what we must detect)
                if self.silent_until.get(sid, ts) > ts:
                    continue
                if self.rnd.random() < OUTAGE_START_RATE:
                    self.silent_until[sid] = ts + timedelta(minutes=self.rnd.uniform(6, 25))
                    continue
                if self.rnd.random() < INCIDENT_RATE:
                    self.incident_until[s["true_segment_id"]] = ts + timedelta(minutes=self.rnd.uniform(10, 30))
                events.append(self.make_reading(s, ts))
        return events


def make_late(event, now, rnd=random):
    """Move an event back in time: sometimes a little (late), rarely a lot (beyond the watermark)."""
    r = rnd.random()
    if r < VERY_LATE_RATE:
        event["event_time"] = iso(now - timedelta(minutes=rnd.uniform(20, 40)))
    elif r < VERY_LATE_RATE + LATE_RATE:
        event["event_time"] = iso(now - timedelta(seconds=rnd.uniform(30, 300)))
    return event


def make_invalid_reading(event, rnd=random):
    """Break exactly one data-quality rule (Spark must reject the record)."""
    problem = rnd.choice(["negative_count", "speed_too_high", "occupancy_over_100", "missing_sensor",
                          "unknown_sensor", "bad_timestamp", "future_timestamp", "missing_speed"])
    if problem == "negative_count":
        event["vehicle_count"] = -rnd.randint(1, 20)
    elif problem == "speed_too_high":
        event["avg_speed_kmh"] = round(rnd.uniform(220, 400), 1)
    elif problem == "occupancy_over_100":
        event["occupancy_pct"] = round(rnd.uniform(101, 150), 1)
    elif problem == "missing_sensor":
        del event["sensor_id"]
    elif problem == "unknown_sensor":
        event["sensor_id"] = "S999"
    elif problem == "bad_timestamp":
        event["event_time"] = "31/02/2026 25:61"
    elif problem == "future_timestamp":
        event["event_time"] = iso(now_utc() + timedelta(days=1))
    elif problem == "missing_speed":
        del event["avg_speed_kmh"]
    return event


# ------------------------------------------------------------------ public transport
class TransitSimulator:
    """
    Simulates buses and trams. A vehicle leaves the first stop every `headway`
    minutes and arrives at each next stop after the scheduled travel time plus a
    random delay. The delay grows at rush hour and can recover a little.
    """

    def __init__(self, city, start=None, seed=None):
        """Prepare the schedule of every line."""
        self.rnd = random.Random(seed)
        self.lines = city["lines"]
        self.stops = {s["stop_id"]: s for s in city["stops"]}
        start = (start or now_utc()).replace(second=0, microsecond=0)
        self.next_departure = {l["line_id"]: start + timedelta(minutes=self.rnd.uniform(0, l["headway_min"]))
                               for l in self.lines}
        self.vehicles = []       # trips currently on the road
        self.trip_counter = 0

    def _start_trip(self, line, departure):
        """A new vehicle leaves the first stop of the line."""
        self.trip_counter += 1
        stops = line["stops"]
        schedule, t = [], departure
        for stop_id in stops:
            schedule.append((stop_id, t))
            minutes = self.stops[stop_id]["minutes_to_next"]
            if minutes:
                t = t + timedelta(minutes=minutes)
        self.vehicles.append({
            "line_id": line["line_id"],
            "vehicle_id": f"{line['line_id']}-V{self.trip_counter % 40:02d}",
            "trip_id": f"{line['line_id']}-{departure:%Y%m%d%H%M}",
            "schedule": schedule,
            "next_index": 0,
            "delay_s": self.rnd.uniform(-20, 40),
            "passengers": self.rnd.randint(5, 30),
            "capacity": 200 if line["mode"] == "tram" else 90,
        })

    def step(self, now):
        """Return every arrival event that happened up to `now`."""
        for line in self.lines:
            lid = line["line_id"]
            while self.next_departure[lid] <= now:
                self._start_trip(line, self.next_departure[lid])
                self.next_departure[lid] += timedelta(minutes=line["headway_min"])

        events, still_running = [], []
        for v in self.vehicles:
            while v["next_index"] < len(v["schedule"]):
                stop_id, scheduled = v["schedule"][v["next_index"]]
                actual = scheduled + timedelta(seconds=v["delay_s"])
                if actual > now:
                    break
                events.append(self._arrival(v, stop_id, scheduled, actual))
                v["next_index"] += 1
                # delay changes between stops: more at rush hour, sometimes recovers
                rush = demand_factor(actual)
                v["delay_s"] = max(-50.0, v["delay_s"] + self.rnd.gauss(50 * rush - 6, 22))
            if v["next_index"] < len(v["schedule"]):
                still_running.append(v)
        self.vehicles = still_running
        return events

    def _arrival(self, v, stop_id, scheduled, actual):
        """One ARRIVAL event (DEPARTURE for the first stop of a trip)."""
        rush = demand_factor(actual)
        change = int(self.rnd.gauss(12 * rush, 6))
        v["passengers"] = int(min(v["capacity"], max(0, v["passengers"] + change)))
        if v["next_index"] == len(v["schedule"]) - 1:
            v["passengers"] = 0     # everybody gets off at the terminus
        return {
            "event_id": str(uuid.uuid4()),
            "schema_version": 1,
            "line_id": v["line_id"],
            "vehicle_id": v["vehicle_id"],
            "trip_id": v["trip_id"],
            "stop_id": stop_id,
            "event_type": "DEPARTURE" if v["next_index"] == 0 else "ARRIVAL",
            "scheduled_time": iso(scheduled),
            "event_time": iso(actual),
            "passengers": v["passengers"],
        }


def make_invalid_transit(event, rnd=random):
    """Break exactly one rule of a transit event."""
    problem = rnd.choice(["unknown_line", "unknown_stop", "bad_type", "negative_passengers", "missing_scheduled"])
    if problem == "unknown_line":
        event["line_id"] = "X9"
    elif problem == "unknown_stop":
        event["stop_id"] = "ZZ-99"
    elif problem == "bad_type":
        event["event_type"] = "TELEPORT"
    elif problem == "negative_passengers":
        event["passengers"] = -5
    elif problem == "missing_scheduled":
        del event["scheduled_time"]
    return event
