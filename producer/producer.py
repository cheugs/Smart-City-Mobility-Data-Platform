"""
Synthetic mobility producer: sends the events from generator.py to Kafka.

  topic traffic.readings  - key = sensor_id  (one sensor's readings stay in order)
  topic transit.events    - key = line_id    (one line's events stay in order)

On top of the generator's messy data it also simulates two transport problems:
  * duplicates  - the same message sent twice (same event_id)
  * malformed   - messages that are not valid JSON
"""

import json
import logging
import os
import random
import time

from confluent_kafka import Producer

from backfill import run_backfill, topic_is_empty
from generator import (INVALID_RATE, TrafficSimulator, TransitSimulator, load_city, make_invalid_reading,
                       make_invalid_transit, make_late, now_utc)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29093")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic.readings")
TRANSIT_TOPIC = os.getenv("TRANSIT_TOPIC", "transit.events")
BACKFILL_HOURS = float(os.getenv("BACKFILL_HOURS", "0"))   # history sent on the very first start

DUPLICATE_RATE = 0.03     # 3 % of messages are sent twice
MALFORMED_RATE = 0.002    # 0.2 % of steps also send a broken JSON message

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s producer: %(message)s")
log = logging.getLogger(__name__)


def make_producer(bootstrap=KAFKA_BOOTSTRAP):
    """Create a Kafka producer with safe delivery settings."""
    # Delivery guarantee on the producer side:
    #   acks=all             -> the broker confirms only when the write is safe
    #   enable.idempotence   -> retries never create duplicates inside Kafka
    return Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
    })


def delivery_report(err, msg):
    """Called by Kafka for every message: log it if delivery failed."""
    if err is not None:
        log.error("Delivery failed: %s", err)


def send(producer, topic, key, value):
    """Send one message; if the local buffer is full, wait for it to drain and retry."""
    while True:
        try:
            producer.produce(topic, key=key.encode("utf-8"), value=value.encode("utf-8"),
                             on_delivery=delivery_report)
            return
        except BufferError:
            producer.poll(0.1)


def publish(producer, traffic_events, transit_events, now, messy=True):
    """Send one step of events, adding late / invalid / duplicate / malformed data if `messy`."""
    sent = 0
    for e in traffic_events:
        if messy:
            make_late(e, now)
            if random.random() < INVALID_RATE:
                make_invalid_reading(e)
        value = json.dumps(e)
        send(producer, TRAFFIC_TOPIC, e.get("sensor_id", "unknown"), value)
        sent += 1
        if messy and random.random() < DUPLICATE_RATE:   # an upstream system re-sends it
            send(producer, TRAFFIC_TOPIC, e.get("sensor_id", "unknown"), value)
    for e in transit_events:
        if messy and random.random() < INVALID_RATE:
            make_invalid_transit(e)
        value = json.dumps(e)
        send(producer, TRANSIT_TOPIC, e.get("line_id", "unknown"), value)
        sent += 1
        if messy and random.random() < DUPLICATE_RATE:
            send(producer, TRANSIT_TOPIC, e.get("line_id", "unknown"), value)
    if messy and random.random() < MALFORMED_RATE:
        send(producer, TRAFFIC_TOPIC, "broken", '{"event_id": "broken", "sensor_id": ')
    return sent


def main():
    """Send the history once (first start only), then events in real time, forever."""
    city = load_city()
    producer = make_producer()
    # First start: if Kafka is still empty, send the last BACKFILL_HOURS hours first (see backfill.py)
    if BACKFILL_HOURS > 0 and topic_is_empty(KAFKA_BOOTSTRAP, TRAFFIC_TOPIC):
        run_backfill(producer, publish, BACKFILL_HOURS)
    traffic, transit = TrafficSimulator(city), TransitSimulator(city)
    log.info("Simulating %d sensors and %d transit lines -> %s / %s on %s",
             len(city["sensors"]), len(city["lines"]), TRAFFIC_TOPIC, TRANSIT_TOPIC, KAFKA_BOOTSTRAP)
    total = 0
    while True:
        now = now_utc()
        total += publish(producer, traffic.step(now), transit.step(now), now)
        producer.poll(0)
        if total and total % 1000 < 10:
            log.info("%d events sent", total)
        time.sleep(1)


if __name__ == "__main__":
    main()
