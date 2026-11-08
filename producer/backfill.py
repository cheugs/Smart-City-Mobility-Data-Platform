"""
Backfill: simulate the last N hours of the city and send them to Kafka quickly,
so the dashboard has a full day of history (rush hours, night...) right away.

Events are sent in event-time order, so the Spark watermark moves forward with
them and no window is dropped. That only works if NO live event was sent before
(a live event would move the watermark to "now" and the history would be too late).
So producer.py runs the backfill automatically on the very first start, when the
traffic topic is still empty (BACKFILL_HOURS in .env, 0 = no history).

It can also be run by hand on an empty stack:
    docker compose run --rm producer python backfill.py --hours 24
"""

import argparse
import logging
from datetime import timedelta

from confluent_kafka import Consumer, TopicPartition

from generator import TrafficSimulator, TransitSimulator, load_city, now_utc

log = logging.getLogger("backfill")


def topic_is_empty(bootstrap, topic):
    """True if no message was ever written to the topic (all partitions at offset 0)."""
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "backfill-check"})
    try:
        partitions = consumer.list_topics(topic, timeout=30).topics[topic].partitions
        for p in partitions:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30)
            if high > 0:
                return False
        return True
    finally:
        consumer.close()


def run_backfill(producer, publish, hours):
    """Simulate minute after minute from now - N hours until now, and publish everything."""
    city = load_city()
    end = now_utc()
    t = end - timedelta(hours=hours)
    traffic, transit = TrafficSimulator(city, start=t, seed=1), TransitSimulator(city, start=t, seed=2)
    log.info("backfill: simulating %.1f hours of history, from %s", hours, t.strftime("%Y-%m-%d %H:%M"))
    total = 0
    while t < end:
        t = min(t + timedelta(seconds=30), end)
        total += publish(producer, traffic.step(t), transit.step(t), t)
        producer.poll(0)
        if t.minute == 0 and t.second < 30:
            log.info("backfill: simulated up to %s (%d events)", t.strftime("%Y-%m-%d %H:%M"), total)
    producer.flush()
    log.info("backfill done: %d events covering %.1f hours", total, hours)
    return total


def main():
    """Command-line entry point (manual backfill)."""
    from producer import make_producer, publish     # imported here to avoid a circular import
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=float, default=24)
    args = parser.parse_args()
    run_backfill(make_producer(), publish, args.hours)


if __name__ == "__main__":
    main()
