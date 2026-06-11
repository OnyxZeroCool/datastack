"""
Генератор событий продаж в Kafka (топик sales_events).

Запуск из python-контейнера:
    docker compose exec python python scripts/kafka_produce_sales.py [N]
N — сколько событий отправить (по умолчанию 1000).

Событие — JSON той же формы, что таблица sales в Greenplum:
    {"id": 1, "ts": "2026-06-11T12:00:00", "region": "north",
     "product": "widget", "amount": 123.45}
"""
import json
import os
import random
import sys
from datetime import datetime, timedelta

from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "sales_events")

REGIONS = ["north", "south", "east", "west"]
PRODUCTS = ["widget", "gadget", "gizmo", "doohickey"]


def make_event(i: int, base_ts: datetime) -> dict:
    return {
        "id": i,
        "ts": (base_ts + timedelta(seconds=i)).isoformat(timespec="seconds"),
        "region": random.choice(REGIONS),
        "product": random.choice(PRODUCTS),
        "amount": round(random.uniform(1, 1000), 2),
    }


def main(n: int) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    base_ts = datetime.now().replace(microsecond=0)
    delivered = 0

    def on_delivery(err, msg):
        nonlocal delivered
        if err is not None:
            raise RuntimeError(f"delivery failed: {err}")
        delivered += 1

    for i in range(1, n + 1):
        event = make_event(i, base_ts)
        # key=region — события одного региона попадают в одну партицию
        producer.produce(
            TOPIC,
            key=event["region"],
            value=json.dumps(event),
            on_delivery=on_delivery,
        )
        # отдаём callbacks и не даём переполниться внутренней очереди
        producer.poll(0)

    producer.flush(30)
    print(f"OK: отправлено {delivered}/{n} событий в {TOPIC} ({BOOTSTRAP})")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 1000)
