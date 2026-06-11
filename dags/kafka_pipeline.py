"""
Демо-DAG: стриминговый ингест Kafka → Iceberg по расписанию (микробатчи).

  produce_events → spark_kafka_to_iceberg → verify_counts

1) produce_events         — шлёт пачку JSON-событий продаж в топик sales_events
                            (confluent-kafka, вшит в airflow-образ).
2) spark_kafka_to_iceberg — spark-submit jobs/kafka_to_iceberg.py: Structured
                            Streaming с trigger(availableNow) дочитывает бэклог
                            топика в Iceberg и завершается. Checkpoint на S3 —
                            каждый запуск берёт только новые события.
3) verify_counts          — инвариант exactly-once: сумма end-offset'ов топика
                            == count(*) таблицы (Impala, после INVALIDATE).

Требует профили kafka и impala. Запуск вручную из UI.
"""
from __future__ import annotations

import json
import os
import random
from datetime import datetime, timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from lib.spark_task import spark_submit_task

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "sales_events"
BATCH_SIZE = 500


def produce_events():
    from confluent_kafka import Producer

    regions = ["north", "south", "east", "west"]
    products = ["widget", "gadget", "gizmo", "doohickey"]
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    base_ts = datetime.now().replace(microsecond=0)
    for i in range(1, BATCH_SIZE + 1):
        event = {
            "id": i,
            "ts": (base_ts + timedelta(seconds=i)).isoformat(timespec="seconds"),
            "region": random.choice(regions),
            "product": random.choice(products),
            "amount": round(random.uniform(1, 1000), 2),
        }
        producer.produce(TOPIC, key=event["region"], value=json.dumps(event))
        producer.poll(0)
    producer.flush(30)
    print(f"[produce_events] отправлено {BATCH_SIZE} событий в {TOPIC}")


def verify_counts():
    from confluent_kafka import Consumer, TopicPartition
    from impala.dbapi import connect

    # сколько всего сообщений в топике (сумма end-offset'ов всех партиций)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "verify_counts",
        "enable.auto.commit": False,
    })
    meta = consumer.list_topics(TOPIC, timeout=15)
    topic_total = 0
    for p in meta.topics[TOPIC].partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=15)
        topic_total += high
    consumer.close()

    conn = connect(host=os.environ.get("IMPALA_HOST", "impalad"), port=21050, auth_mechanism="NOSASL")
    cur = conn.cursor()
    cur.execute("INVALIDATE METADATA analytics.sales_events")
    cur.execute("SELECT count(*) FROM analytics.sales_events")
    table_total = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"[verify_counts] topic={topic_total}, iceberg(impala)={table_total}")
    assert table_total == topic_total, f"расхождение: топик {topic_total} != таблица {table_total}"


with DAG(
    dag_id="kafka_pipeline",
    schedule=None,                       # запуск вручную из UI
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["datastack", "demo", "kafka", "iceberg"],
) as dag:

    t1 = PythonOperator(
        task_id="produce_events",
        python_callable=produce_events,
    )

    t2 = spark_submit_task("spark_kafka_to_iceberg", "kafka_to_iceberg.py", packages="kafka")

    t3 = PythonOperator(
        task_id="verify_counts",
        python_callable=verify_counts,
    )

    t1 >> t2 >> t3
