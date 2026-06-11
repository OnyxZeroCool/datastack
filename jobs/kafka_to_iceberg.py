"""
PySpark Structured Streaming: Kafka (sales_events) → Iceberg-таблица в MinIO.

Триггер availableNow: джоба дочитывает весь накопившийся бэклог топика и
завершается — удобно для запуска из Airflow и ноутбуков (микробатч-режим
«стриминг по расписанию»). Прогресс хранится в checkpoint'е на S3, поэтому
повторный запуск читает только новые события — exactly-once в Iceberg.

Запуск (внутри контейнера spark-iceberg):
    docker compose exec spark-iceberg spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
        /home/iceberg/jobs/kafka_to_iceberg.py

Версия пакета spark-sql-kafka должна совпадать с версией Spark в образе
(проверь `spark-submit --version`).
"""
import os
import sys

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

# общий get_spark лежит рядом в lib/ — путь добавляем от файла,
# чтобы работало под обоими mount'ами (/home/iceberg/jobs и /opt/airflow/jobs)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib.session import get_spark

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "sales_events")
TABLE = "hive.analytics.sales_events"
CHECKPOINT = "s3a://warehouse/_checkpoints/sales_events"

spark = get_spark("kafka_to_iceberg")

event_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("ts", TimestampType()),
        StructField("region", StringType()),
        StructField("product", StringType()),
        StructField("amount", DoubleType()),
    ]
)

spark.sql("CREATE DATABASE IF NOT EXISTS hive.analytics")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id INT, ts TIMESTAMP, region STRING, product STRING, amount DOUBLE,
        kafka_partition INT, kafka_offset BIGINT
    ) USING iceberg
    """
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

events = (
    raw.select(
        from_json(col("value").cast("string"), event_schema).alias("e"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
    )
    .select("e.*", "kafka_partition", "kafka_offset")
)

query = (
    events.writeStream.format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TABLE)
)
query.awaitTermination()

total = spark.table(TABLE).count()
print(f"[kafka_to_iceberg] бэклог дочитан, всего строк в {TABLE}: {total}")

spark.stop()
