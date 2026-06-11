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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "sales_events")
TABLE = "hive.analytics.sales_events"
CHECKPOINT = "s3a://warehouse/_checkpoints/sales_events"

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
S3_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

spark = (
    SparkSession.builder.appName("kafka_to_iceberg")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hive.type", "hive")
    .config("spark.sql.catalog.hive.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.hive.warehouse", "s3a://warehouse/")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

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
