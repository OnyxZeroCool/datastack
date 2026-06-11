"""
Общая фабрика SparkSession для всех джоб стека.

Один и тот же блок iceberg+s3a конфигов раньше копипастился в каждую джобу —
теперь он живёт здесь. Джобы остаются самодостаточными: конфиги задаются
в коде, поэтому работают и при запуске внутри контейнера spark-iceberg,
и при spark-submit из Airflow (которому добавляются --packages).
"""
import os

from pyspark.sql import SparkSession

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
S3_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
HMS_URI = os.environ.get("HMS_URI", "thrift://hive-metastore:9083")


def get_spark(app_name: str) -> SparkSession:
    """SparkSession с каталогом `hive` (Iceberg через Hive Metastore) и s3a→MinIO."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hive.type", "hive")
        .config("spark.sql.catalog.hive.uri", HMS_URI)
        .config("spark.sql.catalog.hive.warehouse", "s3a://warehouse/")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
