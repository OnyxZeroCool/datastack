"""
PySpark ETL: Greenplum (sales) → Iceberg-таблица в MinIO через Hive Metastore.

Самодостаточен: все нужные Spark-конфиги заданы в коде, поэтому работает
и при запуске внутри контейнера spark-iceberg, и при spark-submit из Airflow.

Локальный запуск (внутри контейнера spark-iceberg, все jar'ы уже есть):
    docker compose exec spark-iceberg spark-submit /home/iceberg/jobs/gp_to_iceberg.py

Из Airflow — см. dags/gp_to_iceberg_etl.py (передаёт --packages, т.к. pip-pyspark
не содержит iceberg/hadoop-aws/postgres jar'ов).
"""
import os
from pyspark.sql import SparkSession

GP_HOST = os.environ.get("GP_HOST", "greenplum")
GP_PORT = os.environ.get("GP_PORT", "5432")
GP_DB = os.environ.get("GP_DB", "gpadmin")
GP_USER = os.environ.get("GP_USER", "gpadmin")
GP_PASSWORD = os.environ.get("GP_PASSWORD", "gpadmin")

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
S3_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

spark = (
    SparkSession.builder.appName("gp_to_iceberg")
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

# 1) Читаем из Greenplum по JDBC (GP говорит на Postgres wire protocol)
jdbc_url = f"jdbc:postgresql://{GP_HOST}:{GP_PORT}/{GP_DB}"
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "sales")
    .option("user", GP_USER)
    .option("password", GP_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)
print(f"[gp_to_iceberg] прочитано строк из Greenplum.sales: {df.count()}")

# 2) Пишем агрегат как Iceberg-таблицу в MinIO, регистрируем в Hive Metastore
spark.sql("CREATE DATABASE IF NOT EXISTS hive.analytics")

agg = df.groupBy("region", "product").sum("amount").withColumnRenamed("sum(amount)", "total_amount")
(
    agg.writeTo("hive.analytics.sales_by_region_product")
    .using("iceberg")
    .createOrReplace()
)

print("[gp_to_iceberg] записана Iceberg-таблица hive.analytics.sales_by_region_product")
spark.table("hive.analytics.sales_by_region_product").show(truncate=False)

spark.stop()
