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
import sys

# общий get_spark лежит рядом в lib/ — путь добавляем от файла,
# чтобы работало под обоими mount'ами (/home/iceberg/jobs и /opt/airflow/jobs)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib.session import get_spark

GP_HOST = os.environ.get("GP_HOST", "greenplum")
GP_PORT = os.environ.get("GP_PORT", "5432")
GP_DB = os.environ.get("GP_DB", "gpadmin")
GP_USER = os.environ.get("GP_USER", "gpadmin")
GP_PASSWORD = os.environ.get("GP_PASSWORD", "gpadmin")

spark = get_spark("gp_to_iceberg")

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
