"""
Медальон, слой landing: Greenplum.sales → hive.landing.sales (1:1 + метаданные).

Сырьё не трогаем: колонки как в источнике, плюс служебные `_loaded_at`
(когда загрузили) и `_source` (откуда). Вся чистка — дальше, в core.

Запуск (внутри контейнера spark-iceberg):
    docker compose exec spark-iceberg spark-submit /home/iceberg/jobs/sales__landing__from_gp.py
Из Airflow — DAG sales_medallion (добавляет --packages с postgres-драйвером).
"""
import os
import sys

from pyspark.sql.functions import current_timestamp, lit

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib.session import get_spark

GP_HOST = os.environ.get("GP_HOST", "greenplum")
GP_PORT = os.environ.get("GP_PORT", "5432")
GP_DB = os.environ.get("GP_DB", "gpadmin")
GP_USER = os.environ.get("GP_USER", "gpadmin")
GP_PASSWORD = os.environ.get("GP_PASSWORD", "gpadmin")

TABLE = "hive.landing.sales"

spark = get_spark("sales__landing__from_gp")
spark.sql("CREATE DATABASE IF NOT EXISTS hive.landing")

df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:postgresql://{GP_HOST}:{GP_PORT}/{GP_DB}")
    .option("dbtable", "sales")
    .option("user", GP_USER)
    .option("password", GP_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
    .withColumn("_loaded_at", current_timestamp())
    .withColumn("_source", lit("greenplum"))
)

# landing перезаписывается целиком (snapshot-загрузка) — таблица никогда
# не получает delete-файлов, и её спокойно читает icebergS3() из ClickHouse
df.writeTo(TABLE).using("iceberg").createOrReplace()

print(f"[landing_from_gp] записано строк в {TABLE}: {spark.table(TABLE).count()}")
spark.stop()
