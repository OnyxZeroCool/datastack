"""
Медальон, слой core: landing.sales + landing.sales_events → hive.core.sales.

Здесь сырьё двух источников приводится к единой схеме и чистится:
  - общая схема: id bigint, sale_date date (для kafka — cast(ts as date)),
    region в нижнем регистре, amount decimal(12,2);
  - дедуп по (_source, id): из дублей выживает строка с максимальным _loaded_at
    (producer kafka генерит id 1..N каждый запуск — дубли там норма);
  - флаг is_valid (id есть, 0 < amount <= 1000) — невалидные строки не выкидываем,
    их отфильтрует mart, а посчитает mart.sales_quality;
  - sale_month — для месячных агрегатов в mart.

Запуск (внутри контейнера spark-iceberg):
    docker compose exec spark-iceberg spark-submit /home/iceberg/jobs/sales__core__build.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib.session import get_spark

TABLE = "hive.core.sales"

spark = get_spark("sales__core__build")
spark.sql("CREATE DATABASE IF NOT EXISTS hive.core")

df = spark.sql(
    """
    WITH unified AS (
        SELECT CAST(id AS BIGINT)            AS id,
               sale_date,
               lower(region)                 AS region,
               product,
               CAST(amount AS DECIMAL(12,2)) AS amount,
               _source, _loaded_at
        FROM hive.landing.sales
        UNION ALL
        SELECT CAST(id AS BIGINT),
               CAST(ts AS DATE),
               lower(region),
               product,
               CAST(amount AS DECIMAL(12,2)),
               _source, _loaded_at
        FROM hive.landing.sales_events
    ),
    dedup AS (
        SELECT *,
               row_number() OVER (PARTITION BY _source, id ORDER BY _loaded_at DESC) AS rn
        FROM unified
    )
    SELECT id, sale_date, region, product, amount,
           (id IS NOT NULL AND amount > 0 AND amount <= 1000) AS is_valid,
           CAST(date_trunc('month', sale_date) AS DATE)       AS sale_month,
           _source, _loaded_at
    FROM dedup
    WHERE rn = 1
    """
)

# core тоже пересобирается целиком — никаких delete-файлов
df.writeTo(TABLE).using("iceberg").createOrReplace()

total = spark.table(TABLE).count()
valid = spark.table(TABLE).where("is_valid").count()
print(f"[core_build] строк в {TABLE}: {total}, из них is_valid: {valid}")
spark.stop()
