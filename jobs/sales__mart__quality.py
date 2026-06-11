"""
Медальон, витрина качества: landing.* → hive.mart.sales_quality.

DQ-метрики считаются по landing ДО дедупа и фильтров — иначе витрина
показывала бы качество уже почищенных данных, а не сырья:
  rows_cnt, null_id_cnt, null_region_cnt, null_amount_cnt,
  dup_id_cnt (строки-дубли по id внутри источника/месяца),
  amount_out_of_range_cnt (amount <= 0 или > 1000) — по (_source, month).

На демо-данных dup_id_cnt > 0 для kafka после второго прогона kafka_pipeline
(producer каждый раз генерит id 1..N) — это ожидаемая картинка.

Запуск (внутри контейнера spark-iceberg):
    docker compose exec spark-iceberg spark-submit /home/iceberg/jobs/sales__mart__quality.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib.session import get_spark

TABLE = "hive.mart.sales_quality"

spark = get_spark("sales__mart__quality")
spark.sql("CREATE DATABASE IF NOT EXISTS hive.mart")

df = spark.sql(
    """
    WITH landing AS (
        SELECT _source,
               CAST(date_trunc('month', sale_date) AS DATE) AS month,
               id, region, amount
        FROM hive.landing.sales
        UNION ALL
        SELECT _source,
               CAST(date_trunc('month', CAST(ts AS DATE)) AS DATE),
               id, region, amount
        FROM hive.landing.sales_events
    )
    SELECT _source, month,
           count(*)                                            AS rows_cnt,
           count_if(id IS NULL)                                AS null_id_cnt,
           count_if(region IS NULL)                            AS null_region_cnt,
           count_if(amount IS NULL)                            AS null_amount_cnt,
           count(*) - count(DISTINCT id)                       AS dup_id_cnt,
           count_if(amount IS NOT NULL
                    AND (amount <= 0 OR amount > 1000))        AS amount_out_of_range_cnt,
           current_timestamp()                                 AS checked_at
    FROM landing
    GROUP BY _source, month
    """
)
df.writeTo(TABLE).using("iceberg").createOrReplace()

print(f"[mart_quality] строк в {TABLE}: {spark.table(TABLE).count()}")
spark.table(TABLE).orderBy("_source", "month").show(50, truncate=False)
spark.stop()
