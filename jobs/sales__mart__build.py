"""
Медальон, слой mart: hive.core.sales → две витрины для BI.

  - mart.sales_by_region_product_monthly — месячный агрегат по регионам/продуктам;
  - mart.sales_monthly                   — помесячный итог по источникам.

Обе считаются только по is_valid-строкам core. Витрины пересобираются
целиком (createOrReplace) — delete-файлов не бывает, поэтому publish-слой
в ClickHouse читает их через icebergS3() без сюрпризов.

Запуск (внутри контейнера spark-iceberg):
    docker compose exec spark-iceberg spark-submit /home/iceberg/jobs/sales__mart__build.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib.session import get_spark

spark = get_spark("sales__mart__build")
spark.sql("CREATE DATABASE IF NOT EXISTS hive.mart")

by_region_product = spark.sql(
    """
    SELECT sale_month, _source, region, product,
           count(*)                       AS orders_cnt,
           CAST(sum(amount) AS DECIMAL(18,2)) AS total_amount,
           CAST(avg(amount) AS DECIMAL(18,2)) AS avg_amount
    FROM hive.core.sales
    WHERE is_valid
    GROUP BY sale_month, _source, region, product
    """
)
by_region_product.writeTo("hive.mart.sales_by_region_product_monthly").using("iceberg").createOrReplace()

monthly = spark.sql(
    """
    SELECT sale_month, _source,
           count(*)                       AS orders_cnt,
           CAST(sum(amount) AS DECIMAL(18,2)) AS total_amount,
           count(DISTINCT region)         AS regions_cnt,
           count(DISTINCT product)        AS products_cnt
    FROM hive.core.sales
    WHERE is_valid
    GROUP BY sale_month, _source
    """
)
monthly.writeTo("hive.mart.sales_monthly").using("iceberg").createOrReplace()

for t in ("sales_by_region_product_monthly", "sales_monthly"):
    print(f"[mart_build] строк в hive.mart.{t}: {spark.table(f'hive.mart.{t}').count()}")
spark.stop()
