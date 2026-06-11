"""
Медальон, слой landing: hive.analytics.sales_events → hive.landing.sales_events.

Источник наполняет стриминговый ингест (DAG kafka_pipeline / jobs/kafka_to_iceberg.py),
здесь — обычное батч-чтение его результата + служебные `_loaded_at`/`_source`.
Если стриминг ни разу не запускался (профиль kafka выключен) — создаём пустую
таблицу с правильной схемой, чтобы core-слой и весь DAG оставались зелёными.

Учебный нюанс: producer генерит id 1..N при каждом запуске, поэтому в landing
копятся легитимные дубли по id — их схлопнет dedup в core, а покажет
dup_id_cnt в mart.sales_quality.

Запуск (внутри контейнера spark-iceberg):
    docker compose exec spark-iceberg spark-submit /home/iceberg/jobs/sales__landing__from_kafka.py
"""
import os
import sys

from pyspark.sql.functions import current_timestamp, lit

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib.session import get_spark

SOURCE = "hive.analytics.sales_events"
TABLE = "hive.landing.sales_events"

spark = get_spark("sales__landing__from_kafka")
spark.sql("CREATE DATABASE IF NOT EXISTS hive.landing")

if spark.catalog.tableExists(SOURCE):
    df = (
        spark.table(SOURCE)
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source", lit("kafka"))
    )
    # snapshot-перезапись: delete-файлов не бывает, icebergS3() в CH читает без сюрпризов
    df.writeTo(TABLE).using("iceberg").createOrReplace()
else:
    # источника нет — пустая таблица с той же схемой; существующий landing не трогаем
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id INT, ts TIMESTAMP, region STRING, product STRING, amount DOUBLE,
            kafka_partition INT, kafka_offset BIGINT,
            _loaded_at TIMESTAMP, _source STRING
        ) USING iceberg
        """
    )
    print(f"[landing_from_kafka] источника {SOURCE} нет — гарантирована пустая {TABLE}")

print(f"[landing_from_kafka] строк в {TABLE}: {spark.table(TABLE).count()}")
spark.stop()
