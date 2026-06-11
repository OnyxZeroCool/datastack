"""
Медальон-пайплайн: landing → core → mart → publish в ClickHouse + сверки.

  seed_greenplum → landing_from_gp ─┐
  landing_from_kafka ───────────────┴→ core_build → mart_build → mart_quality
                                        → publish_clickhouse → verify_publish

Слои (всё в Iceberg, схемы hive.landing / hive.core / hive.mart):
  landing — сырьё 1:1 + _loaded_at/_source (jobs/sales__landing__*.py);
  core    — единая схема, дедуп, флаг is_valid (jobs/sales__core__build.py);
  mart    — витрины + качество данных (jobs/sales__mart__*.py);
  publish — копия витрин в ClickHouse MergeTree: BI крутит дашборды по CH,
            не дёргая Spark. CH сам читает parquet витрин из MinIO через
            icebergS3() — без тяжёлых insert'ов через драйвер.

Все spark-таски сидят в pool'е spark_pool (limit 1) — две параллельные
spark-джобы на этой машине кончаются OOM'ом, поэтому landing_from_kafka
честно ждёт в queued, пока едет landing_from_gp.

Работает и без Kafka-профиля: если analytics.sales_events нет, landing_from_kafka
создаст пустую таблицу, и пайплайн пройдёт только по greenplum-ветке.
"""
from __future__ import annotations

import os

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from lib.gp_seed import GP_DSN, seed_greenplum
from lib.spark_task import spark_submit_task

TOLERANCE = 0.01

# что копируем в ClickHouse: витрина → колонки (порядок = DDL ниже)
MART_TABLES = {
    "sales_by_region_product_monthly":
        "sale_month, _source, region, product, orders_cnt, total_amount, avg_amount",
    "sales_monthly":
        "sale_month, _source, orders_cnt, total_amount, regions_cnt, products_cnt",
    "sales_quality":
        "_source, month, rows_cnt, null_id_cnt, null_region_cnt, null_amount_cnt, "
        "dup_id_cnt, amount_out_of_range_cnt, checked_at",
}

# те же определения, что в ddl/clickhouse_up.sql (каноничная копия — там);
# таска создаёт таблицы сама, чтобы DAG был зелёным без ручного make ddl-ch-up
PUBLISH_DDL = [
    "CREATE DATABASE IF NOT EXISTS analytics_mart",
    """CREATE TABLE IF NOT EXISTS analytics_mart.sales_by_region_product_monthly (
        sale_month Date, _source String, region String, product String,
        orders_cnt UInt64, total_amount Decimal(18,2), avg_amount Decimal(18,2)
    ) ENGINE = MergeTree ORDER BY (sale_month, region, product)""",
    """CREATE TABLE IF NOT EXISTS analytics_mart.sales_monthly (
        sale_month Date, _source String, orders_cnt UInt64,
        total_amount Decimal(18,2), regions_cnt UInt64, products_cnt UInt64
    ) ENGINE = MergeTree ORDER BY (sale_month)""",
    """CREATE TABLE IF NOT EXISTS analytics_mart.sales_quality (
        _source String, month Date, rows_cnt UInt64, null_id_cnt UInt64,
        null_region_cnt UInt64, null_amount_cnt UInt64, dup_id_cnt UInt64,
        amount_out_of_range_cnt UInt64, checked_at DateTime
    ) ENGINE = MergeTree ORDER BY (month, _source)""",
]


def _ch_client():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"), port=8123,
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"))


def publish_clickhouse():
    ch = _ch_client()
    for ddl in PUBLISH_DDL:
        ch.command(ddl)
    for table, cols in MART_TABLES.items():
        # витрины пересобираются целиком → publish тоже полной заменой;
        # icebergS3 читает parquet витрины напрямую из MinIO (без Spark)
        ch.command(f"TRUNCATE TABLE analytics_mart.{table}")
        ch.command(
            f"INSERT INTO analytics_mart.{table} ({cols}) "
            f"SELECT {cols} FROM icebergS3(minio, filename='mart.db/{table}')"
        )
        rows = ch.query(f"SELECT count(*) FROM analytics_mart.{table}").result_rows[0][0]
        print(f"[publish_clickhouse] analytics_mart.{table}: {rows} строк")


def verify_publish():
    ch = _ch_client()

    # 1) MergeTree == витрина в Iceberg (то, что скопировали, == источнику)
    for table in ("sales_by_region_product_monthly", "sales_monthly"):
        mt = float(ch.query(
            f"SELECT sum(total_amount) FROM analytics_mart.{table}").result_rows[0][0])
        ice = float(ch.query(
            f"SELECT sum(total_amount) FROM icebergS3(minio, filename='mart.db/{table}')"
        ).result_rows[0][0])
        print(f"[verify_publish] {table}: MergeTree={mt} iceberg={ice}")
        assert abs(mt - ice) < TOLERANCE, f"{table}: {mt} != {ice}"
    q_mt = ch.query("SELECT count(*) FROM analytics_mart.sales_quality").result_rows[0][0]
    q_ice = ch.query(
        "SELECT count(*) FROM icebergS3(minio, filename='mart.db/sales_quality')").result_rows[0][0]
    assert q_mt == q_ice, f"sales_quality: {q_mt} != {q_ice}"

    # 2) сквозная сверка слоёв: витрина == core (is_valid) по greenplum-источнику
    mart_gp = float(ch.query(
        "SELECT sum(total_amount) FROM analytics_mart.sales_monthly "
        "WHERE _source = 'greenplum'").result_rows[0][0])
    core_gp = float(ch.query(
        "SELECT sum(amount) FROM icebergS3(minio, filename='core.db/sales') "
        "WHERE is_valid AND _source = 'greenplum'").result_rows[0][0])
    print(f"[verify_publish] greenplum-ветка: mart={mart_gp} core(valid)={core_gp}")
    assert abs(mart_gp - core_gp) < TOLERANCE, f"mart {mart_gp} != core {core_gp}"

    # 3) ...и не больше суммы источника (равенство не требуем: строки с amount=0.00
    #    легитимно отсеяны фильтром is_valid)
    user, pwd = GP_DSN.split("//")[1].split("@")[0].split(":")
    gp_total = float(ch.query(
        f"SELECT sum(amount) FROM postgresql('greenplum:5432','gpadmin','sales','{user}','{pwd}')"
    ).result_rows[0][0])
    print(f"[verify_publish] greenplum-источник: {gp_total}")
    assert core_gp <= gp_total + TOLERANCE, f"core {core_gp} > источника {gp_total}"
    print("ОК — publish сошёлся с витринами, витрины — с core и источником.")


with DAG(
    dag_id="sales_medallion",
    schedule=None,                       # запуск вручную из UI
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["datastack", "demo", "medallion", "iceberg", "clickhouse"],
) as dag:

    seed = PythonOperator(task_id="seed_greenplum", python_callable=seed_greenplum)
    landing_gp = spark_submit_task("landing_from_gp", "sales__landing__from_gp.py", packages="jdbc")
    landing_kafka = spark_submit_task("landing_from_kafka", "sales__landing__from_kafka.py")
    core = spark_submit_task("core_build", "sales__core__build.py")
    mart = spark_submit_task("mart_build", "sales__mart__build.py")
    quality = spark_submit_task("mart_quality", "sales__mart__quality.py")
    publish = PythonOperator(task_id="publish_clickhouse", python_callable=publish_clickhouse)
    verify = PythonOperator(task_id="verify_publish", python_callable=verify_publish)

    seed >> landing_gp
    [landing_gp, landing_kafka] >> core >> mart >> quality >> publish >> verify
