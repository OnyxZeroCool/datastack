"""
Сквозной тест с проверками: данные проходят весь стек, на каждом шаге сверяется сумма.

  seed_greenplum → spark_etl → verify_across_engines

Если где-то цифры разъехались — verify падает, и DAG краснеет.
Нужен полный стек (Greenplum, Spark, Impala, ClickHouse). spark_etl на первом
прогоне тянет jar'ы через --packages, поэтому нужен интернет (либо прогнать
make spark-demo заранее).
"""
from __future__ import annotations

import os
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from lib.gp_seed import GP_DSN, seed_greenplum
from lib.spark_task import spark_submit_task

TOLERANCE = 0.01


def verify_across_engines():
    import psycopg2, clickhouse_connect
    from impala.dbapi import connect

    # источник
    gp = psycopg2.connect(GP_DSN)
    with gp.cursor() as cur:
        cur.execute("SELECT round(sum(amount), 2) FROM sales")
        gp_total = float(cur.fetchone()[0])

    # Iceberg глазами Impala (таблицу создал spark_etl).
    # INVALIDATE — страховка: event polling обычно сам успевает за секунды.
    imp = connect(host=os.environ.get("IMPALA_HOST", "impalad"), port=21050, auth_mechanism="NOSASL").cursor()
    imp.execute("INVALIDATE METADATA")
    imp.execute("SELECT round(sum(total_amount), 2) FROM analytics.sales_by_region_product")
    impala_total = float(imp.fetchall()[0][0])

    ch = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"), port=8123,
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"))
    # Greenplum глазами ClickHouse (независимая сверка источника)
    ch_gp_total = float(ch.query(
        "SELECT round(sum(amount),2) FROM postgresql("
        "'greenplum:5432','gpadmin','sales','gpadmin','gpadmin')").result_rows[0][0])
    # Iceberg глазами ClickHouse — напрямую из MinIO, мимо метастора
    ch_ice_total = float(ch.query(
        "SELECT round(sum(total_amount),2) FROM icebergS3(minio, "
        "filename='analytics.db/sales_by_region_product')").result_rows[0][0])

    print(f"Greenplum={gp_total}  Impala={impala_total}  "
          f"CH(gp)={ch_gp_total}  CH(iceberg)={ch_ice_total}")
    assert abs(gp_total - impala_total) < TOLERANCE, f"Impala разошлась: {impala_total} != {gp_total}"
    assert abs(gp_total - ch_gp_total) < TOLERANCE, f"ClickHouse(GP) разошёлся: {ch_gp_total} != {gp_total}"
    assert abs(gp_total - ch_ice_total) < TOLERANCE, f"ClickHouse(Iceberg) разошёлся: {ch_ice_total} != {gp_total}"
    print("ОК — суммы сошлись во всех движках.")


with DAG(
    dag_id="e2e_pipeline_test",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["datastack", "test", "e2e"],
) as dag:

    t1 = PythonOperator(task_id="seed_greenplum", python_callable=seed_greenplum)
    t2 = spark_submit_task("spark_etl", "gp_to_iceberg.py", packages="jdbc")
    t3 = PythonOperator(task_id="verify_across_engines", python_callable=verify_across_engines)

    t1 >> t2 >> t3
