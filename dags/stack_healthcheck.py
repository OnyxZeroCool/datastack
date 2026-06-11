"""
Хелсчек стека: дёргает каждый движок и падает, если кто-то не отвечает.

Задачи независимые, идут параллельно — по красным сразу видно, что лежит.
Адреса берутся из env (заданы в docker-compose, секция airflow-common).
Удобно прогонять сразу после подъёма стека.
"""
from __future__ import annotations

import os
import socket
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


def check_minio():
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
    )
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    print("MinIO живой, бакеты:", buckets)
    assert "warehouse" in buckets, "нет бакета warehouse — отработал ли mc-init?"


def check_greenplum():
    import psycopg2
    conn = psycopg2.connect(os.environ["GP_DSN"])
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM gp_segment_configuration WHERE role='p'")
        print("Greenplum живой, primary-сегментов:", cur.fetchone()[0])
    conn.close()


def check_hive_metastore():
    host = os.environ.get("HMS_HOST", "hive-metastore")
    with socket.create_connection((host, 9083), timeout=5):
        print(f"Hive Metastore отвечает на {host}:9083")


def check_clickhouse():
    import clickhouse_connect
    ch = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"), port=8123,
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"),
    )
    print("ClickHouse живой, версия:", ch.server_version)


def check_impala():
    from impala.dbapi import connect
    conn = connect(host=os.environ.get("IMPALA_HOST", "impalad"), port=21050, auth_mechanism="NOSASL")
    cur = conn.cursor()
    cur.execute("SELECT 1")
    assert cur.fetchone()[0] == 1
    print("Impala живой")
    cur.close(); conn.close()


CHECKS = {
    "minio": check_minio,
    "greenplum": check_greenplum,
    "hive_metastore": check_hive_metastore,
    "clickhouse": check_clickhouse,
    "impala": check_impala,
}

with DAG(
    dag_id="stack_healthcheck",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["datastack", "healthcheck"],
) as dag:
    # каждая проверка — отдельная задача без зависимостей (бегут параллельно)
    for name, fn in CHECKS.items():
        PythonOperator(task_id=f"check_{name}", python_callable=fn)
