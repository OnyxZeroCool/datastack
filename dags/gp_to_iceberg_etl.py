"""
Демо-DAG: оркестрация ETL Greenplum → Iceberg (MinIO) средствами Airflow.

  seed_greenplum  → spark_gp_to_iceberg

1) seed_greenplum     — создаёт/наполняет таблицу sales в Greenplum (psycopg2).
2) spark_gp_to_iceberg — spark-submit джобы /opt/airflow/jobs/gp_to_iceberg.py
                         на Spark-кластер spark://spark-iceberg:7077.

ВНИМАНИЕ: spark-submit из Airflow тянет iceberg/hadoop-aws/postgres jar'ы
через --packages (Ivy) → при первом запуске нужен интернет. Для оффлайна
запускай джобу внутри контейнера spark-iceberg (см. jobs/gp_to_iceberg.py).
"""
from __future__ import annotations

import os
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

GP_DSN = os.environ.get("GP_DSN", "postgresql://gpadmin:gpadmin@greenplum:5432/gpadmin")

SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1",  # = версия в образе spark-iceberg
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.postgresql:postgresql:42.7.3",
])


def seed_greenplum():
    import psycopg2
    ddl = """
        CREATE TABLE IF NOT EXISTS sales (
            id bigint, sale_date date, region text, product text, amount numeric(12,2)
        ) DISTRIBUTED BY (id);
    """
    seed = """
        INSERT INTO sales
        SELECT g,
               date '2025-01-01' + (g %% 180),
               (ARRAY['EMEA','APAC','AMER','LATAM'])[1 + (g %% 4)],
               (ARRAY['widget','gadget','gizmo','doohickey'])[1 + (g %% 4)],
               round((random()*1000)::numeric, 2)
        FROM generate_series(1, 100000) AS g;
    """
    conn = psycopg2.connect(GP_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute("SELECT count(*) FROM sales;")
        if cur.fetchone()[0] == 0:
            cur.execute(seed)
        cur.execute("ANALYZE sales;")
        cur.execute("SELECT count(*) FROM sales;")
        print(f"[seed_greenplum] sales rows = {cur.fetchone()[0]}")
    conn.close()


with DAG(
    dag_id="gp_to_iceberg_etl",
    schedule=None,                       # запуск вручную из UI
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["datastack", "demo", "iceberg", "greenplum"],
) as dag:

    t1 = PythonOperator(
        task_id="seed_greenplum",
        python_callable=seed_greenplum,
    )

    t2 = BashOperator(
        task_id="spark_gp_to_iceberg",
        bash_command=(
            "spark-submit "
            "--master spark://spark-iceberg:7077 "
            f"--packages {SPARK_PACKAGES} "
            "/opt/airflow/jobs/gp_to_iceberg.py"
        ),
    )

    t1 >> t2
