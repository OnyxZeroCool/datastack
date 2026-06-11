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

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from lib.gp_seed import seed_greenplum
from lib.spark_task import spark_submit_task

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

    t2 = spark_submit_task("spark_gp_to_iceberg", "gp_to_iceberg.py", packages="jdbc")

    t1 >> t2
