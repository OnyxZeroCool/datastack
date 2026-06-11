"""
Фабрика spark-submit тасок для DAG'ов — одно место вместо копипасты
SPARK_PACKAGES и bash_command в каждом DAG'е.

Все spark-таски сидят в pool'е `spark_pool` (limit 1, создаётся в
airflow-init): два параллельных spark-submit на этой машине — почти
гарантированный OOM, поэтому вторая таска честно ждёт в queued.
"""
from __future__ import annotations

from airflow.operators.bash import BashOperator

SPARK_MASTER = "spark://spark-iceberg:7077"

# версии прибиты к образу spark-iceberg: iceberg 1.8.1, Hadoop 3.3.4, Spark 3.5.5
_BASE_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
]
PACKAGES = {
    "base": _BASE_PACKAGES,
    "jdbc": _BASE_PACKAGES + ["org.postgresql:postgresql:42.7.3"],
    "kafka": _BASE_PACKAGES + ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"],
}


def spark_submit_task(task_id: str, job_file: str, packages: str = "base",
                      pool: str = "spark_pool") -> BashOperator:
    """BashOperator со spark-submit джобы из /opt/airflow/jobs.

    packages: 'base' (iceberg+s3a) | 'jdbc' (+postgres) | 'kafka' (+kafka-источник).
    При первом запуске jar'ы тянутся через --packages (Ivy) — нужен интернет.
    """
    pkgs = ",".join(PACKAGES[packages])
    return BashOperator(
        task_id=task_id,
        pool=pool,
        bash_command=(
            f"spark-submit --master {SPARK_MASTER} "
            f"--packages {pkgs} "
            f"/opt/airflow/jobs/{job_file}"
        ),
    )
