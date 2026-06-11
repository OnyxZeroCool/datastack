"""
Учебный DAG: основные паттерны Airflow на живом стеке.

Тут не про данные (см. gp_to_iceberg_etl / e2e_pipeline_test), а про сам Airflow:

  wait_for_table (сенсор) → extract_regions → region_stats (×N динамически)
      → aggregate → flaky_step (retries) → weekday_or_weekend (ветвление)
      → [weekday_report | weekend_report] → done (trigger_rule)

Паттерны и где они в коде:
- расписание + catchup ........ параметры DAG (запускается и руками)
- сенсор ...................... PythonSensor, mode='reschedule' (не держит слот воркера)
- XCom ........................ @task возвращает значение — оно едет дальше само
- dynamic task mapping ........ region_stats.expand(region=...) — задача на каждый регион
- TaskGroup ................... группа reports в UI
- ветвление ................... @task.branch выбирает одну из веток по дню недели
- retries ..................... flaky_step падает на 1-й попытке и проходит на 2-й
- trigger_rule ................ done живёт после ЛЮБОЙ из веток (none_failed_min_one_success)

Нужны Greenplum (с данными: make gp-init) и MinIO. Spark не нужен — DAG лёгкий.
"""
from __future__ import annotations

import os
import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

GP_DSN = os.environ.get("GP_DSN", "postgresql://gpadmin:gpadmin@greenplum:5432/gpadmin")


def _iceberg_table_in_minio() -> bool:
    """Poke-функция сенсора: появилась ли Iceberg-таблица в MinIO."""
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
    )
    resp = s3.list_objects_v2(
        Bucket="warehouse", Prefix="analytics.db/sales_by_region_product/", MaxKeys=1)
    return resp.get("KeyCount", 0) > 0


@dag(
    dag_id="airflow_patterns",
    schedule="@daily",            # каждый день в полночь; руками тоже можно
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,                # не догонять пропущенные дни с 2025 года
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(seconds=10),
    },
    tags=["datastack", "learning"],
)
def airflow_patterns():

    # --- Сенсор: ждём внешнее условие, не занимая воркера (mode='reschedule') ---
    wait_for_table = PythonSensor(
        task_id="wait_for_table",
        python_callable=_iceberg_table_in_minio,
        poke_interval=15,
        timeout=300,
        mode="reschedule",
    )

    # --- XCom без церемоний: return из @task едет в следующую задачу ---
    @task
    def extract_regions() -> list[str]:
        import psycopg2
        with psycopg2.connect(GP_DSN) as conn, conn.cursor() as cur:
            cur.execute("SELECT DISTINCT region FROM sales ORDER BY region")
            regions = [r[0] for r in cur.fetchall()]
        print("регионы:", regions)
        return regions

    # --- Dynamic task mapping: по задаче на каждый регион, число известно в рантайме ---
    @task
    def region_stats(region: str) -> dict:
        import psycopg2
        with psycopg2.connect(GP_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*), round(sum(amount), 2) FROM sales WHERE region = %s",
                (region,))
            cnt, total = cur.fetchone()
        return {"region": region, "rows": cnt, "total": float(total)}

    # --- Собрать результаты всех замапленных задач (это снова XCom) ---
    @task
    def aggregate(stats: list[dict]) -> float:
        grand = round(sum(s["total"] for s in stats), 2)
        for s in stats:
            print(f"{s['region']:>6}: {s['rows']} строк, {s['total']}")
        print("итого:", grand)
        return grand

    # --- Retries: первая попытка падает, вторая проходит. В UI видно обе. ---
    @task
    def flaky_step(grand_total: float) -> float:
        from airflow.operators.python import get_current_context
        ti = get_current_context()["ti"]
        if ti.try_number < 2:
            raise RuntimeError("Эмуляция сбоя: упадём, retry через 10 секунд всё починит")
        print(f"попытка №{ti.try_number} — ок, сумма {grand_total}")
        return grand_total

    # --- Ветвление: вернуть task_id той ветки, которая должна выполниться ---
    @task.branch
    def weekday_or_weekend() -> str:
        import datetime
        is_weekend = datetime.datetime.now().weekday() >= 5
        return "reports.weekend_report" if is_weekend else "reports.weekday_report"

    # --- TaskGroup: в UI ветки сложены в одну группу ---
    with TaskGroup(group_id="reports") as reports:
        @task
        def weekday_report():
            print("будний отчёт: полный прогон")

        @task
        def weekend_report():
            print("выходной отчёт: сокращённый")

        weekday_report()
        weekend_report()

    # --- trigger_rule: дефолтный all_success здесь не подходит — одна из веток
    #     всегда skipped. none_failed_min_one_success живёт после любой из них. ---
    done = EmptyOperator(task_id="done", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    regions = extract_regions()
    stats = region_stats.expand(region=regions)
    grand = aggregate(stats)
    branch = weekday_or_weekend()

    wait_for_table >> regions
    grand >> flaky_step(grand) >> branch >> reports >> done


airflow_patterns()
