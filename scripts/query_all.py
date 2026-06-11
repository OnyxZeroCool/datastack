"""
Пример: один Python-процесс ходит во все движки стека.
Запуск:  make py-run            (= docker compose exec python python scripts/query_all.py)

Каждый блок изолирован try/except — если профиль движка не поднят, скрипт
не падает целиком, а печатает, что сервис недоступен.
"""
import os


def section(title):
    print(f"\n{'='*60}\n{title}\n{'='*60}")


# ---------------------------------------------------------------- Greenplum
def greenplum():
    section("Greenplum (psycopg2)")
    import psycopg2
    dsn = os.environ.get("GP_DSN", "postgresql://gpadmin:gpadmin@greenplum:5432/gpadmin")
    conn = psycopg2.connect(dsn)
    with conn.cursor() as cur:
        cur.execute("SELECT gp_segment_id, count(*) FROM sales GROUP BY 1 ORDER BY 1;")
        for seg, cnt in cur.fetchall():
            print(f"  segment {seg}: {cnt} rows")
    conn.close()


# ---------------------------------------------------------------- ClickHouse
def clickhouse():
    section("ClickHouse (clickhouse-connect)")
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=8123,
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"),
    )
    print("  version:", client.server_version)
    # Прямое чтение Greenplum из ClickHouse через postgresql() table function:
    rows = client.query(
        "SELECT region, count() FROM postgresql("
        "'greenplum:5432','gpadmin','sales','gpadmin','gpadmin') "
        "GROUP BY region ORDER BY region"
    ).result_rows
    for region, cnt in rows:
        print(f"  {region}: {cnt}")


# ---------------------------------------------------------------- Impala
def impala():
    section("Impala (impyla, NOSASL)")
    from impala.dbapi import connect
    conn = connect(
        host=os.environ.get("IMPALA_HOST", "impalad"),
        port=21050,
        auth_mechanism="NOSASL",
    )
    cur = conn.cursor()
    cur.execute("INVALIDATE METADATA")
    cur.execute("SHOW DATABASES")
    print("  databases:", [r[0] for r in cur.fetchall()])
    cur.close()
    conn.close()


# ------------------------------------------------- Iceberg напрямую (pyiceberg)
def iceberg():
    section("Iceberg через Hive Metastore + MinIO (pyiceberg, без Spark)")
    from pyiceberg.catalog import load_catalog
    cat = load_catalog(
        "hive",
        **{
            "type": "hive",
            "uri": "thrift://hive-metastore:9083",
            "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "s3.path-style-access": "true",
        },
    )
    print("  namespaces:", cat.list_namespaces())


if __name__ == "__main__":
    for fn in (greenplum, clickhouse, impala, iceberg):
        try:
            fn()
        except Exception as e:  # noqa: BLE001 — демо: не валим весь скрипт
            print(f"  [пропущено: {fn.__name__}] {type(e).__name__}: {e}")
    print("\nГотово.")
