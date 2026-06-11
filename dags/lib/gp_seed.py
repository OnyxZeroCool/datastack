"""
Общий seed Greenplum: таблица sales на 100к строк — стартовая точка
всех демо-пайплайнов. Идемпотентен: наполняет только пустую таблицу.
"""
from __future__ import annotations

import os

GP_DSN = os.environ.get("GP_DSN", "postgresql://gpadmin:gpadmin@greenplum:5432/gpadmin")


def seed_greenplum() -> None:
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
