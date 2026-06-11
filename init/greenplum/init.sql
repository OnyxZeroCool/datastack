-- Демо-схема в Greenplum: распределённая (по сегментам) таблица продаж.
-- Запуск:  make gp-init     (или: psql -h localhost -p 5432 -U gpadmin -f init.sql)

DROP TABLE IF EXISTS sales;

CREATE TABLE sales (
    id          bigint,
    sale_date   date,
    region      text,
    product     text,
    amount      numeric(12,2)
)
DISTRIBUTED BY (id);   -- MPP: строки шардируются по сегментам по hash(id)

INSERT INTO sales (id, sale_date, region, product, amount)
SELECT  g,
        date '2025-01-01' + (g % 180),
        (ARRAY['EMEA','APAC','AMER','LATAM'])[1 + (g % 4)],
        (ARRAY['widget','gadget','gizmo','doohickey'])[1 + (g % 4)],
        round((random()*1000)::numeric, 2)
FROM generate_series(1, 100000) AS g;

ANALYZE sales;

-- Проверка распределения по сегментам:
--   SELECT gp_segment_id, count(*) FROM sales GROUP BY 1 ORDER BY 1;
SELECT gp_segment_id, count(*) AS rows FROM sales GROUP BY 1 ORDER BY 1;
