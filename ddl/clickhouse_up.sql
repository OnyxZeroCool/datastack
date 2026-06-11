-- Publish-слой медальона: приёмники в ClickHouse для витрин hive.mart.*
-- Наполняет таска publish_clickhouse DAG'а sales_medallion (TRUNCATE + INSERT
-- ... SELECT FROM icebergS3(...)). Идемпотентен: IF NOT EXISTS.
-- Применить:  make ddl-ch-up      Откатить:  make ddl-ch-down

CREATE DATABASE IF NOT EXISTS analytics_mart;

CREATE TABLE IF NOT EXISTS analytics_mart.sales_by_region_product_monthly (
    sale_month   Date,
    _source      String,
    region       String,
    product      String,
    orders_cnt   UInt64,
    total_amount Decimal(18, 2),
    avg_amount   Decimal(18, 2)
) ENGINE = MergeTree
ORDER BY (sale_month, region, product);

CREATE TABLE IF NOT EXISTS analytics_mart.sales_monthly (
    sale_month   Date,
    _source      String,
    orders_cnt   UInt64,
    total_amount Decimal(18, 2),
    regions_cnt  UInt64,
    products_cnt UInt64
) ENGINE = MergeTree
ORDER BY (sale_month);

CREATE TABLE IF NOT EXISTS analytics_mart.sales_quality (
    _source                 String,
    month                   Date,
    rows_cnt                UInt64,
    null_id_cnt             UInt64,
    null_region_cnt         UInt64,
    null_amount_cnt         UInt64,
    dup_id_cnt              UInt64,
    amount_out_of_range_cnt UInt64,
    checked_at              DateTime
) ENGINE = MergeTree
ORDER BY (month, _source);
