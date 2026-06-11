-- Откат publish-слоя медальона. Данные не жалко: источник истины — Iceberg,
-- повторный прогон sales_medallion наполнит всё заново.

DROP TABLE IF EXISTS analytics_mart.sales_by_region_product_monthly;
DROP TABLE IF EXISTS analytics_mart.sales_monthly;
DROP TABLE IF EXISTS analytics_mart.sales_quality;
DROP DATABASE IF EXISTS analytics_mart;
