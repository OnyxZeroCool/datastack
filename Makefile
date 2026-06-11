# datastack — ярлыки. `make` или `make help` — список команд.
.DEFAULT_GOAL := help
COMPOSE := docker compose
MVN := https://repo1.maven.org/maven2
PG_JAR_VER := 42.7.3
PG_JAR_URL := $(MVN)/org/postgresql/postgresql/$(PG_JAR_VER)/postgresql-$(PG_JAR_VER).jar
# s3a-коннектор для Spark (версия Hadoop как в образе spark-iceberg — 3.3.4)
HADOOP_AWS_VER := 3.3.4
AWS_SDK_VER := 1.12.262
HADOOP_AWS_URL := $(MVN)/org/apache/hadoop/hadoop-aws/$(HADOOP_AWS_VER)/hadoop-aws-$(HADOOP_AWS_VER).jar
AWS_SDK_URL := $(MVN)/com/amazonaws/aws-java-sdk-bundle/$(AWS_SDK_VER)/aws-java-sdk-bundle-$(AWS_SDK_VER).jar

include .env
export

.PHONY: help fetch-jars up up-full up-clickhouse up-impala up-airflow up-python up-kafka up-nifi up-superset down down-v ps logs \
        gp-init psql-gp spark-sql spark-demo clickhouse-cli impala-shell py py-sh py-run urls validate \
        ddl-ch-up ddl-ch-down

help: ## Показать список команд
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(firstword $(MAKEFILE_LIST)) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

fetch-jars: ## Скачать jar'ы (Postgres JDBC для Spark-джоб + hadoop-aws/aws-sdk для Spark) — один раз до первого up
	@mkdir -p conf/hive/lib conf/spark/jars
	@test -f conf/hive/lib/postgresql-$(PG_JAR_VER).jar || \
		curl -fL "$(PG_JAR_URL)" -o conf/hive/lib/postgresql-$(PG_JAR_VER).jar
	@test -f conf/spark/jars/hadoop-aws-$(HADOOP_AWS_VER).jar || \
		curl -fL "$(HADOOP_AWS_URL)" -o conf/spark/jars/hadoop-aws-$(HADOOP_AWS_VER).jar
	@test -f conf/spark/jars/aws-java-sdk-bundle-$(AWS_SDK_VER).jar || \
		curl -fL "$(AWS_SDK_URL)" -o conf/spark/jars/aws-java-sdk-bundle-$(AWS_SDK_VER).jar
	@echo "OK: jar'ы на месте (conf/hive/lib, conf/spark/jars)"

up: fetch-jars ## Поднять ЯДРО (MinIO, HMS, Spark, Greenplum)
	$(COMPOSE) up -d

up-full: fetch-jars ## Поднять ВЕСЬ стек (+ ClickHouse, Impala, Airflow)
	$(COMPOSE) --profile full up -d

up-clickhouse: fetch-jars ## Ядро + ClickHouse
	$(COMPOSE) --profile clickhouse up -d

up-impala: fetch-jars ## Ядро + Impala
	$(COMPOSE) --profile impala up -d

up-airflow: fetch-jars ## Ядро + Airflow
	$(COMPOSE) --profile airflow up -d

up-python: fetch-jars ## Ядро + python-контейнер (со всеми клиентами)
	$(COMPOSE) --profile python up -d --build

up-kafka: fetch-jars ## Ядро + Kafka + kafka-ui
	$(COMPOSE) --profile kafka up -d

up-nifi: fetch-jars ## Ядро + NiFi (тяжёлый, стартует несколько минут)
	$(COMPOSE) --profile nifi up -d

up-superset: fetch-jars ## Ядро + Superset (BI)
	$(COMPOSE) --profile superset up -d --build

down: ## Остановить всё (контейнеры)
	$(COMPOSE) --profile full down

down-v: ## Остановить всё + удалить volumes (полный сброс данных)
	$(COMPOSE) --profile full down -v

ps: ## Статус сервисов
	$(COMPOSE) --profile full ps

logs: ## Логи (S=имя_сервиса для одного, иначе все). Пример: make logs S=hive-metastore
	$(COMPOSE) logs -f $(S)

gp-init: ## Завести базу gpadmin и демо-таблицу sales в Greenplum
	$(COMPOSE) exec -T -u gpadmin greenplum bash /init/bootstrap.sh

psql-gp: ## psql в Greenplum
	$(COMPOSE) exec -u gpadmin greenplum bash -lc 'source /usr/local/gpdb/greenplum_path.sh; psql -U $(GP_USER) -d $(GP_DB)'

spark-sql: ## Spark SQL shell (Iceberg-каталог hive)
	$(COMPOSE) exec spark-iceberg spark-sql

spark-demo: ## Запустить ETL Greenplum→Iceberg внутри контейнера spark-iceberg
	$(COMPOSE) exec spark-iceberg spark-submit /home/iceberg/jobs/gp_to_iceberg.py

clickhouse-cli: ## ClickHouse client
	$(COMPOSE) exec clickhouse clickhouse-client --password $(CLICKHOUSE_PASSWORD)

ddl-ch-up: ## Создать publish-слой analytics_mart в ClickHouse (идемпотентно)
	$(COMPOSE) exec -T clickhouse clickhouse-client --password $(CLICKHOUSE_PASSWORD) --multiquery < ddl/clickhouse_up.sql
	@echo "OK: analytics_mart создан"

ddl-ch-down: ## Удалить publish-слой analytics_mart из ClickHouse
	$(COMPOSE) exec -T clickhouse clickhouse-client --password $(CLICKHOUSE_PASSWORD) --multiquery < ddl/clickhouse_down.sql
	@echo "OK: analytics_mart удалён"

impala-shell: ## Impala shell (отдельный client-образ: в impalad самого шелла нет)
	docker run --rm -it --network datastack apache/impala:$${IMPALA_TAG:-4.4.1}-impala_quickstart_client impala-shell -i impalad

py: ## Python REPL в контейнере (все клиенты движков предустановлены)
	$(COMPOSE) exec python python

py-sh: ## Shell в python-контейнере
	$(COMPOSE) exec python bash

py-run: ## Пример: один Python ходит во все движки (scripts/query_all.py)
	$(COMPOSE) exec python python scripts/query_all.py

validate: ## Проверить синтаксис compose
	$(COMPOSE) --profile full config -q && echo "compose OK"

urls: ## Показать адреса UI
	@echo "MinIO console : http://localhost:$(MINIO_CONSOLE_PORT)  ($(MINIO_ROOT_USER)/$(MINIO_ROOT_PASSWORD))"
	@echo "Spark Master  : http://localhost:$(SPARK_MASTER_UI_PORT)"
	@echo "Spark App UI  : http://localhost:$(SPARK_APP_UI_PORT)   (во время job)"
	@echo "Spark History : http://localhost:$(SPARK_HISTORY_PORT)"
	@echo "Jupyter       : http://localhost:$(JUPYTER_PORT)"
	@echo "ClickHouse    : http://localhost:$(CLICKHOUSE_HTTP_PORT)/play"
	@echo "Impala web    : http://localhost:$(IMPALA_WEBUI_PORT)"
	@echo "Airflow       : http://localhost:$(AIRFLOW_WEB_PORT)  ($(AIRFLOW_ADMIN_USER)/$(AIRFLOW_ADMIN_PASSWORD))"
	@echo "Kafka UI      : http://localhost:$(KAFKA_UI_PORT)"
	@echo "Kafka (host)  : localhost:$(KAFKA_HOST_PORT)  (изнутри сети: kafka:9092)"
	@echo "NiFi          : https://localhost:$(NIFI_PORT)/nifi  ($(NIFI_USER)/$(NIFI_PASSWORD), self-signed)"
	@echo "Superset      : http://localhost:$(SUPERSET_PORT)  ($(SUPERSET_ADMIN_USER)/$(SUPERSET_ADMIN_PASSWORD))"
