# data-lakehouse-lab

Локальный лейкхаус, который поднимается одной командой и не требует ничего, кроме Docker. Внутри: MinIO (S3), Hive Metastore, Spark с Iceberg, Greenplum (MPP), ClickHouse, Impala, Kafka, NiFi, Superset, Airflow, Jupyter и питон-песочница.

Вся папка самодостаточная: скопируй на другую машину — заведётся без доустановки.

## С чего начать

```bash
cp .env.example .env          # уже скопировано
make up                       # ядро: MinIO, HMS, Spark, Greenplum
make urls                     # куда тыкать в браузере
make up-full                  # все профили разом (нужно ~10 ГБ RAM, см. «Сколько ест памяти»)
make down                     # выключить
make down-v                   # выключить и стереть данные подчистую
```

Просто `make` покажет весь список команд.

Версии всех образов запинены в `.env.example` (блок «Версии образов» внизу файла), включая `spark-iceberg:3.5.5_1.8.1` — от него зависят версии `--packages` в DAG'ах и ноутбуках. Обновление стека — осознанная правка тега, а не сюрприз от `latest`.

## Как оно устроено

```
 Greenplum ──(JDBC)──┐
                     ▼
              Spark + Iceberg ──(пишет Parquet)──► MinIO (s3a://warehouse)
                     │                                   ▲
                     └──(регистрирует таблицы)──► Hive Metastore ◄── Impala (читает те же таблицы)
                                                         ▲
                                              ClickHouse ─┘ (s3()/icebergS3() прямо из MinIO)

 Kafka ──(события)──► Spark Streaming ──► Iceberg
        └──────────► ClickHouse (ENGINE=Kafka → MergeTree)
        └──────────► NiFi ──► MinIO (сырые JSON)

 Airflow ── гоняет ETL, стриминг-микробатчи и медальон-пайплайн (DAG'и ниже)
```

Ключевой узел — Hive Metastore (порт 9083). Это общий каталог: Spark туда таблицу записал — Impala её сразу видит (HMS пишет события, catalogd Impala их подхватывает за секунды). Вокруг него всё и собрано.

Стриминговая часть — отдельная ветка: producer пишет события в Kafka, дальше их разбирают независимые консьюмеры — Spark Structured Streaming (в Iceberg), ClickHouse (`ENGINE = Kafka` + materialized view) и NiFi (сырые JSON в MinIO).

## Что поднимать

`docker compose up` (он же `make up`) поднимает только ядро: `minio`, `hive-metastore`, `spark-iceberg`, `greenplum` плюс служебный `mc-init` (создаёт бакет и выходит). Всё тяжёлое спрятано за профилями и включается по надобности:

| Команда | Что добавится |
|---|---|
| `make up` | ядро |
| `make up-clickhouse` | + ClickHouse |
| `make up-impala` | + Impala |
| `make up-airflow` | + Airflow |
| `make up-python` | + питон-контейнер со всеми клиентами |
| `make up-kafka` | + Kafka и kafka-ui |
| `make up-nifi` | + NiFi |
| `make up-superset` | + Superset |
| `make up-full` | всё сразу *(в 8 ГБ Docker уже не влезает — см. раздел про память)* |

Профили дружат между собой, так что набор собирается под себя: `docker compose --profile clickhouse --profile airflow up -d`.

## Куда тыкать

| Сервис | Адрес | Логин |
|---|---|---|
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| MinIO S3 API | http://localhost:9000 | — |
| **Spark Master UI** | http://localhost:8080 | — |
| **Spark Application UI** | http://localhost:4040 | — *(пока крутится job)* |
| **Spark History Server** | http://localhost:18080 | — |
| Jupyter | http://localhost:8888 | без токена |
| Greenplum | `localhost:5432` | `gpadmin` / `gpadmin` |
| ClickHouse (play) | http://localhost:8123/play | `default` / `clickhouse` |
| ClickHouse native | `localhost:9100` | *(9000 отдали MinIO)* |
| Impala web UI | http://localhost:25000 | — |
| Impala HS2 | `localhost:21050` | — |
| Airflow | http://localhost:8081 | `airflow` / `airflow` *(8080 занял Spark)* |
| Kafka UI | http://localhost:8082 | — |
| Kafka broker (с хоста) | `localhost:29092` | *(изнутри сети: `kafka:9092`)* |
| NiFi | https://localhost:8443/nifi | `nifi` / `nifinifinifi` *(self-signed, браузер предупредит)* |
| Superset | http://localhost:8089 | `admin` / `admin` |

То же самое печатает `make urls`.

## Как подключаться

### Из IDE (DBeaver, DataGrip и прочие)

Все движки торчат наружу обычными портами, так что подключение — как к любой базе:

| Движок | Драйвер в DBeaver | Параметры |
|---|---|---|
| Greenplum | **Greenplum** (или PostgreSQL) | `localhost:5432`, база `gpadmin`, `gpadmin`/`gpadmin` |
| ClickHouse | **ClickHouse** | `localhost:8123`, `default`/`clickhouse` |
| Impala | **Apache Impala** | `localhost:21050`, без аутентификации (No Auth / NOSASL) |

Через Impala из IDE видны все Iceberg-таблицы — это самый «обычный SQL» способ
ходить по лейкхаусу. Greenplum-драйвер покажет и план MPP-запроса (`EXPLAIN`).
Драйверы DBeaver скачивает сам при первом подключении.

### Jupyter

Встроен в spark-iceberg и уже работает: http://localhost:8888 (без токена).
PySpark-сессия с Iceberg-каталогом доступна из любой клетки, ноутбуки лежат
в `notebooks/` (примонтированы внутрь). Список — ниже.

### Из Python

Два пути:

**С хоста** — ставь клиенты себе и ходи на localhost-порты:
```python
pip install psycopg2-binary clickhouse-connect impyla "pyiceberg[hive,pandas]" boto3

psycopg2.connect("postgresql://gpadmin:gpadmin@localhost:5432/gpadmin")        # Greenplum
clickhouse_connect.get_client(host="localhost", port=8123, password="clickhouse")
impala.dbapi.connect(host="localhost", port=21050, auth_mechanism="NOSASL")    # Impala
# pyiceberg: uri="thrift://localhost:9083", s3.endpoint="http://localhost:9000"
```

**Из контейнера `python`** (профиль `python`) — клиенты уже вшиты в образ, адреса
внутренние (`greenplum`, `clickhouse`, `impalad`, `hive-metastore`, `minio`):
```bash
make py-run       # scripts/query_all.py: один скрипт обходит все движки
make py           # REPL
```

### Из консоли

```bash
make psql-gp          # psql в Greenplum
make spark-sql        # Spark SQL с Iceberg-каталогом hive
make clickhouse-cli   # clickhouse-client
make impala-shell     # impala-shell (отдельный client-образ)
```

## Про железо

Образы прибиты к `platform: linux/amd64` (меняется через `PLATFORM` в `.env`). Расклад такой:

- **Linux x86_64 или Intel Mac** — всё родное, эмуляции ноль.
- **Apple Silicon** — Impala, Greenplum и остальные x86-образы идут через эмуляцию (Rosetta/qemu): работает, но медленнее. В Docker Desktop включи *Use Rosetta for x86/amd64 emulation*.

Чтобы одна и та же папка одинаково заводилась на разных машинах, `linux/amd64` лучше оставить как есть — в этом и переносимость.

### Сколько ест памяти

Замеры на живом стеке (Rosetta, `docker stats`), округлённо:

| Что запущено | RAM |
|---|---|
| ядро (MinIO, HMS, Spark, Greenplum) | ~2.7 ГБ |
| + ClickHouse | +0.5–1 ГБ |
| + Impala (3 демона) | +1.5 ГБ |
| + Airflow (db + webserver + scheduler) | +1.6 ГБ |
| + Kafka + kafka-ui | +1 ГБ |
| + NiFi | +1.3–1.5 ГБ |
| + Superset | +1 ГБ |
| всё (`full`) | ~9–10 ГБ |

Плюс пики: `spark-submit` на время джобы добавляет 0.5–1 ГБ. На лимите Docker ~8 ГБ `full` целиком уже **не помещается** — поднимай профили под текущую задачу, лишнее гаси `docker compose stop <сервис>`.

> ⚠️ Проверено вживую: при выходе за лимит Docker **OOM-убивает контейнеры** (exit 137; первыми обычно гибнут `hive-metastore`, `clickhouse`, `impala-catalog`). Если сервис внезапно `Exited (137)` — это нехватка RAM, а не баг конфига: подними меньше профилей или дай Docker больше памяти.

## Что попробовать

**Spark + Iceberg (это уже в ядре):**
```bash
make spark-sql
# дальше в шелле:
#   CREATE DATABASE hive.demo;
#   CREATE TABLE hive.demo.t (id bigint) USING iceberg;
#   INSERT INTO hive.demo.t VALUES (1),(2);
#   SELECT * FROM hive.demo.t;
```
То же самое, но интерактивно и с графиками — в Jupyter на http://localhost:8888, ноутбук `00_quickstart.ipynb`.

**Greenplum и как он раскидывает данные по сегментам:**
```bash
make gp-init      # зальёт таблицу sales: 100k строк, DISTRIBUTED BY id
make psql-gp
# SELECT gp_segment_id, count(*) FROM sales GROUP BY 1 ORDER BY 1;
```
Последняя строчка и показывает MPP вживую — видно, как строки разъехались по сегментам.

**Перелить из Greenplum в Iceberg (целиком внутри Spark, интернет не нужен):**
```bash
make gp-init
make spark-demo   # запустит jobs/gp_to_iceberg.py → таблица hive.analytics.sales_by_region_product
```

**Та же таблица, но уже глазами Impala:**
```bash
make impala-shell
# SHOW DATABASES;  SELECT * FROM analytics.sales_by_region_product;
# INSERT INTO analytics.sales_by_region_product VALUES (...);  -- запись тоже работает
```
`INVALIDATE METADATA` обычно не нужна: catalogd подхватывает изменения от Spark через event polling за пару секунд. Если таблица «не видна» сразу после создания — подожди секунду-другую или дёрни `INVALIDATE METADATA` вручную, это безвредно.

**ClickHouse читает MinIO напрямую, без всякого Spark:**
```bash
make clickhouse-cli
# SELECT * FROM s3(minio, filename='analytics/.../*.parquet', format='Parquet') LIMIT 10;
```
Коллекция `minio` (с эндпоинтом и ключами) уже прописана в `conf/clickhouse/config.d/s3.xml`, так что руками ничего вбивать не нужно.

**Kafka: события → Iceberg (профиль `kafka`):**
```bash
make up-kafka
docker compose exec python python scripts/kafka_produce_sales.py 1000   # 1000 событий в топик
docker compose exec spark-iceberg spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  /home/iceberg/jobs/kafka_to_iceberg.py                                # дочитать бэклог в Iceberg
```
Джоба со `trigger(availableNow)` каждый запуск забирает только новые события (checkpoint на S3) — повторный прогон не создаст дублей. Топики и consumer-группы видны в kafka-ui: http://localhost:8082. Интерактивная версия с ClickHouse-консьюмером — ноутбук `07_kafka`.

**NiFi: визуальный поток Kafka → MinIO (профиль `nifi`):**
```bash
make up-nifi                                # стартует несколько минут
python3 scripts/nifi_provision_flow.py     # соберёт и запустит flow через REST API
docker compose exec python python scripts/kafka_produce_sales.py 300
```
Скрипт создаёт process group `kafka_to_minio`: ConsumeKafka (топик `sales_events`) → MergeContent (склейка в пачки по 100) → PutS3Object (`warehouse/nifi-landing/`). Через полминуты файлы видны в MinIO console (бакет `warehouse`, папка `nifi-landing/`). Смотреть и редактировать поток — в UI: https://localhost:8443/nifi (`nifi`/`nifinifinifi`, self-signed сертификат). Тот же flow лежит в `flows/nifi/kafka_to_minio.json` — в UI его можно импортировать руками: перетащи на канвас Process Group → кнопка Browse у поля Name → выбери файл.

**Superset (профиль `superset`)** — BI поверх стека: http://localhost:8089 (`admin`/`admin`). Подключения к ClickHouse и Greenplum создаются автоматически при первом старте — можно сразу писать запросы в SQL Lab и собирать чарты.

**Airflow** живёт на http://localhost:8081. Там лежат DAG'и — кнопка Trigger нальёт Greenplum и прогонит ETL (подробнее про каждый DAG ниже, в разделе про пайплайны).

**Jupyter** встроен в spark-iceberg, http://localhost:8888. Ноутбуки — это и есть
учебный курс по стеку, идти можно по порядку:

| Ноутбук | Про что | Чему учит |
|---|---|---|
| `00_quickstart` | Spark + Iceberg | партиционирование, INSERT, эволюция схемы, MERGE, снапшоты, time travel, метаданные файлов |
| `01_greenplum` | MPP вживую | раскладка строк по сегментам, EXPLAIN с Motion, политики распределения (ключ vs реплика) |
| `02_clickhouse` | ClickHouse как ридер | `postgresql()` к Greenplum, MergeTree, чтение MinIO через `s3()` и `icebergS3()` |
| `03_impala` | Impala по Iceberg | SELECT/INSERT, `DESCRIBE HISTORY`, time travel, DELETE и его побочки |
| `04_pyiceberg` | Iceberg без JVM | каталог из чистого Python: scan с pushdown-фильтрами, append, copy-on-write delete |
| `05_iceberg_maintenance` | уход за таблицами | компакция, `expire_snapshots`, лечение delete-файлов (кейс ClickHouse), сироты |
| `06_minio_s3` | что под капотом | boto3, анатомия `data/`+`metadata/`, чтение metadata.json, presigned URL |
| `07_kafka` | стриминг | producer/consumer, Spark Structured Streaming → Iceberg (availableNow, exactly-once), ClickHouse `ENGINE=Kafka` + MV |
| `10_end_to_end` | весь стек разом | GP → Spark → Iceberg → Impala/ClickHouse со сверкой сумм |

Клиенты ставятся прямо в первой ячейке через `!pip install`, так что один раз понадобится интернет.
Все ячейки прогнаны вживую — должны работать как есть.

**Питон-песочница (профиль `python`)** — отдельный контейнер, где клиенты ко всем движкам уже вшиты в образ:
```bash
make up-python    # соберёт datastack-python и поднимет
make py-run       # scripts/query_all.py: один питон обходит GP, ClickHouse, Impala и Iceberg
make py           # просто REPL
make py-sh        # bash внутри
```
Свои скрипты — в `scripts/`, папка примонтирована в `/work/scripts`.

## Пайплайны в Airflow

В `dags/` шесть DAG'ов — пять про данные, один про сам Airflow. Общее (фабрика spark-submit тасок, seed Greenplum) вынесено в `dags/lib/`:

- **`stack_healthcheck`** — дёргает каждый движок (MinIO, Greenplum, HMS, ClickHouse, Impala) параллельно и падает, если кто-то не отвечает. Прогоняй первым после подъёма стека.
- **`gp_to_iceberg_etl`** — базовый ETL: налить Greenplum → `spark-submit` → Iceberg. Минимальный пример оркестрации Spark из Airflow.
- **`e2e_pipeline_test`** — сквозной тест: GP → Iceberg → чтение из Impala и ClickHouse (тот в двух ролях — сверяет и источник через `postgresql()`, и сам Iceberg через `icebergS3()`). Суммы сходятся — зелёный, разъехались — красный.
- **`kafka_pipeline`** — стриминг микробатчами: налить событий в топик → `spark-submit` стриминг-джобы (`availableNow`) → сверка «сообщений в топике == строк в Iceberg» через Impala. Требует профили `kafka` и `impala`.
- **`sales_medallion`** — медальон-пайплайн целиком (см. следующий раздел).
- **`airflow_patterns`** — учебный, про паттерны самого Airflow на живом стеке: расписание с `catchup`, сенсор (`mode='reschedule'`), XCom, dynamic task mapping (задача на каждый регион из Greenplum), TaskGroup, ветвление `@task.branch`, retries (одна задача нарочно падает и проходит со второй попытки), `trigger_rule` для схождения веток. Каждый паттерн помечен в коде комментарием.

Запуск — кнопкой Trigger в UI либо `docker compose exec airflow-scheduler airflow dags trigger <dag_id>`.

Все spark-таски всех DAG'ов сидят в pool'е **`spark_pool` (1 слот, создаётся при инициализации)**: две параллельные spark-джобы на одной машине — почти гарантированный OOM, поэтому вторая честно ждёт в queued. Это же стандартный прод-приём для дросселирования доступа к общему ресурсу.

## Медальон: landing → core → mart → ClickHouse

DAG `sales_medallion` собирает классическую слоёную архитектуру поверх стека — то, как пайплайны устроены в реальных хранилищах:

```
seed_greenplum → landing_from_gp ─┐
landing_from_kafka ───────────────┴→ core_build → mart_build → mart_quality
                                      → publish_clickhouse → verify_publish
```

| Слой | Таблицы | Что происходит |
|---|---|---|
| **landing** | `hive.landing.sales`, `hive.landing.sales_events` | сырьё 1:1 из Greenplum и Kafka-ингеста + служебные `_loaded_at`/`_source`; никакой чистки |
| **core** | `hive.core.sales` | единая схема двух источников, дедуп по `(_source, id)` (свежий `_loaded_at` выигрывает), флаг `is_valid` — невалидное не выкидывается, а помечается |
| **mart** | `hive.mart.sales_by_region_product_monthly`, `hive.mart.sales_monthly` | месячные агрегаты по `is_valid`-строкам |
| **mart (DQ)** | `hive.mart.sales_quality` | качество **сырья** (до дедупа): null'ы, дубли id, amount вне диапазона — по источникам и месяцам |
| **publish** | ClickHouse `analytics_mart.*` (MergeTree) | копия витрин для BI: дашборды крутятся по CH и не дёргают Spark; CH сам читает parquet витрин из MinIO через `icebergS3()` |

`verify_publish` в конце сверяет цепочку целиком: MergeTree == витрины в Iceberg, витрина == core по greenplum-ветке, и всё это ≤ суммы источника.

Работает и без Kafka: нет `analytics.sales_events` — landing-таблица создаётся пустой, пайплайн едет по greenplum-ветке. А с Kafka витрина качества показывает живые дубли: producer каждый запуск генерит id 1..N, дедуп в core их схлопывает, `dup_id_cnt` — показывает.

Джобы слоёв — отдельные файлы `jobs/sales__*.py` (общий SparkSession-конфиг — в `jobs/lib/session.py`), каждую можно гонять руками без Airflow. DDL publish-слоя продублирован в `ddl/clickhouse_up.sql` / `clickhouse_down.sql` (`make ddl-ch-up` / `ddl-ch-down`), но DAG зелёный и без них — таска создаёт таблицы сама.

## Известные грабли

Это учебный стенд, не прод. Ядровой путь прогнан вживую и работает: Greenplum (4 сегмента) → Spark по JDBC → Iceberg в MinIO через Hive Metastore → чтение обратно. Несколько вещей, которые стоит знать:

- **Jar'ы для S3 берутся через `fetch-jars`** (его дёргает любой `make up*`). Скачиваются: `postgresql` (для чтения Greenplum из Spark по JDBC) и `hadoop-aws` + `aws-java-sdk-bundle` (s3a-коннектор для Spark — в образе его нет, только iceberg-aws-bundle). Один раз нужен интернет; `aws-java-sdk-bundle` весит ~280 МБ.
- **HMS — CDP-флейвор, и это принципиально.** Метастор — `apache/impala:*-impala_quickstart_hms` (Hive 3.1.3000, тот же код, что в клиенте Impala). С vanilla `apache/hive` data-запросы Impala по Iceberg падают `access type is: NONE` ([IMPALA-10792](https://issues.apache.org/jira/browse/IMPALA-10792)): на 4.0.0 — accessType NONE, на 3.1.3 каталог вообще не инициализируется (`Invalid method name: get_dataconnectors`). Перепробовано всё конфигурируемое — лечится только сменой образа. База метастора — встроенный Derby в volume `hms-data` (как в апстримном quickstart), отдельный Postgres не нужен. s3a-классы включаются флагом `HADOOP_OPTIONAL_TOOLS=hadoop-aws`; конфиг — `conf/hms-cdp/`. Один volume смонтирован сразу в `/var/lib/hive` и `/user/hive/warehouse` — это трюк апстрима с наследованием прав, не убирай один из mount'ов.
- **Event polling включён.** В `conf/hms-cdp/hive-site.xml` явно задан `metastore.transactional.event.listeners=DbNotificationListener` — без него HMS не пишет события и Impala не видит новые таблицы Spark без `INVALIDATE METADATA`. С ним catalogd (`-hms_event_polling_interval_s=1`) подхватывает изменения за секунды.
- **Greenplum.** Дефолтная база у образа — `postgres`; `make gp-init` сам заводит базу `gpadmin` и наливает демо-таблицу. Бинарники GP не в общем PATH — все обращения идут через `bash -lc` с `greenplum_path.sh` (учтено в Makefile и healthcheck). Первый старт кластера на 4 сегмента долгий — `start_period` healthcheck это закладывает.
- **Airflow → Spark.** airflow-образ кастомный (`airflow/Dockerfile`: apache/airflow + JRE 17 + клиенты движков) — без Java `spark-submit` не работает. DAG `gp_to_iceberg_etl` тянет джарники через `--packages` (Ivy) → на первом прогоне нужен интернет, и нужно достаточно RAM (иначе OOM посреди job). Без сети тот же ETL гоняется внутри Spark: `make spark-demo`. `stack_healthcheck` от этого не зависит и проходит всегда.
- **Kafka: два адреса брокера.** Изнутри docker-сети — `kafka:9092`, с хоста — `localhost:29092`. Это не дубль, а два advertised-listener'а: клиент получает от брокера адрес «куда ходить дальше», и для контейнеров и хоста он обязан быть разным. Подключился с хоста на 9092 и получил странные ошибки соединения — ты попал в чужой listener.
- **Версия коннектора Spark↔Kafka = версии Spark.** `spark-sql-kafka-0-10_2.12:3.5.5` соответствует Spark 3.5.5 из образа (`SPARK_ICEBERG_TAG` в `.env`). Обновишь тег — поменяй версии пакетов в `dags/lib/spark_task.py`, докстринге `jobs/kafka_to_iceberg.py` и ноутбуке 07.
- **NiFi тяжёлый и не быстрый.** Под Rosetta стартует несколько минут (healthcheck это закладывает), ест ~1.3–1.5 ГБ. UI только HTTPS с self-signed сертификатом — предупреждение браузера нормально. Пароль в `.env` обязан быть ≥12 символов: с коротким NiFi молча сгенерирует случайный, и в UI не пустит.
- **Superset** — метадата в SQLite внутри volume `superset-data` (для учебного стенда отдельная СУБД лишняя). Драйверы ClickHouse/Greenplum добавлены в кастомном образе (`superset/Dockerfile`). Админа и подключения к базам создаёт одноразовый `superset-init` при первом старте.
- **Одна spark-джоба за раз.** Spark-таски Airflow дросселируются pool'ом `spark_pool` (1 слот): параллельные spark-submit на этой машине стабильно кончались OOM-каскадом. Видишь таску в queued при свободном работнике — она ждёт слот пула, это штатно. Пул создаёт `airflow-init`; если его нет (старый volume) — `docker compose up airflow-init` или `airflow pools set spark_pool 1 "..."` руками.
- **Spark-worker выключен.** Master в `spark-iceberg` исполняет джобы сам — для учёбы хватает. В compose лежит закомментированный сервис `spark-worker` — раскомментируй при 16+ ГБ для Docker VM, получишь честный кластер в Spark Master UI. Entrypoint там задан явно не просто так: дефолтный entrypoint образа поднял бы второй master + jupyter.

Когда что-то не поднялось — `make ps` показывает статусы, а `make logs S=hive-metastore` (вместо `hive-metastore` — нужный сервис) показывает, на чём именно споткнулось.

## Что где лежит

```
data-lakehouse-lab/
├── docker-compose.yml      # все сервисы и профили
├── .env / .env.example     # порты, логины, версии образов
├── Makefile                # все ярлыки (make help)
├── conf/                   # spark-defaults, hms-cdp (метастор), impala, clickhouse
├── init/                   # создание бакета в minio, демо-таблица в greenplum
├── superset/               # Dockerfile (apache/superset + драйверы CH/GP)
├── ddl/                    # clickhouse_up/down.sql — publish-слой медальона (make ddl-ch-up)
├── jobs/                   # gp_to_iceberg, kafka_to_iceberg, sales__* (медальон); lib/ — общий SparkSession
├── dags/                   # 6 DAG'ов (см. «Пайплайны в Airflow»); lib/ — spark_submit_task, seed
├── airflow/                # Dockerfile (apache/airflow + JRE + клиенты движков)
├── python/                 # Dockerfile + requirements (образ datastack-python)
├── scripts/                # query_all.py, kafka_produce_sales.py, nifi_provision_flow.py
├── flows/nifi/             # экспорт NiFi-flow Kafka→MinIO (для импорта в UI)
└── notebooks/              # 00-07 + 10_end_to_end — учебный курс, см. раздел про Jupyter
```

Нужен чистый лист — `make down-v`. Снесёт контейнеры вместе с данными.
