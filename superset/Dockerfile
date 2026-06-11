FROM apache/superset:4.1.4

# Драйверы к движкам стека: ClickHouse (clickhouse-connect) и
# Greenplum/Postgres (psycopg2). В базовом образе их нет.
USER root
RUN pip install --no-cache-dir clickhouse-connect psycopg2-binary
USER superset
