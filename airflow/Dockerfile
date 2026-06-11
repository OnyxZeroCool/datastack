FROM apache/airflow:2.10.5

# spark-submit требует JRE (базовый airflow-образ питоновый, Java в нём нет).
# Java 17 — как в образе spark-iceberg. procps нужен spark-скриптам (ps).
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Клиенты движков + spark-provider/pyspark — в образ (а не _PIP на каждом старте)
USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark==4.*" "pyspark==3.5.*" \
    psycopg2-binary clickhouse-connect impyla boto3 confluent-kafka
