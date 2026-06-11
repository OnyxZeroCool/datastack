FROM python:3.12-slim

# gcc нужен для сборки части C-расширений (impyla/thrift, pyarrow при необходимости)
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc g++ \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /work
# Контейнер живёт как точка входа: `docker compose exec python python scripts/...`
CMD ["sleep", "infinity"]
