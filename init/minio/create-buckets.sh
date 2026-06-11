#!/bin/sh
# Создаёт бакет warehouse в MinIO. Запускается одноразовым сервисом mc-init.
set -e

mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

mc mb -p "local/${WAREHOUSE_BUCKET}" || true
mc anonymous set none "local/${WAREHOUSE_BUCKET}" || true

echo "MinIO готов: бакет '${WAREHOUSE_BUCKET}' создан."
mc ls local
