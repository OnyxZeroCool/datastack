#!/bin/bash
# Идемпотентный bootstrap Greenplum: завести базу gpadmin (дефолтная у образа — postgres),
# затем налить демо-таблицу sales. Запускается через `make gp-init`.
set -e
source /usr/local/gpdb/greenplum_path.sh

psql -U gpadmin -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='gpadmin'" | grep -q 1 \
  || psql -U gpadmin -d postgres -c "CREATE DATABASE gpadmin"

psql -U gpadmin -d gpadmin -f /init/init.sql
