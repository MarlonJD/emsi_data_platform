#!/usr/bin/env bash
set -euo pipefail

grep -q "POSTGRES_IMAGE=postgres:18.4-alpine3.24" versions.env
grep -q "POSTGRES_VOLUME_TARGET=/var/lib/postgresql" versions.env
grep -q "PYTHON_IMAGE=python:3.12.13-slim-bookworm" versions.env
grep -q "dagster==1.13.9" pyproject.toml
grep -q "dbt-core==1.11.11" pyproject.toml
grep -q "SODA_CORE_VERSION=4.14.0" versions.env
grep -q "SODA_POSTGRES_VERSION=4.14.0" versions.env
grep -q "REDPANDA_IMAGE=redpandadata/redpanda:v26.1.10" versions.env
grep -q "REDPANDA_PROTOCOL_CONTRACT=kafka-api" versions.env
grep -q "REDPANDA_TOPIC_ANALYTICS_EVENTS=emsi.analytics.events.v1" versions.env
grep -q "APACHE_KAFKA_STATUS=rollback-certification" versions.env
grep -q "KAFKA_PYTHON_VERSION=3.0.2" versions.env
grep -q "INGEST_POSTGRES_DRIVER=psycopg2-binary==2.9.12" versions.env
grep -q "kafka-python==3.0.2" pyproject.toml
grep -q "psycopg2-binary==2.9.12" pyproject.toml
grep -q "psycopg2-binary==2.9.10" docker/superset/requirements-postgres-metadata.txt
grep -q "ScalefreeCOM/datavault4dbt" dbt/packages.yml

PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/emsi-data-platform-pycache}" \
  python3 -m py_compile dagster_project/*.py ingest_worker/*.py

if grep -R "soda-core-postgres" docker dbt dagster_project soda pyproject.toml docker-compose.yml docker-compose.superset-postgres-metadata.yml; then
  echo "legacy Soda v3 package found in runnable scaffold" >&2
  exit 1
fi

if grep -n "/var/lib/postgresql/data" docker-compose.yml docker-compose.superset-postgres-metadata.yml; then
  echo "legacy PostgreSQL data mount found in compose files" >&2
  exit 1
fi

if grep -R "payload_preview\\|raw_payload" ingest_worker sql docker-compose.yml; then
  echo "raw DLQ payload storage found in ingest path" >&2
  exit 1
fi

if [ "${SKIP_DOCKER_CONFIG:-0}" = "1" ]; then
  echo "skipped docker compose config parse because SKIP_DOCKER_CONFIG=1" >&2
elif command -v docker >/dev/null 2>&1; then
  docker compose --env-file versions.env --profile local --profile streaming --profile observability --profile evidence --profile object-storage config >/dev/null
  docker compose --env-file versions.env -f docker-compose.yml -f docker-compose.superset-postgres-metadata.yml --profile superset config >/dev/null
else
  echo "docker not found; skipped docker compose config parse" >&2
fi

echo "static verification passed"
