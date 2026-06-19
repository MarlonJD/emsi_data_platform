#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-.env}"

if [ ! -f "${ENV_FILE}" ]; then
  cp .env.example "${ENV_FILE}"
fi

./scripts/run_ingest_smoke.sh

COMPOSE_ENV_ARGS=(--env-file versions.env --env-file "${ENV_FILE}")

docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local --profile hot-analytics up -d --build \
  analytics-postgres clickhouse

docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local --profile hot-analytics --profile hot-analytics-smoke build \
  clickhouse-candidate-smoke

docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local --profile hot-analytics --profile hot-analytics-smoke run --rm \
  clickhouse-candidate-smoke
