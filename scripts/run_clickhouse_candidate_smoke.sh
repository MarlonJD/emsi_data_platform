#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-.env}"

if [ ! -f "${ENV_FILE}" ]; then
  cp .env.example "${ENV_FILE}"
fi

CLICKHOUSE_PROMOTION_REPORT_DIR="${CLICKHOUSE_PROMOTION_REPORT_DIR:-artifacts/clickhouse-promotion-gate}"
mkdir -p "${CLICKHOUSE_PROMOTION_REPORT_DIR}"

export CLICKHOUSE_PROMOTION_REPORT_JSON="${CLICKHOUSE_PROMOTION_REPORT_JSON:-/workspace/${CLICKHOUSE_PROMOTION_REPORT_DIR}/report.json}"
export CLICKHOUSE_PROMOTION_REPORT_MD="${CLICKHOUSE_PROMOTION_REPORT_MD:-/workspace/${CLICKHOUSE_PROMOTION_REPORT_DIR}/report.md}"

./scripts/run_ingest_smoke.sh

COMPOSE_ENV_ARGS=(--env-file versions.env --env-file "${ENV_FILE}")

docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local --profile hot-analytics up -d --build \
  analytics-postgres

docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local --profile hot-analytics up -d --build --force-recreate \
  clickhouse

docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local --profile hot-analytics --profile hot-analytics-smoke build \
  clickhouse-candidate-smoke

docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local --profile hot-analytics --profile hot-analytics-smoke run --rm \
  clickhouse-candidate-smoke

echo "ClickHouse promotion gate report: ${CLICKHOUSE_PROMOTION_REPORT_DIR}/report.md"
echo "ClickHouse promotion gate JSON: ${CLICKHOUSE_PROMOTION_REPORT_DIR}/report.json"
