#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-.env}"
SMOKE_RUN_ID="${SMOKE_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
export SMOKE_EVENT_ID="${SMOKE_EVENT_ID:-local-dev-smoke-event-${SMOKE_RUN_ID}}"
export SMOKE_INVALID_EVENT_ID="${SMOKE_INVALID_EVENT_ID:-local-dev-smoke-invalid-${SMOKE_RUN_ID}}"
export SMOKE_CHECK_ATTEMPTS="${SMOKE_CHECK_ATTEMPTS:-90}"

if [ ! -f "${ENV_FILE}" ]; then
  cp .env.example "${ENV_FILE}"
fi

docker compose --env-file versions.env --env-file "${ENV_FILE}" --profile local --profile streaming up -d --build \
  analytics-postgres redpanda redpanda-topic-init analytics-ingest-worker

docker compose --env-file versions.env --env-file "${ENV_FILE}" --profile local --profile streaming --profile smoke build \
  analytics-ingest-smoke-producer analytics-ingest-smoke-check

docker compose --env-file versions.env --env-file "${ENV_FILE}" --profile local --profile streaming --profile smoke run --rm analytics-ingest-smoke-producer
docker compose --env-file versions.env --env-file "${ENV_FILE}" --profile local --profile streaming --profile smoke run --rm analytics-ingest-smoke-producer
docker compose --env-file versions.env --env-file "${ENV_FILE}" --profile local --profile streaming --profile smoke run --rm analytics-ingest-smoke-check
docker compose --env-file versions.env --env-file "${ENV_FILE}" --profile local --profile streaming restart analytics-ingest-worker
docker compose --env-file versions.env --env-file "${ENV_FILE}" --profile local --profile streaming --profile smoke run --rm analytics-ingest-smoke-check
