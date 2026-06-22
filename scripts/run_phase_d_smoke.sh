#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-.env}"

if [ ! -f "${ENV_FILE}" ]; then
  cp .env.example "${ENV_FILE}"
fi

COMPOSE_ENV_ARGS=(--env-file versions.env --env-file "${ENV_FILE}")

docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local --profile streaming up -d --build \
  analytics-postgres dagster-postgres redpanda redpanda-topic-init analytics-ingest-worker

docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local build \
  dbt-runner soda-runner dagster-webserver dagster-daemon

docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local run --rm dbt-runner \
  dbt deps --project-dir /workspace/dbt
docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local run --rm dbt-runner \
  dbt debug --project-dir /workspace/dbt --profiles-dir /workspace/dbt
docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local run --rm dbt-runner \
  dbt run --project-dir /workspace/dbt --profiles-dir /workspace/dbt --select tag:phase_d_smoke

docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local run --rm soda-runner \
  soda contract verify --data-source /workspace/soda/configuration.yml \
    --contract /workspace/soda/contracts/raw_event_landing.yml

./scripts/launch_dagster_job.sh phase_d_local_smoke_job
