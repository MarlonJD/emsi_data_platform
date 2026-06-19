#!/usr/bin/env bash
set -euo pipefail

COMPOSE_BIN=${COMPOSE_BIN:-docker compose}
COMPOSE_FILES=(-f docker-compose.yml -f docker-compose.superset-postgres-metadata.yml)
BACKUP_DIR=${SUPERSET_BACKUP_DIR:-.local/superset-metadata-smoke}

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

COMPOSE_ENV_ARGS=(--env-file versions.env)
if [ -f .env ]; then
  COMPOSE_ENV_ARGS+=(--env-file .env)
fi

SUPERSET_POSTGRES_DB=${SUPERSET_POSTGRES_DB:-superset}
SUPERSET_POSTGRES_USER=${SUPERSET_POSTGRES_USER:-superset}
SUPERSET_POSTGRES_PASSWORD=${SUPERSET_POSTGRES_PASSWORD:-superset_local_password}

timestamp=$(date -u +"%Y%m%dT%H%M%SZ")
backup_file="${BACKUP_DIR}/superset-metadata-${timestamp}.dump"
restore_db="superset_restore_smoke_${timestamp}"

mkdir -p "${BACKUP_DIR}"

echo "Starting Superset PostgreSQL metadata services."
${COMPOSE_BIN} "${COMPOSE_ENV_ARGS[@]}" "${COMPOSE_FILES[@]}" --profile superset up -d superset-postgres
${COMPOSE_BIN} "${COMPOSE_ENV_ARGS[@]}" "${COMPOSE_FILES[@]}" --profile superset run --rm superset-init

echo "Creating metadata backup at ${backup_file}."
${COMPOSE_BIN} "${COMPOSE_ENV_ARGS[@]}" "${COMPOSE_FILES[@]}" exec -T superset-postgres \
  pg_dump -U "${SUPERSET_POSTGRES_USER}" -d "${SUPERSET_POSTGRES_DB}" --format=custom > "${backup_file}"

echo "Restoring metadata backup into ${restore_db}."
${COMPOSE_BIN} "${COMPOSE_ENV_ARGS[@]}" "${COMPOSE_FILES[@]}" exec -T superset-postgres \
  createdb -U "${SUPERSET_POSTGRES_USER}" "${restore_db}"
${COMPOSE_BIN} "${COMPOSE_ENV_ARGS[@]}" "${COMPOSE_FILES[@]}" exec -T superset-postgres \
  pg_restore -U "${SUPERSET_POSTGRES_USER}" -d "${restore_db}" < "${backup_file}"

echo "Dropping temporary restore database ${restore_db}."
${COMPOSE_BIN} "${COMPOSE_ENV_ARGS[@]}" "${COMPOSE_FILES[@]}" exec -T superset-postgres \
  dropdb -U "${SUPERSET_POSTGRES_USER}" "${restore_db}"

echo "Superset metadata backup/restore smoke passed for local-dev only."
