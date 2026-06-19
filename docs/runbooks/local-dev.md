# Local Dev Runbook

Status: local-dev only

## Start Core Services

```sh
cp .env.example .env
docker compose --env-file versions.env --env-file .env --profile local up --build
```

Core services:

- `analytics-postgres`: PostgreSQL 18.4 analytics warehouse baseline.
- `dagster-postgres`: separate PostgreSQL 18.4 metadata database for Dagster.
- `dagster-webserver` and `dagster-daemon`: local orchestration runtime.
- `dbt-runner`: on-demand dbt container.
- `soda-runner`: on-demand self-hosted Soda v4 container.

## Event Streaming Profile

```sh
docker compose --env-file versions.env --env-file .env --profile streaming up redpanda redpanda-topic-init
```

Streaming services:

- `redpanda`: local-dev/candidate event broker using the Kafka API contract.
- `redpanda-topic-init`: idempotent local topic creation for baseline streams.

Baseline topics:

- `emsi.analytics.events.v1`: accepted analytics event envelope.
- `emsi.analytics.events.dlq.v1`: failed or rejected analytics events for
  local replay/debugging.
- `emsi.product.events.v1`: product-domain event stream candidate for future
  ingest separation.

Local host clients can use `localhost:19092`. Compose-network clients should
use `redpanda:9092`.

## Optional Profiles

```sh
docker compose --env-file versions.env --env-file .env --profile observability up grafana
docker compose --env-file versions.env --env-file .env --profile evidence up --build evidence
docker compose --env-file versions.env --env-file .env --profile object-storage up seaweedfs
docker compose --env-file versions.env --env-file .env -f docker-compose.yml -f docker-compose.superset-postgres-metadata.yml --profile superset up --build
```

## Verification Split

Passed by local runtime only when run successfully:

- Compose config parses for the selected profile.
- Redpanda starts and baseline topics are created when the `streaming` profile
  is run.
- PostgreSQL containers become healthy.
- Dagster webserver starts against its own metadata database.
- dbt dependencies resolve and `dbt debug` can connect to analytics Postgres.
- Soda v4 runner can connect to analytics Postgres.
- Superset metadata backup/restore smoke passes against PostgreSQL metadata DB.

Skipped by default:

- Image vulnerability scans.
- License review.
- Registry provenance and digest promotion.
- Production backup/restore drills.
- Production DQ readiness and source-bound target-window checks.
- Evidence.dev production hosting audit.

Blocked before production acceptance:

- Owner-approved production data source bindings.
- Secrets management and deployment topology.
- Data retention and access-control review.
- Restore evidence for analytics, Dagster metadata, and Superset metadata.
- BI security review and dashboard ownership.
- npm audit and owner acceptance for Evidence.dev hosting.
