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

## EMSI Go API Integration

From the EMSI monorepo parent, the backend opt-in target starts this
data-platform core plus the Go API:

```sh
cd backend/go-api
make docker-data-platform
```

The backend target uses this Compose project network
(`emsi-data-platform_default`) so the API container can publish to
`redpanda:9092`. It starts only `analytics-postgres`, `dagster-postgres`,
`redpanda`, and `redpanda-topic-init` on the data-platform side. Superset,
Grafana, Evidence.dev, SeaweedFS, and ClickHouse stay opt-in.

This is local-dev integration evidence only. The Go API still writes accepted
analytics events to its legacy `analytics.events` table while it also publishes
best-effort Kafka envelopes when `ANALYTICS_EVENT_PUBLISHER=kafka`. The
canonical analytics warehouse remains PostgreSQL; ClickHouse remains
candidate-only hot analytics until measured need and production gates exist.

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
