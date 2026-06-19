# EMSI Data Platform

Private EMSI analytics and data-platform workspace.

This repository holds data-platform code and governance assets:

- Dagster orchestration assets and jobs.
- dbt or SQL transformation models.
- Data quality checks and production-readiness report generators.
- Manifest schemas, run metadata, and evidence-id conventions.
- Local-development tooling for the analytics platform.

It starts from the AviaCore360-proven baseline decisions, but it does not claim
production readiness. Local Docker smoke checks are local-dev evidence only.
Production acceptance still requires image provenance, security/license review,
owner approval, source-bound data-quality evidence, backup/restore evidence, and
deployment review.

## Baseline

- PostgreSQL: `postgres:18.4-alpine3.24`, mounted at `/var/lib/postgresql`.
- Python: `python:3.12.13-slim-bookworm`, `>=3.12,<3.13`.
- Dagster: `1.13.9` with `dagster-postgres==0.29.9` and `dagster-dbt==0.29.9`.
- dbt: `dbt-core==1.11.11`, `dbt-postgres==1.10.1`.
- Data Vault: `ScalefreeCOM/datavault4dbt==1.18.3`.
- Event streaming: `redpandadata/redpanda:v26.1.10` as a local-dev candidate
  broker, with Kafka API as the protocol contract.
- Soda v4: `soda-core==4.14.0`, `soda-postgres==4.14.0`.
- Superset: `apache/superset:6.1.0` plus a narrow local custom image that adds
  `psycopg2-binary==2.9.10` for PostgreSQL metadata storage.
- Grafana: `grafana/grafana:13.0.2`.
- Evidence.dev: `node:22.17.0-bookworm-slim` and pinned Evidence packages.
- Object storage local/CI reference: SeaweedFS `4.34`.

See [versions.env](versions.env) and
[docs/decisions/0001-data-platform-baseline.md](docs/decisions/0001-data-platform-baseline.md)
for the classification of each capability.

## Local Dev

Copy the local environment template, then run the core local profile:

```sh
cp .env.example .env
docker compose --env-file versions.env --env-file .env --profile local up --build
```

Run one-off dbt and Soda containers after the databases are healthy:

```sh
docker compose --env-file versions.env --env-file .env --profile local run --rm dbt-runner dbt deps --project-dir /workspace/dbt
docker compose --env-file versions.env --env-file .env --profile local run --rm soda-runner soda scan -d analytics_postgres -c /workspace/soda/configuration.yml /workspace/soda/checks
```

Optional BI and observability profiles:

```sh
docker compose --env-file versions.env --env-file .env --profile streaming up redpanda redpanda-topic-init
docker compose --env-file versions.env --env-file .env --profile observability up grafana
docker compose --env-file versions.env --env-file .env --profile evidence up --build evidence
docker compose --env-file versions.env --env-file .env --profile object-storage up seaweedfs
docker compose --env-file versions.env --env-file .env -f docker-compose.yml -f docker-compose.superset-postgres-metadata.yml --profile superset up --build
```

Redpanda is the local-dev/candidate event backbone. Producers and consumers
should target the Kafka API contract, not Redpanda-specific APIs, so Apache
Kafka remains a rollback/certification lane. Runtime services should publish
events to the stream boundary; data-platform consumers land accepted events into
analytics Postgres. Application services must not write directly into the
warehouse.

Superset metadata backup/restore local smoke:

```sh
./scripts/superset_metadata_backup_restore_smoke.sh
```

## Guardrails

Do not commit raw production data, personal data, large exports, model artifacts,
or generated dashboard/evidence dumps. Keep source-of-truth data in Postgres,
object storage, model registries, or other approved runtime systems; keep only
code, manifests, schemas, and non-sensitive metadata in git.
