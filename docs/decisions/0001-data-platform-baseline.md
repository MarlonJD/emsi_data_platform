# ADR 0001: Data Platform Baseline

Date: 2026-06-19
Status: Accepted for local-dev scaffold; production acceptance blocked pending
evidence

## Context

EMSI needs a PostgreSQL-first data platform scaffold for analytics,
orchestration, data quality, Data Vault modeling, BI, and production-readiness
evidence generation. The starting point reuses AviaCore360 decisions that were
either proven locally or had their boundary clarified.

This ADR does not promote the scaffold to production-ready. It records baseline,
local-dev, candidate, rollback/history, and blocked states so the project starts
from explicit evidence boundaries.

## Decisions

### PostgreSQL

Status: baseline

- Forward baseline: PostgreSQL `18.4`.
- Local image: `postgres:18.4-alpine3.24`.
- Docker volumes mount at `/var/lib/postgresql`.
- Containers set `PGDATA=/var/lib/postgresql/18/docker`.
- Do not mount the old PostgreSQL 17-style `/var/lib/postgresql/data` path.
- PostgreSQL 17 remains rollback/history evidence only and should not be built
  out unless an actual rollback drill needs it.

Production gap: this repo still needs its own digest pinning/provenance capture,
restore drills, backup policy, and owner approval.

### Python And Dagster

Status: local-dev baseline

- Runtime lane: Python `3.12`.
- Base image: `python:3.12.13-slim-bookworm`.
- Python package metadata: `>=3.12,<3.13`.
- Dagster packages:
  - `dagster==1.13.9`
  - `dagster-webserver==1.13.9`
  - `dagster-postgres==0.29.9`
  - `dagster-dbt==0.29.9`
- Dagster metadata uses a dedicated PostgreSQL database and separate local
  credential set. It does not share product OLTP credentials.

Production gap: Dagster deployment topology, run isolation, secrets management,
backup/restore evidence, and operational ownership remain blocked.

### dbt

Status: baseline

- `dbt-core==1.11.11`.
- `dbt-postgres==1.10.1`.
- dbt Core 2/Fusion is not a baseline here; it stays in a separate watch or
  certification lane.

Production gap: dbt artifacts, lineage, CI promotion, and source-bound
acceptance checks are not yet established.

### Data Vault

Status: baseline for `datavault4dbt`; candidate/watch for AutomateDV

- Method: Data Vault 2.0.
- Selected package: `ScalefreeCOM/datavault4dbt==1.18.3`.
- Comparison/watchlist package: `Datavault-UK/automate_dv==0.11.4`.
- Do not mix AutomateDV into the same model set. Treat it as a separate POC,
  rollback, or watchlist lane.

Production gap: no production Raw Vault, Business Vault, marts, or feature-store
models are accepted by this scaffold alone.

### Event Streaming

Status: local-dev candidate for Redpanda; rollback/certification for Apache
Kafka

- Local-dev broker image: `redpandadata/redpanda:v26.1.10`.
- Protocol contract: Kafka API.
- Redpanda is selected for the local-dev/candidate event backbone because it
  gives the project a compact single-binary broker for developer workflows while
  keeping producer and consumer code on the Kafka protocol boundary.
- Apache Kafka is not the first local baseline here. It remains the
  rollback/certification lane for ecosystem compatibility, managed service
  requirements, and customer/platform constraints.
- Application services publish events to the stream boundary. Data-platform
  consumers land accepted events into analytics PostgreSQL. Application services
  do not write directly into the analytics warehouse.
- Baseline local topics:
  - `emsi.analytics.events.v1`
  - `emsi.analytics.events.dlq.v1`
  - `emsi.product.events.v1`

Production gap: TLS, SASL, ACLs, schema compatibility policy, topic retention,
partition sizing, DLQ/replay policy, monitoring, backup/replay drills,
license/security review, and owner approval are blocked before production
acceptance.

### Soda

Status: local-dev

- Soda v4 packages:
  - `soda-core==4.14.0`
  - `soda-postgres==4.14.0`
- `soda-core-postgres==3.5.6` is a legacy Soda Core v3 package line, not the
  Soda v4 PostgreSQL package.
- Start with self-hosted/local Docker runner only.
- Do not enable Soda Cloud for the baseline.

Production gap: local runtime scans are not production DQ readiness. Production
DQ requires source-bound target windows, owner-approved thresholds, evidence ids,
findings retention, and approval records.

### Superset

Status: local-dev candidate

Superset supports PostgreSQL. The repo-specific issue is narrower:

> Tool PostgreSQL'i destekliyor; secili resmi container image PostgreSQL
> metadata/connection driver'ini icermedigi icin pinli, dar kapsamli custom
> image gerekiyor.

Implementation:

- Base image: `apache/superset:6.1.0`.
- Local custom image:
  `emsi-superset-postgres-metadata:6.1.0-psycopg2-2.9.10-local`.
- Added package: `psycopg2-binary==2.9.10`.
- The custom image is only for connecting Superset metadata storage to
  PostgreSQL instead of SQLite. It is not a general-purpose BI hardening image.

Production gap: vulnerability scan, license/security review, registry
provenance, owner approval, metadata backup/restore smoke, and disaster recovery
runbook evidence are required before production acceptance.

### Grafana

Status: local-dev

- Image: `grafana/grafana:13.0.2`.
- Intended for operational observability and DQ dashboard local profiles.

Production gap: dashboards, data sources, auth, retention, and alert ownership
are not accepted yet.

### Evidence.dev

Status: local-dev candidate

- Node image: `node:22.17.0-bookworm-slim`.
- Evidence package: `@evidence-dev/evidence@40.1.8`.
- Direct package pins:
  - `@evidence-dev/component-utilities@4.0.13`
  - `@evidence-dev/core-components@5.4.2`
  - `@evidence-dev/sdk@4.0.2`
  - `@evidence-dev/tailwind@3.1.4`
  - `typescript@5.4.2`

Production gap: production hosting requires npm audit, security owner
acceptance, deployment provenance, and approved source access.

### Object Storage And DuckDB

Status: local/CI for SeaweedFS; candidate for production-compatible object
storage; helper-only for DuckDB

- Local/CI object-storage reference: SeaweedFS `4.34`.
- Production on-prem reference: customer-approved S3-compatible storage or Ceph
  RGW after capability tests.
- DuckDB is only a local, CI, and offline fixture helper. It is not the durable
  source of truth for raw, vault, mart, feature-store, or ops data.

## Consequences

- The scaffold gives EMSI a consistent local-dev lane for PostgreSQL-first
  analytics work without claiming production readiness.
- Version decisions are pinned in [versions.env](../../versions.env).
- Compose local-dev services are intentionally separable from production
  deployment manifests.
- Capability status must be reported literally:

| Capability | Status | Notes |
| --- | --- | --- |
| PostgreSQL 18.4 | baseline | Digest/provenance and restore evidence still blocked. |
| PG17 | rollback/history | Not implemented unless needed for rollback drills. |
| Python 3.12 | baseline | Runtime lane only. |
| Dagster | local-dev | Dedicated metadata database, not product OLTP credentials. |
| dbt 1.11 | baseline | dbt Core 2/Fusion is candidate/watch only. |
| datavault4dbt | baseline | AutomateDV stays separate. |
| Redpanda | local-dev candidate | Kafka API contract; production acceptance blocked. |
| Apache Kafka | rollback/certification | Compatibility and managed-service lane, not first local baseline. |
| Soda v4 | local-dev | Self-hosted runner only; production DQ readiness blocked. |
| Superset | local-dev candidate | PostgreSQL supported; selected official image needs driver add-on. |
| Grafana | local-dev | Observability/DQ dashboard starting point. |
| Evidence.dev | local-dev candidate | Production hosting blocked pending npm/security acceptance. |
| SeaweedFS | local/CI | Production object storage requires capability tests. |
| DuckDB | local/CI/offline helper | Not durable source of truth. |
