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
- ClickHouse: `clickhouse/clickhouse-server:25.8.24.21` as an optional
  hot-analytics candidate fed from the canonical PostgreSQL landing path.
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
docker compose --env-file versions.env --env-file .env --profile local run --rm soda-runner soda contract verify --data-source /workspace/soda/configuration.yml --contract /workspace/soda/contracts/raw_event_landing.yml
```

Optional BI and observability profiles:

```sh
docker compose --env-file versions.env --env-file .env --profile streaming up redpanda redpanda-topic-init
docker compose --env-file versions.env --env-file .env --profile streaming up -d analytics-ingest-worker
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

Run the local ingest smoke after the streaming profile is available:

```sh
./scripts/run_ingest_smoke.sh
```

The smoke generates a per-run event id, publishes the same valid synthetic
analytics envelope twice, publishes malformed envelopes, restarts the worker,
and checks again. The ingest worker lands the valid event once in
`analytics.raw_event_landing`, routes malformed events to
`analytics.raw_event_dlq` with bounded metadata only, and updates checkpoint and
metric rows. The DLQ path stores hashes, sizes, event ids, and error codes; it
does not store raw payload bodies.

Run the local transform and quality smoke after the ingest smoke has produced
landing rows:

```sh
./scripts/run_phase_d_smoke.sh
```

The Phase D smoke runs dbt dependencies, `dbt debug`, and the first staging plus
Raw Vault-compatible models over `analytics.raw_event_landing`. The dbt views
carry hashes and bounded event metadata, not raw `subject` or `payload` JSON.
The smoke then runs the Soda v4 local contract for non-empty landing data,
unique event ids, valid event timestamps, allowed privacy classes, and blocked
raw personal identifier, contact/reveal payload, note/body/message, raw-content,
request/response body, screenshot, token, and exact GPS/location keys.
The Dagster `phase_d_local_smoke_job` executes the same local ingest/dbt/Soda
flow as orchestration evidence. This is local-dev evidence only, not production
data-quality readiness.

Run the optional ClickHouse hot analytics candidate smoke only after the local
canonical path is available:

```sh
./scripts/run_clickhouse_candidate_smoke.sh
```

The smoke first reruns the ingest smoke so `analytics.raw_event_landing` is the
source of truth. It then starts the `hot-analytics` profile, creates the
ClickHouse candidate schema, copies only bounded/hash event columns into
`analytics_events_local_candidate`, builds an hourly `SummingMergeTree`
aggregate, and compares the same bounded hourly aggregate against PostgreSQL.
The printed timing is a local candidate benchmark only; it does not justify
default startup or production use.

The 2026-06-20 required expansion contract adds metadata-only
`admin_reveal_audit_recorded` and `admin_note_metadata_recorded` event names for
approved local/dev mirrors. Required contact/support reveal audit evidence still
belongs to `app.staff_ops_audit`, and ClickHouse remains non-canonical until a
separate production hot-analytics promotion gate passes.
Phase 3 app producers send `admin_note_metadata_recorded` only after successful
iOS/macOS Admin application decisions and only with bounded note metadata
tokens; note text, applicant-message body, internal-note body, feedback text,
raw search text, and embeddings are outside the Data Platform contract.
Phase 4 Feed ML serving collection is report-only from the Data Platform
perspective: the Go API writes server-owned serving-log buckets in Postgres and
`cmd/feed-ml-serving-collection` joins those rows to accepted feed telemetry.
The local ingest contract does not accept raw scores, raw content, opted-out
training rows, model-ranked traffic, or client-asserted shadow mode. Visible
ranking remains `rules_v1`, shadow mode remains `disabled` unless a separate
approved `score_log_only` evidence path exists, and production ML readiness
remains fail-closed.

## iOS Limited Production Canary Capture

Use this helper only for the accepted-risk iOS limited canary approved by
`EMSI-DP-P1A-IOS-CANARY-20260620-AR01`. It does not emit app events and must
not be used to replace the real seeded-user iOS canary run. Its job is to fail
closed when the approved target, seeded user, privacy evidence, or
warehouse/check access is missing, then capture the bounded warehouse evidence
after the real iOS run.

Preflight before starting the canary:

```sh
cp .env.canary.example .env.canary
# Fill `.env.canary` with the approved target, seeded user, privacy artifact,
# warehouse DSN, and 60-120 second source window.
./scripts/run_ios_limited_canary_capture.sh \
  --preflight-only \
  --evidence-json /private/tmp/emsi-ios-canary-preflight.json
```

Capture after the 1-2 minute iOS run and downstream checks:

```sh
EMSI_DP_CANARY_ALLOW_PRODUCTION_CAPTURE=true \
./scripts/run_ios_limited_canary_capture.sh \
  --evidence-json /private/tmp/emsi-ios-canary-evidence.json
```

The wrapper loads `.env.canary` automatically when present, or another
approved bundle passed with `--env-file <path>` / `EMSI_DP_CANARY_ENV_FILE`.
Only use this for the production or staging-production-equivalent canary access
bundle. The normal local `.env` is intentionally not loaded as canary evidence.
The loader accepts only plain `EMSI_DP_CANARY_*=value` assignments and does not
execute shell code from the env file.

Required preflight inputs:

| Environment variable | Purpose |
| --- | --- |
| `EMSI_DP_CANARY_APPROVAL_ID` | Must equal `EMSI-DP-P1A-IOS-CANARY-20260620-AR01`. |
| `EMSI_DP_CANARY_TARGET_NAME` | Non-PII approved target name; local/dev/test names are rejected. |
| `EMSI_DP_CANARY_TARGET_CLASS` | `production` or `staging-production-equivalent`. |
| `EMSI_DP_CANARY_SEEDED_USER_REF` | Non-PII seeded iOS canary user reference. |
| `EMSI_DP_CANARY_SUBJECT_USER_HASH` | Pseudonymous 64-hex subject hash used for warehouse filtering. |
| `EMSI_DP_CANARY_EVENT_ID_PREFIX` | Per-run event id prefix emitted by the real iOS canary path. |
| `EMSI_DP_CANARY_WINDOW_START` | ISO-8601 canary window start with timezone. |
| `EMSI_DP_CANARY_WINDOW_END` | ISO-8601 canary window end with timezone; duration must be 60-120 seconds. |
| `EMSI_DP_CANARY_WAREHOUSE_DSN` | Read-only production warehouse DSN; local/dev markers and `sslmode=disable` are rejected. |
| `EMSI_DP_CANARY_SHARE_ANALYTICS` | Must be `true` for the seeded user. |
| `EMSI_DP_CANARY_PERSONALIZATION_ENABLED` | Must be `true` for the seeded user. |
| `EMSI_DP_CANARY_PRIVACY_ARTIFACT` | Durable privacy preference evidence id or path. |

Required capture inputs after downstream checks:

| Environment variable | Purpose |
| --- | --- |
| `EMSI_DP_CANARY_DBT_STATUS` and `EMSI_DP_CANARY_DBT_ARTIFACT` | dbt must be `passed` with an artifact id or path. |
| `EMSI_DP_CANARY_SODA_STATUS` and `EMSI_DP_CANARY_SODA_ARTIFACT` | Soda must be `passed` with an artifact id or path. |
| `EMSI_DP_CANARY_DAGSTER_STATUS` and `EMSI_DP_CANARY_DAGSTER_ARTIFACT` | Dagster must be `passed` with an artifact id or run id. |
| `EMSI_DP_CANARY_STOP_ROLLBACK_OUTCOME` | Stop or rollback outcome for the production canary window. |

The evidence JSON intentionally redacts the warehouse DSN and subject hash. It
contains the target class, seeded user reference, canary window, privacy flags,
landing count, DLQ count, accepted event-name counts, forbidden-field result,
downstream check artifacts, success criteria, and stop/rollback outcome.
Local/dev evidence remains disallowed as production canary evidence.

Superset metadata backup/restore local smoke:

```sh
./scripts/superset_metadata_backup_restore_smoke.sh
```

## Guardrails

Do not commit raw production data, personal data, large exports, model artifacts,
or generated dashboard/evidence dumps. Keep source-of-truth data in Postgres,
object storage, model registries, or other approved runtime systems; keep only
code, manifests, schemas, and non-sensitive metadata in git.
