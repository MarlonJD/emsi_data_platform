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
- `analytics-ingest-worker`: local-dev consumer that lands accepted analytics
  envelopes into analytics PostgreSQL and routes invalid events to bounded DLQ
  metadata.

Baseline topics:

- `emsi.analytics.events.v1`: accepted analytics event envelope.
- `emsi.analytics.events.dlq.v1`: failed or rejected analytics events for
  local replay/debugging.
- `emsi.product.events.v1`: product-domain event stream candidate for future
  ingest separation.

Local host clients can use `localhost:19092`. Compose-network clients should
use `redpanda:9092`.

Run the deterministic ingest smoke:

```sh
./scripts/run_ingest_smoke.sh
```

The smoke creates `.env` from `.env.example` if needed, starts
`analytics-postgres`, `redpanda`, `redpanda-topic-init`, and
`analytics-ingest-worker`, publishes the same valid synthetic event twice plus
malformed events, restarts the worker, then verifies:

- the valid event appears exactly once in `analytics.raw_event_landing`;
- the malformed event appears in `analytics.raw_event_dlq`;
- ingest checkpoint rows exist.

## Transform, Quality, And Dagster Smoke

Run this after the ingest smoke so `analytics.raw_event_landing` is non-empty:

```sh
./scripts/run_phase_d_smoke.sh
```

The smoke starts local analytics and Dagster metadata PostgreSQL, then runs:

- `dbt deps`;
- `dbt debug`;
- `dbt run --select tag:phase_d_smoke`;
- `soda contract verify --data-source /workspace/soda/configuration.yml --contract /workspace/soda/contracts/raw_event_landing.yml`;
- `dagster job execute -f /workspace/dagster_project/definitions.py -j phase_d_local_smoke_job`.

The dbt slice creates the first local staging view over
`analytics.raw_event_landing` plus Raw Vault-compatible event hub and payload
satellite views. Those views carry hashes and bounded event metadata, not raw
`subject` or `payload` JSON. The Soda v4 contract checks non-empty landing data,
unique event ids, timestamp order, allowed privacy classes, and absence of
blocked raw personal identifier keys in landed subject/payload fields. The
Dagster job runs the same local flow as orchestration evidence.

This remains local-dev integration evidence. It does not prove production DQ
readiness, retention, owner approval, or production source-bound target-window
quality.

## ClickHouse Hot Analytics Candidate

Run this only as an optional Phase E candidate smoke:

```sh
./scripts/run_clickhouse_candidate_smoke.sh
```

The script reruns the ingest smoke first, then starts `analytics-postgres` and
the `clickhouse` service under the `hot-analytics` profile. ClickHouse is not
published on a host port by default; the smoke reaches it on the Compose
network. The runner creates the candidate schema idempotently, exports bounded
columns from `analytics.raw_event_landing`, loads the local ClickHouse
`MergeTree` event table, refreshes the hourly `SummingMergeTree` aggregate, and
compares the aggregate result with the same PostgreSQL query.

This is candidate evidence only:

- PostgreSQL remains canonical for landing, dbt, Data Vault, Soda, and Dagster.
- ClickHouse stores hashes and bounded event metadata, not raw `subject` or
  `payload` JSON.
- The benchmark output is local and bounded; it does not prove production need,
  retention readiness, backup/restore safety, or source-bound quality.
- Roll back by stopping the `hot-analytics` profile or leaving
  `./scripts/run_clickhouse_candidate_smoke.sh` unrun; canonical analytics
  PostgreSQL remains unaffected.

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
  `redpanda`, `redpanda-topic-init`, and `analytics-ingest-worker` on the
  data-platform side. Superset, Grafana, Evidence.dev, SeaweedFS, and
  ClickHouse stay opt-in.

This is local-dev integration evidence only. The Go API still writes accepted
analytics events to its legacy `analytics.events` table while it also publishes
best-effort Kafka envelopes when `ANALYTICS_EVENT_PUBLISHER=kafka`. The
local ingest worker lands Kafka envelopes into canonical analytics PostgreSQL
landing tables for local verification. ClickHouse remains candidate-only hot
analytics until measured need and production gates exist.

### Four-Platform Event Intake First Smoke

The first EMSI app-originated event smoke is local-dev only. Native clients do
not write directly to Kafka, Redpanda, ClickHouse, or analytics PostgreSQL; they
use Go API GraphQL mutations or documented REST compatibility routes:

- Feed ML Home feed telemetry: GraphQL `recordFeedEvents`, compatibility
  `POST /v2/feed/events`.
- Auth/session/screen and bounded Admin usage analytics: GraphQL
  `recordAppEvents`, compatibility `POST /v1/analytics/events`.

Expected local path:

```text
native app -> Go API privacy gate -> Redpanda/Kafka API -> analytics-ingest-worker -> analytics.raw_event_landing -> dbt/Soda/Dagster local smoke
```

For headless evidence, run the targeted platform/backend tests from the EMSI
monorepo first, then run:

```sh
./scripts/run_ingest_smoke.sh
./scripts/run_phase_d_smoke.sh
```

Screenshot-like or manual QA evidence, when required, belongs outside the main
repository under:

```text
MarlonJD/emsi_qa/data-platform/four-platform-event-intake-smoke/<YYYYMMDD>/<platform>/
```

This smoke may prove local event construction, Go API acceptance/rejection,
Kafka publish, landing, and local dbt/Soda/Dagster checks. It does not prove
production source windows, retention approval, production Feed ML readiness,
model serving, rollout approval, ClickHouse adoption, or owner approval.

The 2026-06-20 required expansion contract also allows metadata-only
`admin_reveal_audit_recorded` and `admin_note_metadata_recorded` events after Go
API validation. These events may not carry raw contact values, reveal payloads,
raw reasons, note bodies, messages, raw content, request/response bodies,
screenshots, tokens, or exact GPS/location fields. Required contact/support
reveal audit truth remains `app.staff_ops_audit`, not the optional analytics
stream.
Phase 3 app note metadata smoke should exercise successful iOS/macOS Admin
application decisions and then verify only bounded `admin_note_metadata_recorded`
fields land; raw note/reason/applicant-message values must remain absent from
landing and DLQ payloads.

Crash/error/performance diagnostics such as Sentry, Crashlytics, MetricKit,
OTEL, `crash_reported`, or `api_request_failed` are not part of this local event
intake smoke. Treat them as a separate diagnostics lane that must be mapped to
bounded metadata before landing; do not ingest raw crash dumps, screenshots,
request/response bodies, tokens, raw stack payloads with PII, raw content, or
exact GPS into the analytics or Feed ML data path.

Feed ML serving/shadow collection evidence is generated by the Go API, not by
native clients or the ingest worker. Run `cmd/feed-ml-serving-collection` from
`backend/go-api` to produce the joined telemetry/serving-log coverage report.
The report expects Postgres `app.feed_serving_items` to carry rules-only
rollback, assignment, holdout, and shadow-mode buckets and keeps production
ranking disabled. Data Platform local smokes still validate accepted bounded
feed telemetry and forbidden-field rejection; they do not approve shadow
scoring, canary rollout, production ML readiness, or ClickHouse adoption.

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
- A synthetic event lands once in `analytics.raw_event_landing`, a malformed
  event lands in `analytics.raw_event_dlq`, and worker restart does not create
  duplicate accepted rows.
- PostgreSQL containers become healthy.
- Dagster webserver starts against its own metadata database.
- dbt dependencies resolve and `dbt debug` can connect to analytics Postgres.
- dbt Phase D staging and Raw Vault-compatible models run against
  `analytics.raw_event_landing`.
- Soda v4 runner can connect to analytics Postgres and pass the local landing
  contract guardrails.
- Dagster can execute `phase_d_local_smoke_job` for the local ingest/dbt/Soda
  flow.
- Superset metadata backup/restore smoke passes against PostgreSQL metadata DB.
- ClickHouse candidate smoke can load bounded landing rows and match the
  PostgreSQL hourly aggregate when the optional `hot-analytics` profile is run.

Skipped by default:

- Image vulnerability scans.
- License review.
- Registry provenance and digest promotion.
- Production backup/restore drills.
- ClickHouse production topology, vulnerability scan, retention, backup/restore,
  and owner approval.
- Production DQ readiness and source-bound target-window checks.
- Evidence.dev production hosting audit.

Blocked before production acceptance:

- Owner-approved production data source bindings.
- Secrets management and deployment topology.
- Data retention and access-control review.
- Restore evidence for analytics, Dagster metadata, and Superset metadata.
- BI security review and dashboard ownership.
- npm audit and owner acceptance for Evidence.dev hosting.
