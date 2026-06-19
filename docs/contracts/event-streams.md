# Event Stream Contracts

Status: local-dev candidate

The data platform uses Redpanda as the local-dev/candidate broker and Kafka API
as the protocol contract. Producers and consumers should depend on Kafka client
semantics, not Redpanda-specific APIs. Apache Kafka remains the
rollback/certification lane.

## Baseline Topics

| Topic | Status | Purpose |
| --- | --- | --- |
| `emsi.analytics.events.v1` | local-dev candidate | Accepted analytics event envelopes. |
| `emsi.analytics.events.dlq.v1` | local-dev candidate | Rejected or failed analytics events for local replay/debugging. |
| `emsi.product.events.v1` | candidate | Product-domain events before analytics-specific normalization. |

## Envelope

The first event envelope is intentionally small and source-agnostic:

```json
{
  "event_id": "018ff4e2-8e2a-7bf6-bdc0-8f4d2d26d001",
  "event_name": "feed.item.impression",
  "event_version": 1,
  "occurred_at": "2026-06-19T12:00:00Z",
  "received_at": "2026-06-19T12:00:01Z",
  "producer": "emsi-go-api",
  "privacy_class": "pseudonymous",
  "consent_scope": ["analytics"],
  "subject": {
    "user_hash": "sha256:example",
    "session_id": "local-dev-session"
  },
  "payload": {
    "surface": "home_feed",
    "item_type": "post"
  }
}
```

## Guardrails

- Data-platform consumers land accepted events into analytics PostgreSQL.
- Application services must not write directly into the analytics warehouse.
- Optional analytics writes must fail closed when consent or personalization
  settings are disabled or unreadable.
- Raw personal data does not belong in event keys, topic names, or warehouse
  staging tables.
- Local topic creation is not production readiness evidence.

## EMSI Go API Producer Status

Status: Phase B local-dev producer implemented; Phase C local-dev consumer
smoke passed.

- `ANALYTICS_EVENT_PUBLISHER=disabled|outbox|kafka` gates the backend producer.
- `kafka` mode publishes best-effort envelopes to
  `ANALYTICS_EVENT_TOPIC` using Kafka API semantics.
- `ANALYTICS_KAFKA_BOOTSTRAP_SERVERS=redpanda:9092` is the Compose-network
  local-dev setting; host tools can use `localhost:19092`.
- Authenticated user-scoped optional analytics are suppressed when
  `share_analytics` is disabled or preference reads are unavailable.
- Authenticated REST analytics events bind the event subject to the
  server-derived pseudonymous `user_hash` for the bearer session; request body
  hash values are not trusted for Kafka publishing.
- The Go API still writes accepted analytics events to its legacy
  `analytics.events` table for compatibility while the data-platform ingest
  worker and downstream dbt/Soda/Dagster gates mature.
- Payload keys that look like raw subject identifiers, such as `user_id`,
  `userId`, `actor_user_id`, `actorUserId`, `email`, `phone`, `authorization`,
  or `token`, are rejected before Kafka publish.

## Data-Platform Consumer Status

Status: Phase C local-dev ingest worker implemented; runtime smoke passed.

- `analytics-ingest-worker` consumes `emsi.analytics.events.v1` through Kafka
  API semantics and lands accepted records in
  `analytics.raw_event_landing`.
- Accepted events are deduplicated by `event_id`.
- Invalid events are recorded in `analytics.raw_event_dlq` and published to
  `emsi.analytics.events.dlq.v1` as bounded metadata. Raw payload bodies are
  not stored in the DLQ table or DLQ event.
- `analytics.event_ingest_checkpoints` stores last processed offsets by
  consumer group/topic/partition.
- `analytics.event_ingest_metrics` stores accepted/rejected counts, last
  processed offset, and observed consumer lag.
- `scripts/run_ingest_smoke.sh` publishes one valid synthetic envelope and one
  malformed envelope, then checks that the valid event lands once and the
  malformed event reaches the DLQ.

## Downstream Local Smoke Status

Status: Phase D local-dev dbt/Soda/Dagster smoke implemented.

- dbt stages `analytics.raw_event_landing` in `stg_analytics_events` and builds
  Raw Vault-compatible event hub and payload satellite views with hashes and
  bounded event metadata, not raw `subject` or `payload` JSON.
- Soda checks `analytics.raw_event_landing` for non-empty data, unique
  `event_id`, timestamp order, allowed privacy classes, and blocked raw
  personal identifier keys in subject or payload fields.
- Dagster exposes `phase_d_local_smoke_job` to run the landing guardrail check,
  dbt smoke, and Soda scan as a local orchestration path.
- The checks are local integration evidence only. Production data-quality
  readiness still requires owner-approved source windows, thresholds, retention,
  and privacy/legal review.

## Production Blockers

- TLS, SASL, ACLs, and secret rotation.
- Schema compatibility policy and registry ownership.
- Topic retention, partition sizing, and replay policy.
- DLQ review, replay runbook, and failure ownership.
- Monitoring, alerting, and runbook evidence.
- License/security review and owner approval.
