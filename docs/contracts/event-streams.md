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

## Production Blockers

- TLS, SASL, ACLs, and secret rotation.
- Schema compatibility policy and registry ownership.
- Topic retention, partition sizing, and replay policy.
- DLQ review, replay runbook, and failure ownership.
- Monitoring, alerting, and runbook evidence.
- License/security review and owner approval.
