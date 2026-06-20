# ClickHouse Hot Analytics Promotion Gate Contract

Status: local-dev candidate, fail-closed.

ClickHouse is an optional hot-analytics sink fed from canonical PostgreSQL
landing rows. It is not the canonical warehouse and is not a consent,
preference, or production-approval source.

## Local Candidate Proof

`scripts/run_clickhouse_candidate_smoke.sh` must:

- rerun the ingest smoke before loading ClickHouse;
- copy only bounded/hash columns from `analytics.raw_event_landing`;
- rebuild the ClickHouse candidate tables from PostgreSQL with a truncate and
  reload;
- compare the same hourly aggregate in PostgreSQL and ClickHouse;
- write JSON and Markdown gate reports under
  `artifacts/clickhouse-promotion-gate/`.

The candidate report always records:

- `canonicalWarehouse=postgresql`;
- `clickhouseMode=local_candidate_noncanonical`;
- `clickhouseCanonical=false`;
- `clickhouseProductionEnabled=false`;
- copied candidate columns and forbidden raw columns.

## Required Production Manifest Evidence

A production hot-analytics promotion manifest must be a JSON object with these
fields before the report can return `productionPromotionReady=true`:

- `schemaVersion`;
- `measuredNeedEvidenceId`;
- `parityEvidenceId`;
- `rebuildFromCanonicalEvidenceId`;
- `retentionPolicyEvidenceId`;
- `backupRestoreEvidenceId`;
- `monitoringEvidenceId`;
- `vulnerabilityScanEvidenceId`;
- `provenanceEvidenceId`;
- `clickhouseCanonical=false`;
- `clickhouseProductionEnabled=false`;
- `ownerApprovals.analytics`;
- `ownerApprovals.sre`;
- `ownerApprovals.privacySecurity`.

Evidence and owner-approval values must be bounded ASCII evidence ids matching
`[A-Za-z0-9][A-Za-z0-9._:-]{2,127}`. Do not place raw approval notes, names,
email addresses, support details, tokens, or other sensitive text in the
manifest. Unsafe values keep the gate blocked and are redacted in the generated
report.

The gate rejects manifests that attempt to mark ClickHouse canonical or enable
production from the smoke runner. Enabling production hot analytics remains a
separate owner-approved infrastructure change after this report is reviewed.

## Forbidden Data

ClickHouse must not store raw `subject`, raw payload JSON, PII, reveal payload
values, raw note text, raw content, tokens, screenshots, request/response
bodies, exact GPS, or unapproved sensitive fields.
