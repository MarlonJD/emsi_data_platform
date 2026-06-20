from __future__ import annotations

import base64
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import psycopg2

from ingest_worker.common import int_env, postgres_config


EVENT_TABLE = "analytics_events_local_candidate"
AGGREGATE_TABLE = "analytics_event_hourly_counts_local_candidate"
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
EVIDENCE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$")
PROMOTION_GATE_SCHEMA_VERSION = "emsi-clickhouse-hot-analytics-promotion-v1"
CANONICAL_WAREHOUSE = "postgresql"
CLICKHOUSE_MODE = "local_candidate_noncanonical"
REQUIRED_PROMOTION_EVIDENCE = (
    ("measuredNeedEvidenceId", "measured need"),
    ("parityEvidenceId", "bounded Postgres/ClickHouse parity"),
    ("rebuildFromCanonicalEvidenceId", "rebuild from canonical PostgreSQL"),
    ("retentionPolicyEvidenceId", "retention policy"),
    ("backupRestoreEvidenceId", "backup/restore or replay proof"),
    ("monitoringEvidenceId", "monitoring and alerting"),
    ("vulnerabilityScanEvidenceId", "vulnerability scan"),
    ("provenanceEvidenceId", "image provenance and license"),
)
REQUIRED_OWNER_APPROVALS = ("analytics", "sre", "privacySecurity")
CLICKHOUSE_CANDIDATE_COLUMNS = (
    "event_id",
    "event_name",
    "event_version",
    "occurred_at",
    "received_at",
    "producer",
    "privacy_class",
    "subject_user_hash",
    "payload_sha256",
    "raw_record_sha256",
    "raw_record_bytes",
    "source_topic",
    "source_partition",
    "source_offset",
    "landed_at",
)
FORBIDDEN_CLICKHOUSE_COLUMNS = (
    "subject",
    "payload",
    "email",
    "phone",
    "token",
    "request_body",
    "response_body",
    "screenshot",
    "exact_gps",
    "raw_content",
    "note_body",
    "reveal_payload",
)


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    timeout_seconds: float

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}/"


def main() -> int:
    clickhouse = clickhouse_config()
    database = clickhouse_identifier(clickhouse.database)
    limit = int_env("CLICKHOUSE_EXPORT_LIMIT", 500)
    if limit <= 0:
        print("clickhouse candidate smoke failed: CLICKHOUSE_EXPORT_LIMIT must be positive", file=sys.stderr)
        return 1

    rows = read_landing_rows(limit)
    if not rows:
        print(
            "clickhouse candidate smoke failed: analytics.raw_event_landing is empty; "
            "run ./scripts/run_ingest_smoke.sh first",
            file=sys.stderr,
        )
        return 1

    try:
        ensure_clickhouse_schema(clickhouse, database)
        load_clickhouse_candidate(clickhouse, database, rows)

        event_ids = [str(row["event_id"]) for row in rows]
        postgres_start = time.perf_counter()
        postgres_aggregate = read_postgres_hourly_aggregate(event_ids)
        postgres_ms = elapsed_ms(postgres_start)

        clickhouse_start = time.perf_counter()
        clickhouse_aggregate = read_clickhouse_hourly_aggregate(clickhouse, database)
        clickhouse_ms = elapsed_ms(clickhouse_start)
    except (urllib.error.URLError, RuntimeError, psycopg2.Error) as exc:
        print(f"clickhouse candidate smoke failed: {exc}", file=sys.stderr)
        return 1

    if postgres_aggregate != clickhouse_aggregate:
        print(
            "clickhouse candidate smoke failed: aggregate mismatch\n"
            f"postgres={json.dumps(postgres_aggregate, sort_keys=True)}\n"
            f"clickhouse={json.dumps(clickhouse_aggregate, sort_keys=True)}",
            file=sys.stderr,
        )
        return 1

    event_count = sum(row[3] for row in clickhouse_aggregate)
    try:
        report = build_promotion_gate_report(
            database=clickhouse.database,
            row_count=len(rows),
            aggregate_event_count=event_count,
            postgres_aggregate_ms=postgres_ms,
            clickhouse_aggregate_ms=clickhouse_ms,
            parity_matched=True,
            promotion_manifest=load_promotion_manifest_from_env(),
        )
        write_promotion_gate_reports(report)
    except RuntimeError as exc:
        print(f"clickhouse candidate smoke failed: {exc}", file=sys.stderr)
        return 1

    print(
        "clickhouse candidate smoke passed: "
        f"rows={len(rows)} aggregate_events={event_count} "
        f"postgres_aggregate_ms={postgres_ms:.2f} clickhouse_aggregate_ms={clickhouse_ms:.2f} "
        f"benchmark_scope=local_candidate_bounded promotion_status={report['productionPromotionStatus']}"
    )
    return 0


def clickhouse_config() -> ClickHouseConfig:
    return ClickHouseConfig(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int_env("CLICKHOUSE_PORT", 8123),
        database=os.getenv("CLICKHOUSE_DB", "emsi_hot_analytics"),
        user=os.getenv("CLICKHOUSE_USER", "emsi_hot_analytics"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse_local_password"),
        timeout_seconds=float(os.getenv("CLICKHOUSE_TIMEOUT_SECONDS", "30")),
    )


def clickhouse_identifier(value: str) -> str:
    if not IDENTIFIER_RE.match(value):
        raise RuntimeError(f"unsafe ClickHouse identifier: {value!r}")
    return f"`{value}`"


def read_landing_rows(limit: int) -> list[dict[str, object]]:
    cfg = postgres_config()
    with psycopg2.connect(cfg.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  event_id,
                  event_name,
                  event_version,
                  occurred_at,
                  received_at,
                  producer,
                  privacy_class,
                  subject_user_hash,
                  payload_sha256,
                  raw_record_sha256,
                  raw_record_bytes,
                  source_topic,
                  source_partition,
                  source_offset,
                  landed_at
                FROM analytics.raw_event_landing
                ORDER BY landed_at DESC, event_id DESC
                LIMIT %s
                """,
                (limit,),
            )
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
    rows.reverse()
    return rows


def read_postgres_hourly_aggregate(event_ids: list[str]) -> list[tuple[str, str, str, int]]:
    cfg = postgres_config()
    with psycopg2.connect(cfg.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  to_char(date_trunc('hour', occurred_at AT TIME ZONE 'UTC'), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS event_hour,
                  event_name,
                  privacy_class,
                  count(*)::integer AS event_count
                FROM analytics.raw_event_landing
                WHERE event_id = ANY(%s)
                GROUP BY 1, 2, 3
                ORDER BY 1, 2, 3
                """,
                (event_ids,),
            )
            return [(row[0], row[1], row[2], row[3]) for row in cur.fetchall()]


def ensure_clickhouse_schema(config: ClickHouseConfig, database: str) -> None:
    query_clickhouse(config, f"CREATE DATABASE IF NOT EXISTS {database}")
    query_clickhouse(
        config,
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{EVENT_TABLE}
        (
          event_id String,
          event_name LowCardinality(String),
          event_version UInt16,
          occurred_at DateTime64(3, 'UTC'),
          received_at DateTime64(3, 'UTC'),
          producer LowCardinality(String),
          privacy_class LowCardinality(String),
          subject_user_hash String,
          payload_sha256 String,
          raw_record_sha256 String,
          raw_record_bytes UInt32,
          source_topic String,
          source_partition Int32,
          source_offset Int64,
          landed_at DateTime64(3, 'UTC'),
          ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(occurred_at)
        ORDER BY (event_name, occurred_at, event_id)
        """,
    )
    query_clickhouse(
        config,
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{AGGREGATE_TABLE}
        (
          event_hour DateTime('UTC'),
          event_name LowCardinality(String),
          privacy_class LowCardinality(String),
          event_count UInt64
        )
        ENGINE = SummingMergeTree
        PARTITION BY toYYYYMM(event_hour)
        ORDER BY (event_hour, event_name, privacy_class)
        """,
    )


def load_clickhouse_candidate(
    config: ClickHouseConfig,
    database: str,
    rows: Iterable[dict[str, object]],
) -> None:
    query_clickhouse(config, f"TRUNCATE TABLE {database}.{EVENT_TABLE}")
    query_clickhouse(config, f"TRUNCATE TABLE {database}.{AGGREGATE_TABLE}")

    payload = "\n".join(json.dumps(clickhouse_event_row(row), sort_keys=True) for row in rows).encode("utf-8")
    query_clickhouse(
        config,
        f"INSERT INTO {database}.{EVENT_TABLE} FORMAT JSONEachRow",
        payload,
    )
    query_clickhouse(
        config,
        f"""
        INSERT INTO {database}.{AGGREGATE_TABLE}
        SELECT
          toStartOfHour(occurred_at) AS event_hour,
          event_name,
          privacy_class,
          count() AS event_count
        FROM {database}.{EVENT_TABLE}
        GROUP BY event_hour, event_name, privacy_class
        """,
    )


def clickhouse_event_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "event_id": row["event_id"],
        "event_name": row["event_name"],
        "event_version": row["event_version"],
        "occurred_at": clickhouse_datetime(row["occurred_at"]),
        "received_at": clickhouse_datetime(row["received_at"]),
        "producer": row["producer"],
        "privacy_class": row["privacy_class"],
        "subject_user_hash": row["subject_user_hash"],
        "payload_sha256": row["payload_sha256"],
        "raw_record_sha256": row["raw_record_sha256"],
        "raw_record_bytes": row["raw_record_bytes"],
        "source_topic": row["source_topic"],
        "source_partition": row["source_partition"],
        "source_offset": row["source_offset"],
        "landed_at": clickhouse_datetime(row["landed_at"]),
    }


def load_promotion_manifest_from_env() -> dict[str, object]:
    manifest_path = os.getenv("CLICKHOUSE_PROMOTION_MANIFEST")
    if not manifest_path:
        return {}

    try:
        with open(manifest_path, encoding="utf-8") as fh:
            manifest = json.load(fh)
    except OSError as exc:
        raise RuntimeError(f"unable to read CLICKHOUSE_PROMOTION_MANIFEST: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid CLICKHOUSE_PROMOTION_MANIFEST JSON: {exc}") from exc

    if not isinstance(manifest, dict):
        raise RuntimeError("CLICKHOUSE_PROMOTION_MANIFEST must contain a JSON object")
    return manifest


def build_promotion_gate_report(
    *,
    database: str,
    row_count: int,
    aggregate_event_count: int,
    postgres_aggregate_ms: float,
    clickhouse_aggregate_ms: float,
    parity_matched: bool,
    promotion_manifest: dict[str, object] | None = None,
) -> dict[str, object]:
    manifest = promotion_manifest or {}
    bounded_columns_only = clickhouse_candidate_columns_are_bounded()
    manifest_declares_noncanonical = manifest.get("clickhouseCanonical") is False
    manifest_declares_production_disabled = manifest.get("clickhouseProductionEnabled") is False
    production_enabled = manifest.get("clickhouseProductionEnabled") is True
    manifest_has_promotion_claim = bool(manifest)

    missing_evidence = [
        {"key": key, "label": label}
        for key, label in REQUIRED_PROMOTION_EVIDENCE
        if not safe_evidence_id(manifest.get(key))
    ]
    owner_approvals = manifest.get("ownerApprovals", {})
    if not isinstance(owner_approvals, dict):
        owner_approvals = {}
    missing_owner_approvals = [
        key
        for key in REQUIRED_OWNER_APPROVALS
        if not safe_evidence_id(owner_approvals.get(key))
    ]

    guardrail_failures: list[str] = []
    if not parity_matched:
        guardrail_failures.append("aggregate_parity_mismatch")
    if not bounded_columns_only:
        guardrail_failures.append("clickhouse_candidate_columns_not_bounded")
    if manifest_has_promotion_claim and "clickhouseCanonical" not in manifest:
        guardrail_failures.append("manifest_missing_clickhouse_noncanonical")
    if manifest_has_promotion_claim and "clickhouseProductionEnabled" not in manifest:
        guardrail_failures.append("manifest_missing_clickhouse_production_disabled")
    if "clickhouseCanonical" in manifest and not manifest_declares_noncanonical:
        guardrail_failures.append("manifest_attempts_clickhouse_canonical")
    if production_enabled:
        guardrail_failures.append("manifest_attempts_enable_production")
    for key, _label in REQUIRED_PROMOTION_EVIDENCE:
        if non_empty_string(manifest.get(key)) and not safe_evidence_id(manifest.get(key)):
            guardrail_failures.append(f"unsafe_manifest_evidence_id:{key}")
    for key in REQUIRED_OWNER_APPROVALS:
        if non_empty_string(owner_approvals.get(key)) and not safe_evidence_id(owner_approvals.get(key)):
            guardrail_failures.append(f"unsafe_manifest_owner_approval:{key}")

    production_promotion_ready = (
        manifest_declares_noncanonical
        and manifest_declares_production_disabled
        and not guardrail_failures
        and not missing_evidence
        and not missing_owner_approvals
    )
    production_status = "ready-for-owner-approved-hot-analytics" if production_promotion_ready else "blocked"

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    report: dict[str, object] = {
        "schemaVersion": PROMOTION_GATE_SCHEMA_VERSION,
        "generatedAt": generated_at,
        "status": production_status,
        "productionPromotionReady": production_promotion_ready,
        "productionPromotionStatus": production_status,
        "canonicalWarehouse": CANONICAL_WAREHOUSE,
        "clickhouseMode": CLICKHOUSE_MODE,
        "clickhouseProductionEnabled": False,
        "clickhouseCanonical": False,
        "candidateDatabase": database,
        "candidateTables": {
            "events": EVENT_TABLE,
            "hourlyAggregate": AGGREGATE_TABLE,
        },
        "sourceOfTruth": "analytics.raw_event_landing",
        "copyBoundary": "truncate-and-reload bounded/hash columns from PostgreSQL",
        "localCandidateEvidence": {
            "parityMatched": parity_matched,
            "rowCount": row_count,
            "aggregateEventCount": aggregate_event_count,
            "postgresAggregateMs": round(postgres_aggregate_ms, 2),
            "clickhouseAggregateMs": round(clickhouse_aggregate_ms, 2),
            "benchmarkScope": "local_candidate_bounded",
            "rebuildFromCanonical": True,
        },
        "privacyGuardrails": {
            "boundedColumnsOnly": bounded_columns_only,
            "rawSubjectCopied": False,
            "rawPayloadCopied": False,
            "rawPiiCopied": False,
            "rawNoteTextCopied": False,
            "revealPayloadValueCopied": False,
            "tokensCopied": False,
            "screenshotsCopied": False,
            "requestBodiesCopied": False,
            "exactGpsCopied": False,
            "candidateColumns": list(CLICKHOUSE_CANDIDATE_COLUMNS),
            "forbiddenColumns": list(FORBIDDEN_CLICKHOUSE_COLUMNS),
        },
        "missingEvidence": missing_evidence,
        "missingOwnerApprovals": missing_owner_approvals,
        "guardrailFailures": guardrail_failures,
        "manifestEvidence": {
            "path": os.getenv("CLICKHOUSE_PROMOTION_MANIFEST", ""),
            "schemaVersion": manifest.get("schemaVersion", ""),
            "evidenceIds": {
                key: sanitized_evidence_id(manifest.get(key))
                for key, _label in REQUIRED_PROMOTION_EVIDENCE
            },
            "ownerApprovals": {
                key: sanitized_evidence_id(value)
                for key, value in owner_approvals.items()
            },
        },
    }
    return report


def clickhouse_candidate_columns_are_bounded() -> bool:
    return not any(column in CLICKHOUSE_CANDIDATE_COLUMNS for column in FORBIDDEN_CLICKHOUSE_COLUMNS)


def non_empty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def safe_evidence_id(value: object) -> bool:
    return isinstance(value, str) and bool(EVIDENCE_ID_RE.match(value.strip()))


def sanitized_evidence_id(value: object) -> str:
    if not non_empty_string(value):
        return ""
    text = value.strip()
    return text if safe_evidence_id(text) else "[unsafe-redacted]"


def write_promotion_gate_reports(report: dict[str, object]) -> None:
    json_path = os.getenv("CLICKHOUSE_PROMOTION_REPORT_JSON")
    markdown_path = os.getenv("CLICKHOUSE_PROMOTION_REPORT_MD")
    if json_path:
        write_text_file(json_path, json.dumps(report, indent=2, sort_keys=True) + "\n")
    if markdown_path:
        write_text_file(markdown_path, render_promotion_gate_markdown(report))


def write_text_file(path: str, body: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(body, encoding="utf-8")


def render_promotion_gate_markdown(report: dict[str, object]) -> str:
    local_evidence = report["localCandidateEvidence"]
    guardrails = report["privacyGuardrails"]
    missing_evidence = report["missingEvidence"]
    missing_approvals = report["missingOwnerApprovals"]
    failures = report["guardrailFailures"]

    lines = [
        "# ClickHouse Hot Analytics Promotion Gate",
        "",
        f"Generated: {report['generatedAt']}",
        f"Status: {report['productionPromotionStatus']}",
        f"Canonical warehouse: {report['canonicalWarehouse']}",
        f"ClickHouse mode: {report['clickhouseMode']}",
        f"ClickHouse production enabled: {str(report['clickhouseProductionEnabled']).lower()}",
        f"ClickHouse canonical: {str(report['clickhouseCanonical']).lower()}",
        "",
        "## Local Candidate Evidence",
        "",
        f"- Parity matched: {str(local_evidence['parityMatched']).lower()}",
        f"- Rows copied: {local_evidence['rowCount']}",
        f"- Aggregate events: {local_evidence['aggregateEventCount']}",
        f"- PostgreSQL aggregate ms: {local_evidence['postgresAggregateMs']}",
        f"- ClickHouse aggregate ms: {local_evidence['clickhouseAggregateMs']}",
        f"- Rebuild from canonical PostgreSQL: {str(local_evidence['rebuildFromCanonical']).lower()}",
        "",
        "## Privacy Guardrails",
        "",
        f"- Bounded columns only: {str(guardrails['boundedColumnsOnly']).lower()}",
        f"- Raw subject copied: {str(guardrails['rawSubjectCopied']).lower()}",
        f"- Raw payload copied: {str(guardrails['rawPayloadCopied']).lower()}",
        f"- Raw PII copied: {str(guardrails['rawPiiCopied']).lower()}",
        "",
        "## Missing Production Evidence",
        "",
    ]
    if missing_evidence:
        lines.extend(f"- {item['label']} ({item['key']})" for item in missing_evidence)
    else:
        lines.append("- none")

    lines.extend(["", "## Missing Owner Approvals", ""])
    if missing_approvals:
        lines.extend(f"- {approval}" for approval in missing_approvals)
    else:
        lines.append("- none")

    lines.extend(["", "## Guardrail Failures", ""])
    if failures:
        lines.extend(f"- {failure}" for failure in failures)
    else:
        lines.append("- none")

    lines.append("")
    return "\n".join(lines)


def read_clickhouse_hourly_aggregate(
    config: ClickHouseConfig,
    database: str,
) -> list[tuple[str, str, str, int]]:
    result = query_clickhouse(
        config,
        f"""
        SELECT
          formatDateTime(event_hour, '%Y-%m-%dT%H:%i:%SZ') AS event_hour,
          event_name,
          privacy_class,
          sum(event_count) AS event_count
        FROM {database}.{AGGREGATE_TABLE}
        GROUP BY event_hour, event_name, privacy_class
        ORDER BY event_hour, event_name, privacy_class
        FORMAT TabSeparated
        """,
    )
    rows: list[tuple[str, str, str, int]] = []
    for line in result.splitlines():
        if not line:
            continue
        event_hour, event_name, privacy_class, event_count = line.split("\t")
        rows.append((event_hour, event_name, privacy_class, int(event_count)))
    return rows


def query_clickhouse(config: ClickHouseConfig, query: str, data: bytes | None = None) -> str:
    body = query.strip().encode("utf-8")
    if data is not None:
        body += b"\n" + data

    last_error: urllib.error.URLError | None = None
    for attempt in range(1, 21):
        request = urllib.request.Request(
            config.base_url,
            data=body,
            method="POST",
        )
        request.add_header("Authorization", basic_auth(config.user, config.password))
        request.add_header("Content-Type", "text/plain; charset=utf-8")

        try:
            with urllib.request.urlopen(request, timeout=config.timeout_seconds) as response:
                return response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ClickHouse HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            last_error = exc
            if attempt == 20:
                break
            time.sleep(1)

    raise RuntimeError(f"ClickHouse HTTP connection failed after retries: {last_error}") from last_error


def basic_auth(user: str, password: str) -> str:
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def clickhouse_datetime(value: object) -> str:
    if not isinstance(value, datetime):
        raise RuntimeError(f"expected datetime from Postgres, got {type(value).__name__}")
    dt = value
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:23]


def elapsed_ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000


if __name__ == "__main__":
    raise SystemExit(main())
