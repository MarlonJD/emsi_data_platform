from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


QUALITY_RUNS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.data_quality_runs (
  data_quality_run_id TEXT PRIMARY KEY,
  pipeline_run_id TEXT,
  tool TEXT NOT NULL,
  contract_name TEXT NOT NULL,
  dataset_name TEXT NOT NULL,
  window_start TIMESTAMPTZ,
  window_end TIMESTAMPTZ,
  status TEXT NOT NULL CHECK (status IN ('passed', 'warning', 'failed', 'error')),
  critical_count INTEGER NOT NULL DEFAULT 0 CHECK (critical_count >= 0),
  warning_count INTEGER NOT NULL DEFAULT 0 CHECK (warning_count >= 0),
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ NOT NULL,
  raw_result_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS data_quality_runs_contract_finished_idx
  ON ops.data_quality_runs (contract_name, finished_at DESC);

CREATE INDEX IF NOT EXISTS data_quality_runs_status_finished_idx
  ON ops.data_quality_runs (status, finished_at DESC);
"""

QUALITY_FINDINGS_DDL = """
CREATE TABLE IF NOT EXISTS ops.data_quality_findings (
  data_quality_finding_id TEXT PRIMARY KEY,
  data_quality_run_id TEXT NOT NULL REFERENCES ops.data_quality_runs (data_quality_run_id) ON DELETE CASCADE,
  severity TEXT NOT NULL CHECK (severity IN ('critical', 'warning', 'info')),
  dimension TEXT NOT NULL,
  dataset_name TEXT NOT NULL,
  check_name TEXT NOT NULL,
  message TEXT NOT NULL,
  sample_count BIGINT,
  owner TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('open', 'acknowledged', 'resolved', 'wont_fix')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS data_quality_findings_run_idx
  ON ops.data_quality_findings (data_quality_run_id);

CREATE INDEX IF NOT EXISTS data_quality_findings_open_idx
  ON ops.data_quality_findings (status, severity, created_at DESC)
  WHERE status IN ('open', 'acknowledged');
"""

UPSERT_QUALITY_RUN_SQL = """
INSERT INTO ops.data_quality_runs (
  data_quality_run_id,
  pipeline_run_id,
  tool,
  contract_name,
  dataset_name,
  window_start,
  window_end,
  status,
  critical_count,
  warning_count,
  started_at,
  finished_at,
  raw_result_json
) VALUES (
  %(data_quality_run_id)s,
  %(pipeline_run_id)s,
  %(tool)s,
  %(contract_name)s,
  %(dataset_name)s,
  %(window_start)s,
  %(window_end)s,
  %(status)s,
  %(critical_count)s,
  %(warning_count)s,
  %(started_at)s,
  %(finished_at)s,
  %(raw_result_json)s::jsonb
)
ON CONFLICT (data_quality_run_id) DO UPDATE SET
  status = EXCLUDED.status,
  critical_count = EXCLUDED.critical_count,
  warning_count = EXCLUDED.warning_count,
  finished_at = EXCLUDED.finished_at,
  raw_result_json = EXCLUDED.raw_result_json;
"""

UPSERT_QUALITY_FINDING_SQL = """
INSERT INTO ops.data_quality_findings (
  data_quality_finding_id,
  data_quality_run_id,
  severity,
  dimension,
  dataset_name,
  check_name,
  message,
  sample_count,
  owner,
  status,
  created_at,
  resolved_at
) VALUES (
  %(data_quality_finding_id)s,
  %(data_quality_run_id)s,
  %(severity)s,
  %(dimension)s,
  %(dataset_name)s,
  %(check_name)s,
  %(message)s,
  %(sample_count)s,
  %(owner)s,
  %(status)s,
  %(created_at)s,
  %(resolved_at)s
)
ON CONFLICT (data_quality_finding_id) DO UPDATE SET
  message = EXCLUDED.message,
  sample_count = EXCLUDED.sample_count,
  status = EXCLUDED.status,
  resolved_at = EXCLUDED.resolved_at;
"""

BLOCKED_RESULT_TERM_RE = re.compile(
    r"(email|phone|authorization|token|api[_-]?key|cookie|signed[_-]?url|"
    r"body|message|note|raw[_-]?text|raw[_-]?content|transcript|screenshot|"
    r"contact[_-]?value|reveal[_-]?value|exact[_-]?gps|latitude|longitude)",
    re.IGNORECASE,
)
SENSITIVE_KEY_VALUE_RE = re.compile(
    r"\b(email|phone|authorization|token|api[_-]?key|cookie|signed[_-]?url|"
    r"body|message|note|raw[_-]?text|raw[_-]?content|transcript|screenshot|"
    r"contact[_-]?value|reveal[_-]?value|exact[_-]?gps|latitude|longitude)"
    r"\b\s*[:=]\s*[^,\s;]+",
    re.IGNORECASE,
)
EMAIL_VALUE_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
PHONE_VALUE_RE = re.compile(r"\+?\d[\d\s().-]{7,}\d")
BEARER_VALUE_RE = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE)
JWT_VALUE_RE = re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b")
GPS_VALUE_RE = re.compile(r"\b(lat|latitude|lon|longitude|lng)\s*[:=]\s*-?\d+(?:\.\d+)?", re.IGNORECASE)
MAX_OUTPUT_EXCERPT_CHARS = 2000


@dataclass(frozen=True)
class QualityFinding:
    data_quality_finding_id: str
    data_quality_run_id: str
    severity: str
    dimension: str
    dataset_name: str
    check_name: str
    message: str
    sample_count: int | None
    owner: str
    status: str
    created_at: datetime
    resolved_at: datetime | None = None

    def as_params(self) -> dict[str, Any]:
        return {
            "data_quality_finding_id": self.data_quality_finding_id,
            "data_quality_run_id": self.data_quality_run_id,
            "severity": self.severity,
            "dimension": self.dimension,
            "dataset_name": self.dataset_name,
            "check_name": self.check_name,
            "message": self.message,
            "sample_count": self.sample_count,
            "owner": self.owner,
            "status": self.status,
            "created_at": self.created_at,
            "resolved_at": self.resolved_at,
        }


@dataclass(frozen=True)
class QualityRunResult:
    data_quality_run_id: str
    pipeline_run_id: str | None
    tool: str
    contract_name: str
    dataset_name: str
    status: str
    critical_count: int
    warning_count: int
    started_at: datetime
    finished_at: datetime
    raw_result_json: dict[str, Any]
    findings: tuple[QualityFinding, ...] = ()
    window_start: datetime | None = None
    window_end: datetime | None = None

    def as_params(self) -> dict[str, Any]:
        return {
            "data_quality_run_id": self.data_quality_run_id,
            "pipeline_run_id": self.pipeline_run_id,
            "tool": self.tool,
            "contract_name": self.contract_name,
            "dataset_name": self.dataset_name,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "status": self.status,
            "critical_count": self.critical_count,
            "warning_count": self.warning_count,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "raw_result_json": json.dumps(self.raw_result_json, sort_keys=True),
        }


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def dataset_name_from_contract(contract_path: Path) -> str:
    for line in contract_path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("dataset:"):
            continue
        dataset_name = line.split(":", 1)[1].strip()
        if dataset_name:
            return dataset_name
    return contract_path.stem


def build_quality_run_result(
    *,
    contract_name: str,
    dataset_name: str,
    pipeline_run_id: str | None,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    expected_check_count: int,
    observed_check_count: int | None,
    output: str,
    error_message: str = "",
) -> QualityRunResult:
    if status not in {"passed", "warning", "failed", "error"}:
        raise ValueError(f"unsupported quality run status: {status}")
    data_quality_run_id = quality_run_id(pipeline_run_id, contract_name)
    redacted_message = redacted_text(error_message)
    raw_result = {
        "contractName": contract_name,
        "datasetName": dataset_name,
        "expectedCheckCount": expected_check_count,
        "observedCheckCount": observed_check_count,
        "outputSha256": hashlib.sha256((output or "").encode("utf-8")).hexdigest(),
        "outputExcerpt": redacted_text(output, limit=MAX_OUTPUT_EXCERPT_CHARS),
        "errorMessage": redacted_message,
    }
    findings = ()
    critical_count = 0
    warning_count = 0
    if status in {"failed", "error"}:
        critical_count = 1
        findings = (
            QualityFinding(
                data_quality_finding_id=f"{data_quality_run_id}:critical:1",
                data_quality_run_id=data_quality_run_id,
                severity="critical",
                dimension="validity",
                dataset_name=dataset_name,
                check_name=contract_name,
                message=redacted_message or f"{contract_name} did not pass quality verification",
                sample_count=observed_check_count,
                owner="Data Platform",
                status="open",
                created_at=finished_at,
            ),
        )
    return QualityRunResult(
        data_quality_run_id=data_quality_run_id,
        pipeline_run_id=pipeline_run_id,
        tool="soda",
        contract_name=contract_name,
        dataset_name=dataset_name,
        status=status,
        critical_count=critical_count,
        warning_count=warning_count,
        started_at=started_at,
        finished_at=finished_at,
        raw_result_json=raw_result,
        findings=findings,
    )


def quality_run_id(pipeline_run_id: str | None, contract_name: str) -> str:
    normalized_contract = re.sub(r"[^a-zA-Z0-9_.-]+", "-", contract_name.strip())
    if pipeline_run_id:
        return f"product-reporting-quality:{pipeline_run_id}:{normalized_contract}"
    return f"product-reporting-quality:{uuid.uuid4()}:{normalized_contract}"


def redacted_text(value: str, *, limit: int = 500) -> str:
    text = value or ""
    text = SENSITIVE_KEY_VALUE_RE.sub("[redacted]", text)
    text = BEARER_VALUE_RE.sub("[redacted]", text)
    text = JWT_VALUE_RE.sub("[redacted]", text)
    text = GPS_VALUE_RE.sub("[redacted]", text)
    text = EMAIL_VALUE_RE.sub("[redacted]", text)
    text = PHONE_VALUE_RE.sub("[redacted]", text)
    text = BLOCKED_RESULT_TERM_RE.sub("[redacted]", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > limit:
        return text[: limit - 3].rstrip() + "..."
    return text


def connect_analytics_postgres() -> Any:
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("ANALYTICS_POSTGRES_HOST", "analytics-postgres"),
        port=int(os.getenv("ANALYTICS_POSTGRES_PORT", "5432")),
        dbname=os.getenv("ANALYTICS_POSTGRES_DB", "analytics"),
        user=os.getenv("ANALYTICS_POSTGRES_USER", "analytics"),
        password=os.getenv("ANALYTICS_POSTGRES_PASSWORD", "analytics_local_password"),
        connect_timeout=int(os.getenv("ANALYTICS_POSTGRES_CONNECT_TIMEOUT_SECONDS", "5")),
    )


def persist_quality_run_result(
    result: QualityRunResult,
    *,
    connect: Callable[[], Any] = connect_analytics_postgres,
) -> None:
    with connect() as connection:
        ensure_quality_tables(connection)
        write_quality_run_result(connection, result)


def ensure_quality_tables(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute(QUALITY_RUNS_DDL)
        cursor.execute(QUALITY_FINDINGS_DDL)


def write_quality_run_result(connection: Any, result: QualityRunResult) -> None:
    with connection.cursor() as cursor:
        cursor.execute(UPSERT_QUALITY_RUN_SQL, result.as_params())
        for finding in result.findings:
            cursor.execute(UPSERT_QUALITY_FINDING_SQL, finding.as_params())
