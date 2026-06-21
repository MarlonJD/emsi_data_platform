from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


EXIT_BLOCKED = 2
EXIT_FAILED = 3

LANES = {"admin", "reveal", "note", "feedml", "clickhouse"}
QUERY_EVENT_NAMES_BY_LANE = {
    "admin": {"admin_surface_viewed", "admin_action_visibility_viewed"},
    "reveal": {"admin_reveal_audit_recorded"},
    "note": {"admin_note_metadata_recorded"},
}

FORBIDDEN_KEYS = {
    "actor_user_id",
    "applicant_message",
    "authorization",
    "auth_token",
    "bearer",
    "body",
    "comment_body",
    "contact_payload",
    "contact_value",
    "cookie",
    "dm_content",
    "email",
    "emailaddress",
    "exact_gps",
    "feedback_message",
    "free_form",
    "freeform",
    "full_url",
    "gps",
    "internal_note",
    "latitude",
    "longitude",
    "message",
    "note",
    "note_body",
    "phone",
    "phonenumber",
    "post_body",
    "private_note",
    "raw_content",
    "raw_note",
    "raw_note_text",
    "raw_policy_text",
    "raw_search_text",
    "raw_text",
    "raw_user_id",
    "reply_body",
    "request_body",
    "response_body",
    "reveal_payload",
    "reveal_value",
    "screenshot",
    "search_text",
    "signed_url",
    "support_contact",
    "support_payload",
    "target_user_id",
    "token",
    "transcript",
    "user_id",
    "userid",
    "view_hierarchy",
}

PAYLOAD_KEYS = {
    "admin_surface_viewed": {
        "admin_surface",
        "module_key",
        "screen_key",
        "result_state",
        "role_scope_key",
    },
    "admin_action_visibility_viewed": {
        "admin_surface",
        "module_key",
        "screen_key",
        "action_key",
        "action_surface",
        "result_state",
        "disabled_reason_key",
        "role_scope_key",
        "proposal_mode",
        "target_type",
        "target_hash",
    },
    "admin_reveal_audit_recorded": {
        "admin_surface",
        "module_key",
        "screen_key",
        "reveal_field_class",
        "reveal_result",
        "actor_role_scope_key",
        "reason_length_bucket",
        "reason_category",
        "confirmation_state",
        "authorization_outcome",
        "audit_action_key",
        "audit_receipt_hash",
        "target_type",
        "target_hash",
    },
    "admin_note_metadata_recorded": {
        "admin_surface",
        "module_key",
        "screen_key",
        "note_surface",
        "note_type",
        "note_action",
        "note_length_bucket",
        "sensitivity_class",
        "redaction_class",
        "has_attachment",
        "language_bucket",
        "lifecycle_state",
        "lifecycle_bucket",
        "target_type",
        "target_hash",
    },
}

LOCAL_DSN_MARKERS = (
    "localhost",
    "127.0.0.1",
    "::1",
    "host=postgres",
    "host=analytics-postgres",
    "sslmode=disable",
    "analytics_local_password",
    "postgres://emsi:emsi@",
)
PLACEHOLDER_MARKERS = ("example", "fixture", "placeholder", "dummy")
SECRET_PLACEHOLDER_MARKERS = PLACEHOLDER_MARKERS + ("redacted",)
TEMPLATE_SECRET_VALUES = {
    "<secret>",
    "<password>",
    "changeme",
    "password",
    "replace-me",
    "replace_me",
    "secret",
}

CHECK_STATUSES = {"passed", "failed", "blocked", "skipped"}
SAFE_REF_PATTERN = re.compile(r"^[A-Za-z0-9._:/-]+$")
HASH_PATTERN = re.compile(r"^(?:sha256:)?[a-fA-F0-9]{64}$")


@dataclass(frozen=True)
class RequiredAnalyticsConfig:
    target_name: str
    target_class: str
    lanes: tuple[str, ...]
    approval_ids: tuple[str, ...]
    seeded_user_ref: str
    subject_user_hash: str
    event_id_prefix: str
    window_start: datetime
    window_end: datetime
    warehouse_dsn: str
    privacy_artifact: str
    admin_scenario_status: str
    admin_scenario_artifact: str
    dbt_status: str
    dbt_artifact: str
    soda_status: str
    soda_artifact: str
    dagster_status: str
    dagster_artifact: str
    feedml_status: str
    feedml_artifact: str
    clickhouse_status: str
    clickhouse_artifact: str
    stop_rollback_outcome: str

    @property
    def duration_seconds(self) -> int:
        return int((self.window_end - self.window_start).total_seconds())

    @property
    def expected_event_names(self) -> set[str]:
        names: set[str] = set()
        for lane in self.lanes:
            names.update(QUERY_EVENT_NAMES_BY_LANE.get(lane, set()))
        return names

    @property
    def has_admin_runtime_lane(self) -> bool:
        return any(lane in self.lanes for lane in ("admin", "reveal", "note"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fail-closed capture helper for Required Analytics external "
            "production/staging-equivalent evidence. This helper does not emit "
            "application events."
        )
    )
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="Validate target/access inputs without connecting to the warehouse.",
    )
    parser.add_argument(
        "--evidence-json",
        type=Path,
        help="Write the bounded evidence summary JSON to this path.",
    )
    parser.add_argument(
        "--evidence-md",
        type=Path,
        help="Write a bounded Markdown evidence summary to this path.",
    )
    args = parser.parse_args()

    exit_code, evidence = collect_evidence(preflight_only=args.preflight_only)
    payload = json.dumps(evidence, indent=2, sort_keys=True)
    if args.evidence_json:
        args.evidence_json.parent.mkdir(parents=True, exist_ok=True)
        args.evidence_json.write_text(payload + "\n", encoding="utf-8")
        print(f"wrote required analytics evidence summary: {args.evidence_json}")
    else:
        print(payload)
    if args.evidence_md:
        args.evidence_md.parent.mkdir(parents=True, exist_ok=True)
        args.evidence_md.write_text(markdown_summary(evidence), encoding="utf-8")
        print(f"wrote required analytics evidence markdown: {args.evidence_md}")
    return exit_code


def collect_evidence(preflight_only: bool) -> tuple[int, dict[str, Any]]:
    cfg, errors, warnings = load_config(preflight_only)
    evidence = base_evidence(cfg, preflight_only, errors, warnings)

    if errors:
        evidence["classification"] = "blocked-access-gate"
        evidence["preflight"]["passed"] = False
        return EXIT_BLOCKED, evidence

    evidence["preflight"]["passed"] = True
    if preflight_only:
        evidence["classification"] = "preflight-ready"
        return 0, evidence

    try:
        landing_rows, dlq_count = read_required_rows(cfg)
    except Exception as exc:  # pragma: no cover - exercised by runtime only.
        evidence["classification"] = "blocked-warehouse-check"
        evidence["preflight"]["passed"] = False
        evidence["preflight"]["errors"].append(
            f"warehouse capture failed without exposing credentials: {type(exc).__name__}"
        )
        return EXIT_BLOCKED, evidence

    counts = Counter(row["event_name"] for row in landing_rows)
    forbidden_key_hits = sum(
        count_forbidden_keys(row["subject"]) + count_forbidden_keys(row["payload"])
        for row in landing_rows
    )
    unsupported_key_hits = sum(count_unsupported_payload_keys(row) for row in landing_rows)
    unsafe_value_hits = sum(count_unsafe_payload_values(row) for row in landing_rows)
    out_of_scope_names = sorted(name for name in counts if name not in cfg.expected_event_names)
    criteria = success_criteria(
        counts=counts,
        dlq_count=dlq_count,
        forbidden_key_hits=forbidden_key_hits,
        unsupported_key_hits=unsupported_key_hits,
        unsafe_value_hits=unsafe_value_hits,
        out_of_scope_names=out_of_scope_names,
        cfg=cfg,
    )

    evidence.update(
        {
            "classification": "required-analytics-external-capture",
            "landing_count": len(landing_rows),
            "dlq_count": dlq_count,
            "accepted_event_names": dict(sorted(counts.items())),
            "forbidden_field_result": {
                "forbidden_key_count": forbidden_key_hits,
                "unsupported_payload_key_count": unsupported_key_hits,
                "unsafe_payload_value_count": unsafe_value_hits,
                "out_of_scope_event_names": out_of_scope_names,
            },
            "downstream_checks": {
                "dbt": {"status": cfg.dbt_status, "artifact": cfg.dbt_artifact},
                "soda": {"status": cfg.soda_status, "artifact": cfg.soda_artifact},
                "dagster": {"status": cfg.dagster_status, "artifact": cfg.dagster_artifact},
            },
            "lane_artifacts": lane_artifacts(cfg),
            "success_criteria": criteria,
            "stop_rollback_outcome": cfg.stop_rollback_outcome,
        }
    )

    if all(criteria.values()):
        evidence["result"] = "passed"
        return 0, evidence
    evidence["result"] = "failed"
    return EXIT_FAILED, evidence


def load_config(preflight_only: bool) -> tuple[RequiredAnalyticsConfig, list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    target_name = env("EMSI_REQUIRED_ANALYTICS_TARGET_NAME")
    validate_safe_ref("EMSI_REQUIRED_ANALYTICS_TARGET_NAME", target_name, errors)
    if any(marker in target_name.lower() for marker in ("local", "localhost", "dev", "test")):
        errors.append("EMSI_REQUIRED_ANALYTICS_TARGET_NAME must not be local/dev/test")

    target_class = env("EMSI_REQUIRED_ANALYTICS_TARGET_CLASS")
    if target_class not in {"production", "staging-production-equivalent"}:
        errors.append(
            "EMSI_REQUIRED_ANALYTICS_TARGET_CLASS must be production or staging-production-equivalent"
        )

    lanes = parse_lanes(errors)
    approval_ids = parse_csv("EMSI_REQUIRED_ANALYTICS_APPROVAL_IDS")
    if not approval_ids:
        errors.append("EMSI_REQUIRED_ANALYTICS_APPROVAL_IDS is required")
    for approval_id in approval_ids:
        validate_safe_ref("EMSI_REQUIRED_ANALYTICS_APPROVAL_IDS", approval_id, errors)

    seeded_user_ref = env("EMSI_REQUIRED_ANALYTICS_SEEDED_USER_REF")
    validate_safe_ref("EMSI_REQUIRED_ANALYTICS_SEEDED_USER_REF", seeded_user_ref, errors)
    subject_user_hash = env("EMSI_REQUIRED_ANALYTICS_SUBJECT_USER_HASH")
    if not HASH_PATTERN.fullmatch(subject_user_hash):
        errors.append(
            "EMSI_REQUIRED_ANALYTICS_SUBJECT_USER_HASH must be a pseudonymous 64-hex hash"
        )
    elif is_placeholder_hash(subject_user_hash):
        errors.append(
            "EMSI_REQUIRED_ANALYTICS_SUBJECT_USER_HASH must not be a fixture or placeholder hash"
        )

    event_id_prefix = env("EMSI_REQUIRED_ANALYTICS_EVENT_ID_PREFIX")
    validate_safe_ref("EMSI_REQUIRED_ANALYTICS_EVENT_ID_PREFIX", event_id_prefix, errors)
    if len(event_id_prefix) < 16:
        errors.append("EMSI_REQUIRED_ANALYTICS_EVENT_ID_PREFIX must be at least 16 characters")
    if "local" in event_id_prefix.lower() or "dev" in event_id_prefix.lower():
        errors.append("EMSI_REQUIRED_ANALYTICS_EVENT_ID_PREFIX must not identify local/dev evidence")

    window_start = parse_time_env("EMSI_REQUIRED_ANALYTICS_WINDOW_START", errors)
    window_end = parse_time_env("EMSI_REQUIRED_ANALYTICS_WINDOW_END", errors)
    if window_start and window_end:
        duration = (window_end - window_start).total_seconds()
        if duration < 60 or duration > 1800:
            errors.append("required analytics source window must be between 60 and 1800 seconds")
    else:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        window_start = window_start or now
        window_end = window_end or now

    warehouse_dsn = env("EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN")
    validate_warehouse_dsn(warehouse_dsn, errors)

    privacy_artifact = env("EMSI_REQUIRED_ANALYTICS_PRIVACY_ARTIFACT")
    if not privacy_artifact:
        errors.append("EMSI_REQUIRED_ANALYTICS_PRIVACY_ARTIFACT is required")
    elif is_placeholder_ref(privacy_artifact):
        errors.append("EMSI_REQUIRED_ANALYTICS_PRIVACY_ARTIFACT must not be a fixture placeholder")

    cfg = RequiredAnalyticsConfig(
        target_name=target_name,
        target_class=target_class,
        lanes=lanes,
        approval_ids=approval_ids,
        seeded_user_ref=seeded_user_ref,
        subject_user_hash=subject_user_hash,
        event_id_prefix=event_id_prefix,
        window_start=window_start,
        window_end=window_end,
        warehouse_dsn=warehouse_dsn,
        privacy_artifact=privacy_artifact,
        admin_scenario_status=env("EMSI_REQUIRED_ANALYTICS_ADMIN_SCENARIO_STATUS"),
        admin_scenario_artifact=env("EMSI_REQUIRED_ANALYTICS_ADMIN_SCENARIO_ARTIFACT"),
        dbt_status=env("EMSI_REQUIRED_ANALYTICS_DBT_STATUS"),
        dbt_artifact=env("EMSI_REQUIRED_ANALYTICS_DBT_ARTIFACT"),
        soda_status=env("EMSI_REQUIRED_ANALYTICS_SODA_STATUS"),
        soda_artifact=env("EMSI_REQUIRED_ANALYTICS_SODA_ARTIFACT"),
        dagster_status=env("EMSI_REQUIRED_ANALYTICS_DAGSTER_STATUS"),
        dagster_artifact=env("EMSI_REQUIRED_ANALYTICS_DAGSTER_ARTIFACT"),
        feedml_status=env("EMSI_REQUIRED_ANALYTICS_FEEDML_COLLECTION_STATUS"),
        feedml_artifact=env("EMSI_REQUIRED_ANALYTICS_FEEDML_COLLECTION_ARTIFACT"),
        clickhouse_status=env("EMSI_REQUIRED_ANALYTICS_CLICKHOUSE_PARITY_STATUS"),
        clickhouse_artifact=env("EMSI_REQUIRED_ANALYTICS_CLICKHOUSE_PARITY_ARTIFACT"),
        stop_rollback_outcome=env("EMSI_REQUIRED_ANALYTICS_STOP_ROLLBACK_OUTCOME"),
    )

    if preflight_only:
        for key in capture_only_keys(cfg):
            if not env(key):
                warnings.append(f"{key} is not required for preflight but is required for capture")
    else:
        if env("EMSI_REQUIRED_ANALYTICS_ALLOW_CAPTURE") != "true":
            errors.append("EMSI_REQUIRED_ANALYTICS_ALLOW_CAPTURE=true is required for capture")
        if cfg.has_admin_runtime_lane:
            require_check_result(
                "Admin scenario",
                cfg.admin_scenario_status,
                cfg.admin_scenario_artifact,
                errors,
            )
        require_check_result("dbt", cfg.dbt_status, cfg.dbt_artifact, errors)
        require_check_result("Soda", cfg.soda_status, cfg.soda_artifact, errors)
        require_check_result("Dagster", cfg.dagster_status, cfg.dagster_artifact, errors)
        if "feedml" in cfg.lanes:
            require_check_result(
                "Feed ML serving collection",
                cfg.feedml_status,
                cfg.feedml_artifact,
                errors,
            )
        if "clickhouse" in cfg.lanes:
            require_check_result(
                "ClickHouse parity",
                cfg.clickhouse_status,
                cfg.clickhouse_artifact,
                errors,
            )
        if not cfg.stop_rollback_outcome:
            errors.append("EMSI_REQUIRED_ANALYTICS_STOP_ROLLBACK_OUTCOME is required for capture")
        elif is_placeholder_ref(cfg.stop_rollback_outcome):
            errors.append("EMSI_REQUIRED_ANALYTICS_STOP_ROLLBACK_OUTCOME must not be a placeholder")

    return cfg, errors, warnings


def base_evidence(
    cfg: RequiredAnalyticsConfig,
    preflight_only: bool,
    errors: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "classification": "pending",
        "target": {
            "name": cfg.target_name or None,
            "class": cfg.target_class or None,
        },
        "scope": {
            "lanes": list(cfg.lanes),
            "expected_event_names": sorted(cfg.expected_event_names),
            "seeded_user_only": True,
            "local_dev_evidence_allowed": False,
        },
        "approval_ids": list(cfg.approval_ids),
        "seeded_user_ref": cfg.seeded_user_ref or None,
        "event_id_prefix": cfg.event_id_prefix or None,
        "window": {
            "start": isoformat_z(cfg.window_start),
            "end": isoformat_z(cfg.window_end),
            "duration_seconds": cfg.duration_seconds,
        },
        "privacy": {
            "artifact": cfg.privacy_artifact or None,
        },
        "preflight": {
            "mode": "preflight-only" if preflight_only else "capture",
            "passed": False,
            "errors": errors,
            "warnings": warnings,
        },
        "landing_count": None,
        "dlq_count": None,
        "accepted_event_names": {},
        "forbidden_field_result": None,
        "downstream_checks": None,
        "lane_artifacts": lane_artifacts(cfg),
        "success_criteria": None,
        "stop_rollback_outcome": None,
        "redacted": {
            "warehouse_dsn": bool(cfg.warehouse_dsn),
            "subject_user_hash": bool(cfg.subject_user_hash),
        },
    }


def read_required_rows(cfg: RequiredAnalyticsConfig) -> tuple[list[dict[str, Any]], int]:
    try:
        import psycopg2
    except ImportError as exc:  # pragma: no cover - depends on runtime image.
        raise RuntimeError("psycopg2 is required for warehouse capture") from exc

    dlq_end = cfg.window_end + timedelta(minutes=5)
    landing_rows: list[dict[str, Any]] = []
    with psycopg2.connect(cfg.warehouse_dsn) as conn:
        with conn.cursor() as cur:
            if cfg.expected_event_names:
                cur.execute(
                    """
                    SELECT event_name, subject, payload
                      FROM analytics.raw_event_landing
                     WHERE left(event_id, %s) = %s
                       AND subject_user_hash = %s
                       AND occurred_at >= %s
                       AND occurred_at <= %s
                       AND event_name = ANY(%s)
                     ORDER BY occurred_at ASC
                    """,
                    (
                        len(cfg.event_id_prefix),
                        cfg.event_id_prefix,
                        cfg.subject_user_hash,
                        cfg.window_start,
                        cfg.window_end,
                        sorted(cfg.expected_event_names),
                    ),
                )
                landing_rows = [
                    {
                        "event_name": row[0],
                        "subject": normalize_json_value(row[1]),
                        "payload": normalize_json_value(row[2]),
                    }
                    for row in cur.fetchall()
                ]
            cur.execute(
                """
                SELECT count(*)
                  FROM analytics.raw_event_dlq
                 WHERE event_id IS NOT NULL
                   AND left(event_id, %s) = %s
                   AND created_at >= %s
                   AND created_at <= %s
                """,
                (len(cfg.event_id_prefix), cfg.event_id_prefix, cfg.window_start, dlq_end),
            )
            dlq_count = int(cur.fetchone()[0])
    return landing_rows, dlq_count


def success_criteria(
    counts: Counter[str],
    dlq_count: int,
    forbidden_key_hits: int,
    unsupported_key_hits: int,
    unsafe_value_hits: int,
    out_of_scope_names: list[str],
    cfg: RequiredAnalyticsConfig,
) -> dict[str, bool]:
    criteria = {
        "dlq_zero": dlq_count == 0,
        "no_forbidden_fields": forbidden_key_hits == 0
        and unsupported_key_hits == 0
        and unsafe_value_hits == 0,
        "only_expected_event_names": not out_of_scope_names,
        "downstream_checks_passed": (
            cfg.dbt_status == "passed"
            and cfg.soda_status == "passed"
            and cfg.dagster_status == "passed"
        ),
        "stop_rollback_recorded": bool(cfg.stop_rollback_outcome),
    }
    if "admin" in cfg.lanes:
        criteria["admin_surface_viewed_seen"] = counts["admin_surface_viewed"] >= 1
        criteria["admin_action_visibility_seen"] = counts["admin_action_visibility_viewed"] >= 1
    if "reveal" in cfg.lanes:
        criteria["admin_reveal_audit_seen"] = counts["admin_reveal_audit_recorded"] >= 1
    if "note" in cfg.lanes:
        criteria["admin_note_metadata_seen"] = counts["admin_note_metadata_recorded"] >= 1
    if cfg.has_admin_runtime_lane:
        criteria["admin_scenario_artifact_passed"] = cfg.admin_scenario_status == "passed"
    if "feedml" in cfg.lanes:
        criteria["feedml_collection_artifact_passed"] = cfg.feedml_status == "passed"
    if "clickhouse" in cfg.lanes:
        criteria["clickhouse_parity_artifact_passed"] = cfg.clickhouse_status == "passed"
    return criteria


def count_forbidden_keys(value: Any) -> int:
    if isinstance(value, dict):
        total = 0
        for key, nested in value.items():
            normalized = str(key).strip().lower().replace("-", "_")
            squashed = normalized.replace("_", "")
            if normalized in FORBIDDEN_KEYS or squashed in FORBIDDEN_KEYS:
                total += 1
            total += count_forbidden_keys(nested)
        return total
    if isinstance(value, list):
        return sum(count_forbidden_keys(item) for item in value)
    return 0


def count_unsupported_payload_keys(row: dict[str, Any]) -> int:
    payload = row["payload"]
    if not isinstance(payload, dict):
        return 1
    allowed = PAYLOAD_KEYS.get(row["event_name"], set())
    return sum(1 for key in payload if str(key).strip() not in allowed)


def count_unsafe_payload_values(row: dict[str, Any]) -> int:
    payload = row["payload"]
    if not isinstance(payload, dict):
        return 1
    total = 0
    for key, value in payload.items():
        total += unsafe_payload_value_count(row["event_name"], str(key).strip(), value)
    return total


def unsafe_payload_value_count(event_name: str, key: str, value: Any) -> int:
    if value is None or isinstance(value, (bool, int, float)):
        return 0
    if not isinstance(value, str):
        return 1
    trimmed = value.strip()
    if len(trimmed) > 256:
        return 1
    lowered = trimmed.lower()
    if "@" in trimmed or "://" in trimmed or "bearer " in lowered or "token=" in lowered:
        return 1
    if key in {"target_hash", "audit_receipt_hash"}:
        return 0 if bounded_hash_value(trimmed) else 1
    if event_name in PAYLOAD_KEYS and not bounded_metadata_value(trimmed):
        return 1
    if phone_like_value(trimmed):
        return 1
    return 0


def lane_artifacts(cfg: RequiredAnalyticsConfig) -> dict[str, dict[str, str]]:
    artifacts: dict[str, dict[str, str]] = {}
    if cfg.has_admin_runtime_lane:
        artifacts["admin_scenario"] = {
            "status": cfg.admin_scenario_status,
            "artifact": cfg.admin_scenario_artifact,
        }
    if "feedml" in cfg.lanes:
        artifacts["feedml_serving_collection"] = {
            "status": cfg.feedml_status,
            "artifact": cfg.feedml_artifact,
        }
    if "clickhouse" in cfg.lanes:
        artifacts["clickhouse_candidate_parity"] = {
            "status": cfg.clickhouse_status,
            "artifact": cfg.clickhouse_artifact,
        }
    return artifacts


def markdown_summary(evidence: dict[str, Any]) -> str:
    lines = [
        "# Required Analytics External Evidence",
        "",
        f"Classification: `{evidence.get('classification')}`",
        f"Result: `{evidence.get('result', 'pending')}`",
        f"Target: `{evidence['target'].get('name')}` ({evidence['target'].get('class')})",
        f"Lanes: `{', '.join(evidence['scope'].get('lanes', []))}`",
        f"Window: `{evidence['window'].get('start')}` to `{evidence['window'].get('end')}`",
        "",
        "## Counts",
        "",
        f"- Landing count: `{evidence.get('landing_count')}`",
        f"- DLQ count: `{evidence.get('dlq_count')}`",
        f"- Accepted event names: `{json.dumps(evidence.get('accepted_event_names', {}), sort_keys=True)}`",
        "",
        "## Guardrails",
        "",
        f"- Forbidden field result: `{json.dumps(evidence.get('forbidden_field_result'), sort_keys=True)}`",
        f"- Downstream checks: `{json.dumps(evidence.get('downstream_checks'), sort_keys=True)}`",
        f"- Lane artifacts: `{json.dumps(evidence.get('lane_artifacts'), sort_keys=True)}`",
        f"- Success criteria: `{json.dumps(evidence.get('success_criteria'), sort_keys=True)}`",
        f"- Stop/rollback outcome: `{evidence.get('stop_rollback_outcome')}`",
        "",
        "The warehouse DSN and subject hash are intentionally redacted.",
        "",
    ]
    return "\n".join(lines)


def normalize_json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def parse_lanes(errors: list[str]) -> tuple[str, ...]:
    lanes = parse_csv("EMSI_REQUIRED_ANALYTICS_LANES")
    if not lanes:
        errors.append("EMSI_REQUIRED_ANALYTICS_LANES is required")
        return tuple()
    invalid = sorted(set(lanes) - LANES)
    if invalid:
        errors.append(
            "EMSI_REQUIRED_ANALYTICS_LANES contains unsupported lanes: "
            + ", ".join(invalid)
        )
    return tuple(dict.fromkeys(lanes))


def parse_csv(key: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in env(key).split(",") if part.strip())


def capture_only_keys(cfg: RequiredAnalyticsConfig) -> tuple[str, ...]:
    keys = [
        "EMSI_REQUIRED_ANALYTICS_DBT_STATUS",
        "EMSI_REQUIRED_ANALYTICS_DBT_ARTIFACT",
        "EMSI_REQUIRED_ANALYTICS_SODA_STATUS",
        "EMSI_REQUIRED_ANALYTICS_SODA_ARTIFACT",
        "EMSI_REQUIRED_ANALYTICS_DAGSTER_STATUS",
        "EMSI_REQUIRED_ANALYTICS_DAGSTER_ARTIFACT",
        "EMSI_REQUIRED_ANALYTICS_STOP_ROLLBACK_OUTCOME",
    ]
    if cfg.has_admin_runtime_lane:
        keys.extend(
            [
                "EMSI_REQUIRED_ANALYTICS_ADMIN_SCENARIO_STATUS",
                "EMSI_REQUIRED_ANALYTICS_ADMIN_SCENARIO_ARTIFACT",
            ]
        )
    if "feedml" in cfg.lanes:
        keys.extend(
            [
                "EMSI_REQUIRED_ANALYTICS_FEEDML_COLLECTION_STATUS",
                "EMSI_REQUIRED_ANALYTICS_FEEDML_COLLECTION_ARTIFACT",
            ]
        )
    if "clickhouse" in cfg.lanes:
        keys.extend(
            [
                "EMSI_REQUIRED_ANALYTICS_CLICKHOUSE_PARITY_STATUS",
                "EMSI_REQUIRED_ANALYTICS_CLICKHOUSE_PARITY_ARTIFACT",
            ]
        )
    return tuple(keys)


def validate_safe_ref(key: str, value: str, errors: list[str]) -> None:
    if not value:
        errors.append(f"{key} is required")
        return
    if not SAFE_REF_PATTERN.fullmatch(value):
        errors.append(f"{key} must be a bounded non-PII reference")
    if is_placeholder_ref(value):
        errors.append(f"{key} must not be a fixture or placeholder reference")
    lowered = value.lower()
    if "@" in value or "bearer" in lowered or "token" in lowered:
        errors.append(f"{key} must not contain contact or token-shaped data")


def validate_warehouse_dsn(value: str, errors: list[str]) -> None:
    if not value:
        errors.append("EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN is required")
        return
    lowered = value.lower()
    for marker in LOCAL_DSN_MARKERS:
        if marker in lowered:
            errors.append("EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN must not point at local/dev")
            break
    parsed = urlparse(value)
    dsn_parts = (parsed.hostname or "", parsed.username or "", parsed.password or "")
    if any(is_secret_placeholder_ref(part) for part in dsn_parts):
        errors.append(
            "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN must not use fixture or placeholder host/credentials"
        )
    if is_template_secret(parsed.password or ""):
        errors.append("EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN must not use a template password")
    if not any(sslmode in lowered for sslmode in ("sslmode=require", "sslmode=verify-ca", "sslmode=verify-full")):
        errors.append("EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN must require TLS via sslmode")


def require_check_result(name: str, status: str, artifact: str, errors: list[str]) -> None:
    if status not in CHECK_STATUSES:
        errors.append(f"{name} status must be one of {', '.join(sorted(CHECK_STATUSES))}")
    if status != "passed":
        errors.append(f"{name} status must be passed for required analytics capture")
    if not artifact:
        errors.append(f"{name} artifact is required for capture")
    elif is_placeholder_ref(artifact):
        errors.append(f"{name} artifact must not be a fixture placeholder")


def parse_time_env(key: str, errors: list[str]) -> datetime | None:
    value = env(key)
    if not value:
        errors.append(f"{key} is required")
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        errors.append(f"{key} must be ISO-8601 with timezone")
        return None
    if parsed.tzinfo is None:
        errors.append(f"{key} must include timezone")
        return None
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def bounded_metadata_value(value: str) -> bool:
    if value == "" or len(value) > 96:
        return False
    for char in value:
        if not (
            ("a" <= char <= "z")
            or ("A" <= char <= "Z")
            or ("0" <= char <= "9")
            or char in {"_", "-", "."}
        ):
            return False
    return True


def bounded_hash_value(value: str) -> bool:
    prefix = "sha256:"
    return value.startswith(prefix) and bounded_metadata_value(value.removeprefix(prefix))


def phone_like_value(value: str) -> bool:
    digits = sum(1 for char in value if "0" <= char <= "9")
    return digits >= 7


def is_placeholder_ref(value: str) -> bool:
    lowered = value.lower()
    return any(marker in lowered for marker in PLACEHOLDER_MARKERS)


def is_secret_placeholder_ref(value: str) -> bool:
    lowered = value.lower()
    return any(marker in lowered for marker in SECRET_PLACEHOLDER_MARKERS)


def is_placeholder_hash(value: str) -> bool:
    normalized = value.removeprefix("sha256:").lower()
    return len(set(normalized)) <= 2


def is_template_secret(value: str) -> bool:
    return value.lower() in TEMPLATE_SECRET_VALUES


def env(key: str) -> str:
    return os.getenv(key, "").strip()


def isoformat_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
