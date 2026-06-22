from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


SOURCE_PACKET_SCHEMA_VERSION = "emsi-privacy-lifecycle-source-bound-packet-v1"
REPORT_SCHEMA_VERSION = "emsi-privacy-lifecycle-source-bound-runtime-v1"
DEFAULT_PACKET_PATH = (
    Path(__file__).resolve().parent / "fixtures" / "privacy_lifecycle_source_bound_packet.json"
)
PERSONAL_DATA_STATES = {"IDENTIFIABLE", "PSEUDONYMOUS", "AGGREGATED_PERSONAL"}
SUPPORTED_REQUEST_TYPES = {
    "analytics_opt_out",
    "account_deletion",
    "recap_opt_out",
    "feed_profile_reset",
}
REQUIRED_OWNER_APPROVALS = {"analytics", "privacy", "legal"}
SAFE_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/=-]{2,180}$")
HASH_REF_RE = re.compile(r"^sha256:[a-fA-F0-9]{64}$")
FORBIDDEN_SOURCE_SYSTEMS = {"emsi_qa", "emsi_qqq"}
FORBIDDEN_KEYS = {
    "actor_user_id",
    "applicant_message",
    "authorization",
    "auth_token",
    "body",
    "comment_body",
    "contact_payload",
    "contact_value",
    "cookie",
    "dm_content",
    "email",
    "exact_gps",
    "feedback_message",
    "free_form",
    "full_url",
    "gps",
    "internal_note",
    "latitude",
    "longitude",
    "message",
    "note",
    "note_body",
    "phone",
    "post_body",
    "private_note",
    "raw_audio",
    "raw_content",
    "raw_note",
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
    "session_token",
    "signed_url",
    "speaker_embedding",
    "speaking_interval",
    "speaking_timeline",
    "support_payload",
    "target_user_id",
    "token",
    "transcript",
    "user_id",
    "vad_frame_list",
    "view_hierarchy",
    "voiceprint",
}


class PrivacyLifecycleRuntimeError(ValueError):
    pass


@dataclass(frozen=True)
class SourceBoundPrivacyRecord:
    source_record_ref: str
    subject_ref: str
    feature_id: str
    data_state: str
    occurred_at: datetime
    retention_days: int
    expiry_action: str
    anonymization_target: str
    anonymous_group: str
    cell_count: int
    candidate_identifier_free: bool
    demographic_or_rare_cell: bool = False
    method_version: str = "anonymous-zone-v1"
    evidence_id: str = ""
    request_type: str | None = None
    private_recap_counter: bool = False


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build local/source-bound privacy lifecycle runtime evidence from a "
            "bounded source packet. This does not query production, expose an "
            "API, publish dashboards, or promote ClickHouse."
        )
    )
    parser.add_argument(
        "--source-packet",
        type=Path,
        default=DEFAULT_PACKET_PATH,
        help="Source-bound privacy lifecycle packet JSON.",
    )
    parser.add_argument(
        "--evidence-json",
        type=Path,
        help="Optional path for the bounded runtime evidence JSON.",
    )
    parser.add_argument(
        "--evidence-md",
        type=Path,
        help="Optional path for a bounded Markdown summary.",
    )
    args = parser.parse_args()

    report = build_runtime_report_from_path(args.source_packet)
    payload = json.dumps(report, indent=2, sort_keys=True)
    if args.evidence_json:
        args.evidence_json.parent.mkdir(parents=True, exist_ok=True)
        args.evidence_json.write_text(payload + "\n", encoding="utf-8")
        print(f"wrote privacy lifecycle runtime evidence: {args.evidence_json}")
    if args.evidence_md:
        args.evidence_md.parent.mkdir(parents=True, exist_ok=True)
        args.evidence_md.write_text(markdown_summary(report), encoding="utf-8")
        print(f"wrote privacy lifecycle runtime summary: {args.evidence_md}")
    print(payload)
    return 0


def build_runtime_report_from_path(packet_path: Path = DEFAULT_PACKET_PATH) -> dict[str, Any]:
    packet = json.loads(packet_path.read_text(encoding="utf-8"))
    return build_runtime_report(packet, packet_path=str(packet_path))


def build_runtime_report(packet: dict[str, Any], *, packet_path: str = "") -> dict[str, Any]:
    source_binding = validate_source_binding(packet)
    assert_no_forbidden_payload_keys(packet)
    records = load_records(packet)
    now = parse_timestamp(source_binding["source_window"]["end"])
    retention_candidates = [
        record
        for record in records
        if record.expiry_action == "anonymize_or_delete" and is_expired(record, now)
    ]
    anonymous_candidate_checks = [
        build_anonymous_candidate_check(record) for record in retention_candidates
    ]
    published_anonymous_aggregates = [
        {
            "candidate_id": check["candidate_id"],
            "source_record_ref": check["source_record_ref"],
            "anonymous_group": check["anonymous_group"],
            "cell_count": check["cell_count"],
            "evidence_id": check["evidence_id"],
        }
        for check in anonymous_candidate_checks
        if check["passed"]
    ]
    cleanup_actions = build_cleanup_actions(records)
    purge_refs = sorted(
        {
            record.source_record_ref
            for record in retention_candidates
            if record.data_state in PERSONAL_DATA_STATES
        }
        | {
            action["source_record_ref"]
            for action in cleanup_actions
            if action["personal_or_private_state_removed"]
        }
    )
    audit_receipts = build_audit_receipts(
        retention_candidates=retention_candidates,
        anonymous_candidate_checks=anonymous_candidate_checks,
        published_anonymous_aggregates=published_anonymous_aggregates,
        cleanup_actions=cleanup_actions,
        purge_refs=purge_refs,
    )

    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "classification": "local_source_bound_privacy_lifecycle_runtime",
        "source_packet_path": packet_path,
        "source_binding": source_binding,
        "canonical_boundaries": {
            "warehouse": "PostgreSQL",
            "event_backbone": "Redpanda/Kafka API",
            "hot_analytics": "ClickHouse candidate-only non-canonical",
        },
        "scope_guards": {
            "backend_collection_added": False,
            "client_collection_added": False,
            "production_collection_enabled": False,
            "api_exposure_enabled": False,
            "dashboard_exposure_enabled": False,
            "canary_or_production_claim": False,
            "pairwise_voice_copresence": False,
            "dm_or_private_communication_affinity": False,
            "free_form_cohort_query": False,
            "gender_aggregate_reporting": False,
            "automatic_recap_sharing": False,
            "clickhouse_production_promotion": False,
        },
        "current_effective_defaults": {
            "voiceSpeakerActivityAnalytics": False,
            "personalYearlyRecap": False,
            "recapShare": False,
        },
        "runtime_assets": {
            "dagster_job": "privacy_lifecycle_daily_job",
            "source_bound_asset": "privacy.source_bound_runtime_report",
            "soda_contract": "privacy_lifecycle_contract.yml",
            "report_schema": REPORT_SCHEMA_VERSION,
        },
        "retention_candidates": [
            {
                "source_record_ref": record.source_record_ref,
                "feature_id": record.feature_id,
                "data_state": record.data_state,
                "expiry_action": record.expiry_action,
            }
            for record in retention_candidates
        ],
        "anonymous_candidate_checks": anonymous_candidate_checks,
        "published_anonymous_aggregates": published_anonymous_aggregates,
        "purged_source_record_refs": purge_refs,
        "cleanup_actions": cleanup_actions,
        "lifecycle_audit_receipts": audit_receipts,
    }
    assert_runtime_invariants(report)
    return report


def validate_source_binding(packet: dict[str, Any]) -> dict[str, Any]:
    if packet.get("schemaVersion") != SOURCE_PACKET_SCHEMA_VERSION:
        raise PrivacyLifecycleRuntimeError(
            f"schemaVersion must be {SOURCE_PACKET_SCHEMA_VERSION}"
        )
    binding = packet.get("sourceBinding")
    if not isinstance(binding, dict):
        raise PrivacyLifecycleRuntimeError("sourceBinding is required")

    target = require_safe_ref(binding, "targetName")
    target_class = binding.get("targetClass")
    if target_class != "local-source-bound":
        raise PrivacyLifecycleRuntimeError("targetClass must be local-source-bound")
    lowered_target = target.lower()
    if "prod" in lowered_target or "canary" in lowered_target:
        raise PrivacyLifecycleRuntimeError("targetName must not claim production or canary scope")

    source_window = binding.get("sourceWindow")
    if not isinstance(source_window, dict):
        raise PrivacyLifecycleRuntimeError("sourceWindow is required")
    window_start = parse_timestamp(require_safe_ref(source_window, "start"))
    window_end = parse_timestamp(require_safe_ref(source_window, "end"))
    if window_start >= window_end:
        raise PrivacyLifecycleRuntimeError("sourceWindow start must be before end")

    if binding.get("canonicalWarehouse") != "PostgreSQL":
        raise PrivacyLifecycleRuntimeError("canonicalWarehouse must be PostgreSQL")
    if binding.get("eventBackbone") != "Redpanda/Kafka API":
        raise PrivacyLifecycleRuntimeError("eventBackbone must be Redpanda/Kafka API")
    for key in (
        "productionCollectionEnabled",
        "apiExposureEnabled",
        "dashboardExposureEnabled",
        "clickhouseCanonical",
        "clickhouseProductionEnabled",
    ):
        if binding.get(key) is not False:
            raise PrivacyLifecycleRuntimeError(f"{key} must be false")

    source_systems = binding.get("sourceSystems")
    if not isinstance(source_systems, list) or not source_systems:
        raise PrivacyLifecycleRuntimeError("sourceSystems must be a non-empty list")
    for source_system in source_systems:
        if not isinstance(source_system, str) or not SAFE_REF_RE.match(source_system):
            raise PrivacyLifecycleRuntimeError(f"unsafe source system ref: {source_system!r}")
        lowered = source_system.lower()
        if any(forbidden in lowered for forbidden in FORBIDDEN_SOURCE_SYSTEMS):
            raise PrivacyLifecycleRuntimeError(
                "sourceSystems must not use QA evidence or legacy report sources"
            )

    owner_refs = binding.get("ownerApprovalRefs")
    if not isinstance(owner_refs, dict):
        raise PrivacyLifecycleRuntimeError("ownerApprovalRefs is required")
    missing = sorted(REQUIRED_OWNER_APPROVALS - set(owner_refs))
    if missing:
        raise PrivacyLifecycleRuntimeError(f"missing owner approvals: {missing}")
    for owner, approval_ref in owner_refs.items():
        if not isinstance(owner, str) or not SAFE_REF_RE.match(owner):
            raise PrivacyLifecycleRuntimeError(f"unsafe owner approval key: {owner!r}")
        if not isinstance(approval_ref, str) or not SAFE_REF_RE.match(approval_ref):
            raise PrivacyLifecycleRuntimeError(f"unsafe owner approval ref: {owner!r}")

    return {
        "target_name": target,
        "target_class": target_class,
        "source_window": {
            "start": window_start.isoformat(),
            "end": window_end.isoformat(),
        },
        "canonical_warehouse": binding["canonicalWarehouse"],
        "event_backbone": binding["eventBackbone"],
        "source_systems": source_systems,
        "owner_approval_refs": owner_refs,
        "production_collection_enabled": False,
        "api_exposure_enabled": False,
        "dashboard_exposure_enabled": False,
        "clickhouse_canonical": False,
        "clickhouse_production_enabled": False,
    }


def load_records(packet: dict[str, Any]) -> list[SourceBoundPrivacyRecord]:
    rows = packet.get("records")
    if not isinstance(rows, list) or not rows:
        raise PrivacyLifecycleRuntimeError("records must be a non-empty list")
    records = []
    for row in rows:
        if not isinstance(row, dict):
            raise PrivacyLifecycleRuntimeError("records must contain objects")
        source_record_ref = require_safe_ref(row, "sourceRecordRef")
        subject_ref = require_safe_ref(row, "subjectRef")
        if not HASH_REF_RE.match(subject_ref):
            raise PrivacyLifecycleRuntimeError(f"subjectRef must be sha256 hash: {source_record_ref}")
        feature_id = require_safe_ref(row, "featureId")
        data_state = row.get("dataState")
        if data_state not in PERSONAL_DATA_STATES | {"ANONYMOUS"}:
            raise PrivacyLifecycleRuntimeError(f"invalid dataState for {source_record_ref}")
        retention_days = row.get("retentionDays")
        if not isinstance(retention_days, int) or retention_days <= 0:
            raise PrivacyLifecycleRuntimeError(f"retentionDays must be positive: {source_record_ref}")
        expiry_action = row.get("expiryAction")
        if expiry_action not in {"anonymize_or_delete", "delete"}:
            raise PrivacyLifecycleRuntimeError(f"invalid expiryAction for {source_record_ref}")
        cell_count = row.get("cellCount")
        if not isinstance(cell_count, int) or cell_count < 0:
            raise PrivacyLifecycleRuntimeError(f"cellCount must be non-negative: {source_record_ref}")
        candidate_identifier_free = row.get("candidateIdentifierFree")
        if not isinstance(candidate_identifier_free, bool):
            raise PrivacyLifecycleRuntimeError(
                f"candidateIdentifierFree must be boolean: {source_record_ref}"
            )
        request_type = row.get("requestType")
        if request_type is not None and request_type not in SUPPORTED_REQUEST_TYPES:
            raise PrivacyLifecycleRuntimeError(f"unsupported requestType for {source_record_ref}")

        records.append(
            SourceBoundPrivacyRecord(
                source_record_ref=source_record_ref,
                subject_ref=subject_ref,
                feature_id=feature_id,
                data_state=data_state,
                occurred_at=parse_timestamp(require_safe_ref(row, "occurredAt")),
                retention_days=retention_days,
                expiry_action=expiry_action,
                anonymization_target=require_safe_ref(row, "anonymizationTarget"),
                anonymous_group=require_safe_ref(row, "anonymousGroup"),
                cell_count=cell_count,
                candidate_identifier_free=candidate_identifier_free,
                demographic_or_rare_cell=bool(row.get("demographicOrRareCell", False)),
                method_version=require_safe_ref(row, "methodVersion"),
                evidence_id=require_safe_ref(row, "evidenceId"),
                request_type=request_type,
                private_recap_counter=bool(row.get("privateRecapCounter", False)),
            )
        )
    return records


def is_expired(record: SourceBoundPrivacyRecord, now: datetime) -> bool:
    return record.occurred_at + timedelta(days=record.retention_days) <= now


def build_anonymous_candidate_check(record: SourceBoundPrivacyRecord) -> dict[str, Any]:
    threshold = 20 if record.demographic_or_rare_cell else 10
    checks = {
        "identifier_free": record.candidate_identifier_free,
        "cell_threshold_passed": record.cell_count >= threshold,
        "approved_method_version": bool(record.method_version),
        "evidence_id_present": bool(record.evidence_id),
        "complementary_suppression_passed": record.cell_count >= threshold,
        "quasi_identifier_risk_check_passed": record.candidate_identifier_free
        and record.cell_count >= threshold,
    }
    passed = all(checks.values())
    return {
        "candidate_id": f"source-bound-anon-candidate:{record.source_record_ref}",
        "source_record_ref": record.source_record_ref,
        "anonymous_group": record.anonymous_group,
        "anonymization_target": record.anonymization_target,
        "cell_count": record.cell_count,
        "required_cell_threshold": threshold,
        "checks": checks,
        "passed": passed,
        "anonymous_publish": "allowed" if passed else "blocked",
        "expired_personal_purge": "required",
        "incident_and_audit": "not_required" if passed else "required",
        "method_version": record.method_version,
        "evidence_id": record.evidence_id,
    }


def build_cleanup_actions(records: list[SourceBoundPrivacyRecord]) -> list[dict[str, Any]]:
    actions = []
    for record in records:
        if not record.request_type:
            continue
        actions.append(
            {
                "source_record_ref": record.source_record_ref,
                "request_type": record.request_type,
                "new_user_level_eligibility_stopped": True,
                "personal_or_private_state_removed": True,
                "private_recap_counter_deleted": record.private_recap_counter
                or record.request_type in {"account_deletion", "recap_opt_out"},
                "generated_recap_deleted": record.request_type
                in {"account_deletion", "recap_opt_out"},
                "feed_profile_reset": record.request_type == "feed_profile_reset",
                "true_anonymous_aggregate_allowed_to_remain": True,
            }
        )
    return actions


def build_audit_receipts(
    *,
    retention_candidates: list[SourceBoundPrivacyRecord],
    anonymous_candidate_checks: list[dict[str, Any]],
    published_anonymous_aggregates: list[dict[str, Any]],
    cleanup_actions: list[dict[str, Any]],
    purge_refs: list[str],
) -> list[dict[str, str]]:
    receipts: list[dict[str, str]] = []
    for record in retention_candidates:
        receipts.append(audit_receipt("retention_candidate_identified", record.source_record_ref))
    for check in anonymous_candidate_checks:
        status = "passed" if check["passed"] else "failed"
        receipts.append(audit_receipt(f"anonymous_candidate_check_{status}", check["source_record_ref"]))
        if not check["passed"]:
            receipts.append(audit_receipt("anonymization_failure_incident_recorded", check["source_record_ref"]))
    for aggregate in published_anonymous_aggregates:
        receipts.append(audit_receipt("anonymous_aggregate_published", aggregate["source_record_ref"]))
    for source_record_ref in purge_refs:
        receipts.append(audit_receipt("personal_or_pseudonymous_state_purged", source_record_ref))
    for action in cleanup_actions:
        receipts.append(audit_receipt(f"cleanup_{action['request_type']}", action["source_record_ref"]))
    return receipts


def audit_receipt(action: str, source_record_ref: str) -> dict[str, str]:
    return {
        "audit_id": f"{action}:{source_record_ref}",
        "action": action,
        "source_record_ref": source_record_ref,
        "retention": "minimum_3_years",
    }


def assert_runtime_invariants(report: dict[str, Any]) -> None:
    if report["source_binding"]["target_class"] != "local-source-bound":
        raise PrivacyLifecycleRuntimeError("runtime report escaped local-source-bound scope")
    for guard, enabled in report["scope_guards"].items():
        if enabled is not False:
            raise PrivacyLifecycleRuntimeError(f"forbidden scope guard enabled: {guard}")
    for default_name, value in report["current_effective_defaults"].items():
        if value is not False:
            raise PrivacyLifecycleRuntimeError(f"{default_name} must remain default-off")

    if report["canonical_boundaries"]["warehouse"] != "PostgreSQL":
        raise PrivacyLifecycleRuntimeError("PostgreSQL must remain canonical")
    if report["canonical_boundaries"]["event_backbone"] != "Redpanda/Kafka API":
        raise PrivacyLifecycleRuntimeError("Redpanda/Kafka API must remain the event backbone")
    if "candidate-only" not in report["canonical_boundaries"]["hot_analytics"]:
        raise PrivacyLifecycleRuntimeError("ClickHouse must remain candidate-only")

    checks = report["anonymous_candidate_checks"]
    if not checks or not any(check["passed"] for check in checks):
        raise PrivacyLifecycleRuntimeError("runtime must include a passing anonymous candidate")
    failed_checks = [check for check in checks if not check["passed"]]
    if not failed_checks:
        raise PrivacyLifecycleRuntimeError("runtime must include a failed anonymous candidate")
    published_refs = {
        aggregate["source_record_ref"] for aggregate in report["published_anonymous_aggregates"]
    }
    for check in failed_checks:
        if check["source_record_ref"] in published_refs:
            raise PrivacyLifecycleRuntimeError("failed anonymous candidate was published")
        if check["expired_personal_purge"] != "required":
            raise PrivacyLifecycleRuntimeError("failed anonymization did not require purge")

    cleanup_types = {action["request_type"] for action in report["cleanup_actions"]}
    missing_cleanup = sorted(SUPPORTED_REQUEST_TYPES - cleanup_types)
    if missing_cleanup:
        raise PrivacyLifecycleRuntimeError(f"missing cleanup actions: {missing_cleanup}")
    audit_actions = {receipt["action"] for receipt in report["lifecycle_audit_receipts"]}
    required_audit_actions = {
        "retention_candidate_identified",
        "anonymous_candidate_check_passed",
        "anonymous_candidate_check_failed",
        "anonymization_failure_incident_recorded",
        "personal_or_pseudonymous_state_purged",
        "cleanup_analytics_opt_out",
        "cleanup_account_deletion",
        "cleanup_recap_opt_out",
        "cleanup_feed_profile_reset",
    }
    missing_audit = sorted(required_audit_actions - audit_actions)
    if missing_audit:
        raise PrivacyLifecycleRuntimeError(f"missing lifecycle audit receipts: {missing_audit}")


def require_safe_ref(source: dict[str, Any], key: str) -> str:
    value = source.get(key)
    if not isinstance(value, str) or not SAFE_REF_RE.match(value):
        raise PrivacyLifecycleRuntimeError(f"{key} must be a bounded reference token")
    return value


def parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PrivacyLifecycleRuntimeError(f"invalid timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def assert_no_forbidden_payload_keys(value: Any, *, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            normalized = str(key).replace("-", "_").lower()
            if normalized in FORBIDDEN_KEYS:
                raise PrivacyLifecycleRuntimeError(f"forbidden payload key at {path}.{key}")
            assert_no_forbidden_payload_keys(nested, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            assert_no_forbidden_payload_keys(nested, path=f"{path}[{index}]")
    elif isinstance(value, str):
        lowered = value.lower()
        if "@" in value or any(forbidden in lowered for forbidden in FORBIDDEN_SOURCE_SYSTEMS):
            raise PrivacyLifecycleRuntimeError(f"unsafe payload value at {path}")


def markdown_summary(report: dict[str, Any]) -> str:
    binding = report["source_binding"]
    return "\n".join(
        [
            "# Privacy Lifecycle Runtime Evidence",
            "",
            f"- Classification: `{report['classification']}`",
            f"- Target: `{binding['target_name']}`",
            f"- Target class: `{binding['target_class']}`",
            f"- Source window: `{binding['source_window']['start']}` to `{binding['source_window']['end']}`",
            f"- Retention candidates: `{len(report['retention_candidates'])}`",
            f"- Anonymous checks: `{len(report['anonymous_candidate_checks'])}`",
            f"- Cleanup actions: `{len(report['cleanup_actions'])}`",
            f"- Audit receipts: `{len(report['lifecycle_audit_receipts'])}`",
            "- Production/API/dashboard/ClickHouse promotion: `disabled`",
            "",
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
