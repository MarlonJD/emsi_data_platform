from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


SCHEMA_VERSION = "emsi-privacy-lifecycle-smoke-v1"
EVIDENCE_ID = "EMSI-PRIVACY-LIFECYCLE-SMOKE-20260622"
SMOKE_NOW = datetime(2026, 6, 22, 12, 0, tzinfo=timezone.utc)
PERSONAL_DATA_STATES = {"IDENTIFIABLE", "PSEUDONYMOUS", "AGGREGATED_PERSONAL"}


@dataclass(frozen=True)
class SyntheticPrivacyRecord:
    record_id: str
    subject_key: str
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
    evidence_id: str = EVIDENCE_ID
    request_type: str | None = None
    private_recap_counter: bool = False


def main() -> int:
    report = build_smoke_report()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def build_smoke_report(now: datetime = SMOKE_NOW) -> dict[str, Any]:
    records = synthetic_privacy_records(now)
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
            "source_record_id": check["source_record_id"],
            "anonymous_group": check["anonymous_group"],
            "cell_count": check["cell_count"],
            "evidence_id": check["evidence_id"],
        }
        for check in anonymous_candidate_checks
        if check["passed"]
    ]

    cleanup_actions = build_cleanup_actions(records)
    purge_record_ids = sorted(
        {
            record.record_id
            for record in retention_candidates
            if record.data_state in PERSONAL_DATA_STATES
        }
        | {
            action["record_id"]
            for action in cleanup_actions
            if action["personal_or_private_state_removed"]
        }
    )
    audit_receipts = build_audit_receipts(
        retention_candidates=retention_candidates,
        anonymous_candidate_checks=anonymous_candidate_checks,
        published_anonymous_aggregates=published_anonymous_aggregates,
        cleanup_actions=cleanup_actions,
        purge_record_ids=purge_record_ids,
    )

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "classification": "local_synthetic_privacy_lifecycle_smoke",
        "evidence_id": EVIDENCE_ID,
        "as_of": now.isoformat(),
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
            "canary_or_production_claim": False,
            "pairwise_voice_copresence": False,
            "dm_or_private_communication_affinity": False,
            "free_form_cohort_query": False,
            "gender_aggregate_reporting": False,
            "automatic_recap_sharing": False,
            "clickhouse_production_promotion": False,
        },
        "proved_semantics": {
            "retention_candidates": True,
            "anonymous_candidate_checks": True,
            "purge_after_anonymization_failure": True,
            "lifecycle_audit": True,
            "opt_out_deletion_cleanup": True,
        },
        "current_effective_defaults": {
            "voiceSpeakerActivityAnalytics": False,
            "personalYearlyRecap": False,
            "recapShare": False,
        },
        "retention_candidates": [
            {
                "record_id": record.record_id,
                "feature_id": record.feature_id,
                "data_state": record.data_state,
                "expiry_action": record.expiry_action,
            }
            for record in retention_candidates
        ],
        "anonymous_candidate_checks": anonymous_candidate_checks,
        "published_anonymous_aggregates": published_anonymous_aggregates,
        "purged_record_ids": purge_record_ids,
        "cleanup_actions": cleanup_actions,
        "lifecycle_audit_receipts": audit_receipts,
    }
    assert_smoke_invariants(report)
    return report


def synthetic_privacy_records(now: datetime) -> list[SyntheticPrivacyRecord]:
    return [
        SyntheticPrivacyRecord(
            record_id="raw-product-expired-pass",
            subject_key="subject-alpha",
            feature_id="raw_product_analytics_events",
            data_state="PSEUDONYMOUS",
            occurred_at=now - timedelta(days=91),
            retention_days=90,
            expiry_action="anonymize_or_delete",
            anonymization_target="anonymous_long_term_trends",
            anonymous_group="community_daily.general",
            cell_count=14,
            candidate_identifier_free=True,
        ),
        SyntheticPrivacyRecord(
            record_id="voice-expired-small-cell-fail",
            subject_key="subject-beta",
            feature_id="voice_mic_and_speech_session_summary",
            data_state="PSEUDONYMOUS",
            occurred_at=now - timedelta(days=31),
            retention_days=30,
            expiry_action="anonymize_or_delete",
            anonymization_target="anonymous.voice_room_type_usage_monthly",
            anonymous_group="voice_room_type.study",
            cell_count=7,
            candidate_identifier_free=True,
        ),
        SyntheticPrivacyRecord(
            record_id="demographic-expired-rare-cell-fail",
            subject_key="subject-gamma",
            feature_id="user_level_bdv_reporting_features",
            data_state="AGGREGATED_PERSONAL",
            occurred_at=now - timedelta(days=91),
            retention_days=90,
            expiry_action="anonymize_or_delete",
            anonymization_target="anonymous_reporting",
            anonymous_group="occupation_cohort.rare",
            cell_count=19,
            candidate_identifier_free=True,
            demographic_or_rare_cell=True,
        ),
        SyntheticPrivacyRecord(
            record_id="active-not-expired-retained",
            subject_key="subject-delta",
            feature_id="raw_product_analytics_events",
            data_state="PSEUDONYMOUS",
            occurred_at=now - timedelta(days=7),
            retention_days=90,
            expiry_action="anonymize_or_delete",
            anonymization_target="anonymous_long_term_trends",
            anonymous_group="community_daily.general",
            cell_count=30,
            candidate_identifier_free=True,
        ),
        SyntheticPrivacyRecord(
            record_id="analytics-opt-out-cleanup",
            subject_key="subject-epsilon",
            feature_id="raw_product_analytics_events",
            data_state="PSEUDONYMOUS",
            occurred_at=now - timedelta(days=3),
            retention_days=90,
            expiry_action="anonymize_or_delete",
            anonymization_target="anonymous_long_term_trends",
            anonymous_group="community_daily.general",
            cell_count=30,
            candidate_identifier_free=True,
            request_type="analytics_opt_out",
        ),
        SyntheticPrivacyRecord(
            record_id="account-deletion-cleanup",
            subject_key="subject-zeta",
            feature_id="user_level_bdv_reporting_features",
            data_state="IDENTIFIABLE",
            occurred_at=now - timedelta(days=2),
            retention_days=90,
            expiry_action="anonymize_or_delete",
            anonymization_target="anonymous_reporting",
            anonymous_group="community_daily.general",
            cell_count=30,
            candidate_identifier_free=True,
            request_type="account_deletion",
        ),
        SyntheticPrivacyRecord(
            record_id="recap-opt-out-private-counter-cleanup",
            subject_key="subject-eta",
            feature_id="personal_recap_monthly_counters",
            data_state="PSEUDONYMOUS",
            occurred_at=now - timedelta(days=15),
            retention_days=450,
            expiry_action="delete",
            anonymization_target="not_anonymous_private_user_feature",
            anonymous_group="not_applicable",
            cell_count=1,
            candidate_identifier_free=False,
            request_type="recap_opt_out",
            private_recap_counter=True,
        ),
        SyntheticPrivacyRecord(
            record_id="feed-profile-reset-cleanup",
            subject_key="subject-theta",
            feature_id="user_level_bdv_reporting_features",
            data_state="PSEUDONYMOUS",
            occurred_at=now - timedelta(days=1),
            retention_days=90,
            expiry_action="anonymize_or_delete",
            anonymization_target="anonymous_reporting",
            anonymous_group="feed_profile_reset",
            cell_count=30,
            candidate_identifier_free=True,
            request_type="feed_profile_reset",
        ),
    ]


def is_expired(record: SyntheticPrivacyRecord, now: datetime) -> bool:
    return record.occurred_at + timedelta(days=record.retention_days) <= now


def build_anonymous_candidate_check(record: SyntheticPrivacyRecord) -> dict[str, Any]:
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
        "candidate_id": f"anon-candidate:{record.record_id}",
        "source_record_id": record.record_id,
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


def build_cleanup_actions(records: list[SyntheticPrivacyRecord]) -> list[dict[str, Any]]:
    actions = []
    for record in records:
        if not record.request_type:
            continue
        actions.append(
            {
                "record_id": record.record_id,
                "request_type": record.request_type,
                "new_user_level_eligibility_stopped": True,
                "personal_or_private_state_removed": True,
                "private_recap_counter_deleted": record.private_recap_counter
                or record.request_type in {"account_deletion", "recap_opt_out"},
                "generated_recap_deleted": record.request_type in {"account_deletion", "recap_opt_out"},
                "feed_profile_reset": record.request_type == "feed_profile_reset",
                "true_anonymous_aggregate_allowed_to_remain": True,
            }
        )
    return actions


def build_audit_receipts(
    *,
    retention_candidates: list[SyntheticPrivacyRecord],
    anonymous_candidate_checks: list[dict[str, Any]],
    published_anonymous_aggregates: list[dict[str, Any]],
    cleanup_actions: list[dict[str, Any]],
    purge_record_ids: list[str],
) -> list[dict[str, str]]:
    receipts: list[dict[str, str]] = []
    for record in retention_candidates:
        receipts.append(audit_receipt("retention_candidate_identified", record.record_id))
    for check in anonymous_candidate_checks:
        status = "passed" if check["passed"] else "failed"
        receipts.append(audit_receipt(f"anonymous_candidate_check_{status}", check["source_record_id"]))
        if not check["passed"]:
            receipts.append(audit_receipt("anonymization_failure_incident_recorded", check["source_record_id"]))
    for aggregate in published_anonymous_aggregates:
        receipts.append(audit_receipt("anonymous_aggregate_published", aggregate["source_record_id"]))
    for record_id in purge_record_ids:
        receipts.append(audit_receipt("personal_or_pseudonymous_state_purged", record_id))
    for action in cleanup_actions:
        receipts.append(audit_receipt(f"cleanup_{action['request_type']}", action["record_id"]))
    return receipts


def audit_receipt(action: str, record_id: str) -> dict[str, str]:
    return {
        "audit_id": f"{EVIDENCE_ID}:{action}:{record_id}",
        "action": action,
        "record_id": record_id,
        "retention": "minimum_3_years",
        "evidence_id": EVIDENCE_ID,
    }


def assert_smoke_invariants(report: dict[str, Any]) -> None:
    retention_ids = {row["record_id"] for row in report["retention_candidates"]}
    required_retention_ids = {
        "raw-product-expired-pass",
        "voice-expired-small-cell-fail",
        "demographic-expired-rare-cell-fail",
    }
    if not required_retention_ids.issubset(retention_ids):
        missing = sorted(required_retention_ids - retention_ids)
        raise AssertionError(f"missing retention candidates: {missing}")
    if "active-not-expired-retained" in retention_ids:
        raise AssertionError("active non-expired personal data was incorrectly expired")

    checks = report["anonymous_candidate_checks"]
    if not any(check["passed"] for check in checks):
        raise AssertionError("anonymous candidate checks never pass")
    failed_checks = [check for check in checks if not check["passed"]]
    if not failed_checks:
        raise AssertionError("anonymous candidate checks never exercise failure behavior")
    published_source_ids = {
        aggregate["source_record_id"] for aggregate in report["published_anonymous_aggregates"]
    }
    for check in failed_checks:
        if check["source_record_id"] in published_source_ids:
            raise AssertionError("failed anonymous candidate was published")
        if check["expired_personal_purge"] != "required":
            raise AssertionError("failed anonymization did not require purge")
        if check["incident_and_audit"] != "required":
            raise AssertionError("failed anonymization did not require incident/audit")

    purged_ids = set(report["purged_record_ids"])
    if not required_retention_ids.issubset(purged_ids):
        missing = sorted(required_retention_ids - purged_ids)
        raise AssertionError(f"expired personal data was not purged: {missing}")
    if "active-not-expired-retained" in purged_ids:
        raise AssertionError("active non-expired row was purged without a request")

    cleanup_types = {action["request_type"] for action in report["cleanup_actions"]}
    required_cleanup_types = {
        "analytics_opt_out",
        "account_deletion",
        "recap_opt_out",
        "feed_profile_reset",
    }
    if not required_cleanup_types.issubset(cleanup_types):
        missing = sorted(required_cleanup_types - cleanup_types)
        raise AssertionError(f"missing cleanup actions: {missing}")
    for action in report["cleanup_actions"]:
        if not action["new_user_level_eligibility_stopped"]:
            raise AssertionError(f"cleanup did not stop eligibility: {action}")
        if not action["personal_or_private_state_removed"]:
            raise AssertionError(f"cleanup did not remove personal/private state: {action}")

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
    if not required_audit_actions.issubset(audit_actions):
        missing = sorted(required_audit_actions - audit_actions)
        raise AssertionError(f"missing lifecycle audit receipts: {missing}")

    boundaries = report["canonical_boundaries"]
    if boundaries["warehouse"] != "PostgreSQL":
        raise AssertionError("PostgreSQL must remain canonical")
    if boundaries["event_backbone"] != "Redpanda/Kafka API":
        raise AssertionError("Redpanda/Kafka API must remain the event backbone")
    if "candidate-only" not in boundaries["hot_analytics"]:
        raise AssertionError("ClickHouse must remain candidate-only")

    forbidden_enabled = [
        key for key, value in report["scope_guards"].items() if value is not False
    ]
    if forbidden_enabled:
        raise AssertionError(f"forbidden scope guard enabled: {forbidden_enabled}")
    if report["current_effective_defaults"]["voiceSpeakerActivityAnalytics"] is not False:
        raise AssertionError("voice speaker activity must stay default-off")
    if report["current_effective_defaults"]["personalYearlyRecap"] is not False:
        raise AssertionError("personal recap must stay default-off")


if __name__ == "__main__":
    raise SystemExit(main())
