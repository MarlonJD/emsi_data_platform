from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from ingest_worker import required_analytics_capture as capture


class RequiredAnalyticsCaptureTest(unittest.TestCase):
    def test_preflight_blocks_when_required_inputs_are_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, capture.EXIT_BLOCKED)
        self.assertEqual(evidence["classification"], "blocked-access-gate")
        self.assertFalse(evidence["preflight"]["passed"])
        self.assertIn(
            "EMSI_REQUIRED_ANALYTICS_TARGET_NAME is required",
            evidence["preflight"]["errors"],
        )
        self.assertIn(
            "EMSI_REQUIRED_ANALYTICS_LANES is required",
            evidence["preflight"]["errors"],
        )

    def test_preflight_rejects_local_target_and_warehouse(self) -> None:
        env = valid_preflight_env()
        env["EMSI_REQUIRED_ANALYTICS_TARGET_NAME"] = "local-admin-test"
        env["EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN"] = (
            "postgres://emsi:emsi@localhost:5432/analytics?sslmode=disable"
        )
        with patch.dict(os.environ, env, clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, capture.EXIT_BLOCKED)
        self.assertIn(
            "EMSI_REQUIRED_ANALYTICS_TARGET_NAME must not be local/dev/test",
            evidence["preflight"]["errors"],
        )
        self.assertIn(
            "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN must not point at local/dev",
            evidence["preflight"]["errors"],
        )

    def test_preflight_rejects_redacted_warehouse_credentials(self) -> None:
        env = valid_preflight_env()
        env["EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN"] = (
            "postgres://analytics_reader:redacted@warehouse.emsi-prod.net:5432/"
            "analytics?sslmode=verify-full"
        )
        with patch.dict(os.environ, env, clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, capture.EXIT_BLOCKED)
        self.assertIn(
            "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN must not use fixture or placeholder host/credentials",
            evidence["preflight"]["errors"],
        )

    def test_preflight_rejects_local_hostaddr_without_approved_tunnel(self) -> None:
        env = valid_preflight_env()
        env["EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN"] = (
            "postgres://analytics_reader:reader_password@warehouse.emsi-prod.net:15432/"
            "analytics?hostaddr=127.0.0.1&sslmode=verify-full"
        )
        with patch.dict(os.environ, env, clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, capture.EXIT_BLOCKED)
        self.assertIn(
            "local hostaddr requires EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_MODE=ssh-approved-warehouse",
            evidence["preflight"]["errors"],
        )

    def test_preflight_allows_approved_ssh_tunnel_hostaddr(self) -> None:
        env = valid_preflight_env()
        env.update(
            {
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN": (
                    "postgres://analytics_reader:reader_password@warehouse.emsi-prod.net:15432/"
                    "analytics?hostaddr=127.0.0.1&sslmode=verify-full"
                ),
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_MODE": "ssh-approved-warehouse",
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_SSH_HOST": "emsi-prod-bastion",
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_REMOTE_HOST": "warehouse.emsi-prod.net",
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_LOCAL_PORT": "15432",
            }
        )
        with patch.dict(os.environ, env, clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, 0)
        self.assertEqual(evidence["classification"], "preflight-ready")

    def test_preflight_allows_owner_approved_local_staging_warehouse(self) -> None:
        env = valid_preflight_env()
        env.update(
            {
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN": (
                    "postgres://analytics:analytics_local_password@"
                    "qa-required-analytics.internal:5438/analytics?"
                    "hostaddr=127.0.0.1&sslmode=disable"
                ),
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_MODE": (
                    "owner-approved-local-warehouse"
                ),
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_REMOTE_HOST": (
                    "qa-required-analytics.internal"
                ),
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_LOCAL_PORT": "5438",
            }
        )
        with patch.dict(os.environ, env, clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, 0)
        self.assertEqual(evidence["classification"], "preflight-ready")
        self.assertEqual(evidence["target"]["class"], "staging-production-equivalent")

    def test_owner_approved_local_warehouse_rejects_production_target(self) -> None:
        env = valid_preflight_env()
        env.update(
            {
                "EMSI_REQUIRED_ANALYTICS_TARGET_CLASS": "production",
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN": (
                    "postgres://analytics:analytics_local_password@"
                    "qa-required-analytics.internal:5438/analytics?"
                    "hostaddr=127.0.0.1&sslmode=disable"
                ),
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_MODE": (
                    "owner-approved-local-warehouse"
                ),
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_REMOTE_HOST": (
                    "qa-required-analytics.internal"
                ),
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_LOCAL_PORT": "5438",
            }
        )
        with patch.dict(os.environ, env, clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, capture.EXIT_BLOCKED)
        self.assertIn(
            "owner-approved local warehouse mode must not be used for production targets",
            evidence["preflight"]["errors"],
        )

    def test_owner_approved_local_warehouse_requires_non_local_hostname(self) -> None:
        env = valid_preflight_env()
        env.update(
            {
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN": (
                    "postgres://analytics:analytics_local_password@localhost:5438/"
                    "analytics?hostaddr=127.0.0.1&sslmode=disable"
                ),
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_MODE": (
                    "owner-approved-local-warehouse"
                ),
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_REMOTE_HOST": (
                    "qa-required-analytics.internal"
                ),
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_LOCAL_PORT": "5438",
            }
        )
        with patch.dict(os.environ, env, clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, capture.EXIT_BLOCKED)
        self.assertIn(
            "owner-approved local warehouse DSN must keep a non-local approved hostname",
            evidence["preflight"]["errors"],
        )

    def test_preflight_rejects_ssh_tunnel_without_verify_full(self) -> None:
        env = valid_preflight_env()
        env.update(
            {
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN": (
                    "postgres://analytics_reader:reader_password@warehouse.emsi-prod.net:15432/"
                    "analytics?hostaddr=127.0.0.1&sslmode=require"
                ),
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_MODE": "ssh-approved-warehouse",
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_SSH_HOST": "emsi-prod-bastion",
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_REMOTE_HOST": "warehouse.emsi-prod.net",
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_LOCAL_PORT": "15432",
            }
        )
        with patch.dict(os.environ, env, clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, capture.EXIT_BLOCKED)
        self.assertIn(
            "SSH tunnel warehouse DSNs must use sslmode=verify-full",
            evidence["preflight"]["errors"],
        )

    def test_preflight_rejects_internal_qa_local_warehouse_without_ack(self) -> None:
        env = valid_preflight_env()
        env.update(
            {
                "EMSI_REQUIRED_ANALYTICS_TARGET_CLASS": "internal-qa",
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN": (
                    "postgres://analytics:analytics_local_password@localhost:5438/"
                    "analytics?sslmode=disable"
                ),
            }
        )
        with patch.dict(os.environ, env, clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, capture.EXIT_BLOCKED)
        self.assertIn(
            "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN must not point at local/dev",
            evidence["preflight"]["errors"],
        )

    def test_preflight_allows_internal_qa_local_warehouse_with_ack(self) -> None:
        env = valid_preflight_env()
        env.update(
            {
                "EMSI_REQUIRED_ANALYTICS_TARGET_CLASS": "internal-qa",
                "EMSI_REQUIRED_ANALYTICS_ALLOW_INTERNAL_QA_LOCAL_WAREHOUSE": "true",
                "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN": (
                    "postgres://analytics:analytics_local_password@localhost:5438/"
                    "analytics?sslmode=disable"
                ),
            }
        )
        with patch.dict(os.environ, env, clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, 0)
        self.assertEqual(evidence["classification"], "preflight-ready")
        self.assertTrue(evidence["scope"]["local_dev_evidence_allowed"])

    def test_preflight_passes_with_required_external_inputs(self) -> None:
        with patch.dict(os.environ, valid_preflight_env(), clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, 0)
        self.assertEqual(evidence["classification"], "preflight-ready")
        self.assertTrue(evidence["preflight"]["passed"])
        self.assertEqual(
            evidence["scope"]["expected_event_names"],
            [
                "admin_action_visibility_viewed",
                "admin_note_metadata_recorded",
                "admin_reveal_audit_recorded",
                "admin_surface_viewed",
            ],
        )

    def test_capture_requires_explicit_opt_in_and_artifacts(self) -> None:
        with patch.dict(os.environ, valid_preflight_env(), clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=False)

        self.assertEqual(exit_code, capture.EXIT_BLOCKED)
        self.assertIn(
            "EMSI_REQUIRED_ANALYTICS_ALLOW_CAPTURE=true is required for capture",
            evidence["preflight"]["errors"],
        )
        self.assertIn(
            "Admin scenario status must be one of blocked, failed, passed, skipped",
            evidence["preflight"]["errors"],
        )
        self.assertIn(
            "Feed ML serving collection status must be one of blocked, failed, passed, skipped",
            evidence["preflight"]["errors"],
        )

    def test_capture_passes_with_bounded_rows_and_artifacts(self) -> None:
        env = valid_capture_env()
        rows = [
            landing_row(
                "admin_surface_viewed",
                {
                    "admin_surface": "admin_console",
                    "module_key": "users",
                    "screen_key": "admin.users",
                    "result_state": "loaded",
                    "role_scope_key": "staff.owner",
                },
            ),
            landing_row(
                "admin_action_visibility_viewed",
                {
                    "admin_surface": "admin_console",
                    "module_key": "users",
                    "screen_key": "admin.users",
                    "action_key": "contact_reveal",
                    "action_surface": "users_table",
                    "result_state": "enabled",
                    "role_scope_key": "staff.owner",
                    "proposal_mode": "direct",
                    "target_type": "user",
                    "target_hash": "sha256:target_hash",
                },
            ),
            landing_row(
                "admin_reveal_audit_recorded",
                {
                    "admin_surface": "admin_console",
                    "module_key": "users",
                    "screen_key": "admin.users",
                    "reveal_field_class": "contact",
                    "reveal_result": "success",
                    "actor_role_scope_key": "support.global",
                    "reason_length_bucket": "21_100",
                    "reason_category": "support_follow_up",
                    "confirmation_state": "confirmed",
                    "authorization_outcome": "allowed",
                    "audit_action_key": "reveal_user_contact",
                    "audit_receipt_hash": "sha256:audit_receipt",
                    "target_type": "user",
                    "target_hash": "sha256:target_hash",
                },
            ),
            landing_row(
                "admin_note_metadata_recorded",
                {
                    "admin_surface": "admin_console",
                    "module_key": "applications",
                    "screen_key": "admin.applications",
                    "note_surface": "application_review",
                    "note_type": "staff_decision_reason",
                    "note_action": "created",
                    "note_length_bucket": "21_100",
                    "sensitivity_class": "p2",
                    "redaction_class": "metadata_only",
                    "has_attachment": False,
                    "lifecycle_state": "active",
                    "lifecycle_bucket": "same_day",
                    "target_type": "application",
                    "target_hash": "sha256:application_hash",
                },
            ),
        ]
        with patch.dict(os.environ, env, clear=True), patch.object(
            capture, "read_required_rows", return_value=(rows, 0)
        ):
            exit_code, evidence = capture.collect_evidence(preflight_only=False)

        self.assertEqual(exit_code, 0)
        self.assertEqual(evidence["result"], "passed")
        self.assertEqual(evidence["landing_count"], 4)
        self.assertTrue(all(evidence["success_criteria"].values()))

    def test_capture_allows_redacted_artifact_refs(self) -> None:
        env = valid_capture_env()
        env["EMSI_REQUIRED_ANALYTICS_DBT_ARTIFACT"] = (
            "artifacts/required-analytics/redacted-dbt-run-results-20260621.json"
        )
        with patch.dict(os.environ, env, clear=True), patch.object(
            capture, "read_required_rows", return_value=(valid_landing_rows(), 0)
        ):
            exit_code, evidence = capture.collect_evidence(preflight_only=False)

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            evidence["downstream_checks"]["dbt"]["artifact"],
            "artifacts/required-analytics/redacted-dbt-run-results-20260621.json",
        )

    def test_capture_fails_when_raw_note_text_lands(self) -> None:
        env = valid_capture_env()
        rows = [
            landing_row(
                "admin_note_metadata_recorded",
                {
                    "admin_surface": "admin_console",
                    "module_key": "applications",
                    "screen_key": "admin.applications",
                    "note_surface": "application review private note",
                    "note_type": "staff_decision_reason",
                    "note_action": "created",
                    "note_length_bucket": "21_100",
                    "sensitivity_class": "p2",
                    "redaction_class": "metadata_only",
                    "has_attachment": False,
                    "lifecycle_state": "active",
                    "lifecycle_bucket": "same_day",
                    "target_type": "application",
                },
            )
        ]
        with patch.dict(os.environ, env, clear=True), patch.object(
            capture, "read_required_rows", return_value=(rows, 0)
        ):
            exit_code, evidence = capture.collect_evidence(preflight_only=False)

        self.assertEqual(exit_code, capture.EXIT_FAILED)
        self.assertGreater(
            evidence["forbidden_field_result"]["unsafe_payload_value_count"],
            0,
        )
        self.assertFalse(evidence["success_criteria"]["no_forbidden_fields"])


def valid_preflight_env() -> dict[str, str]:
    return {
        "EMSI_REQUIRED_ANALYTICS_TARGET_NAME": "required-analytics-staging-a",
        "EMSI_REQUIRED_ANALYTICS_TARGET_CLASS": "staging-production-equivalent",
        "EMSI_REQUIRED_ANALYTICS_LANES": "admin,reveal,note,feedml,clickhouse",
        "EMSI_REQUIRED_ANALYTICS_APPROVAL_IDS": (
            "approval:product:2026-06-21:required-analytics,"
            "approval:privacy:2026-06-21:required-analytics"
        ),
        "EMSI_REQUIRED_ANALYTICS_SEEDED_USER_REF": "staff-seeded-ra-20260621-a",
        "EMSI_REQUIRED_ANALYTICS_SUBJECT_USER_HASH": (
            "2f1c4e6a8b0d3f5a7c9e1b4d6f8a0c2e"
            "3f5a7c9e1b4d6f8a0c2e4f6a8b0d3f5a"
        ),
        "EMSI_REQUIRED_ANALYTICS_EVENT_ID_PREFIX": "required-analytics-20260621-a",
        "EMSI_REQUIRED_ANALYTICS_WINDOW_START": "2026-06-21T14:00:00Z",
        "EMSI_REQUIRED_ANALYTICS_WINDOW_END": "2026-06-21T14:10:00Z",
        "EMSI_REQUIRED_ANALYTICS_WAREHOUSE_DSN": (
            "postgres://analytics_reader:reader_password@warehouse.emsi-prod.net:5432/"
            "analytics?sslmode=verify-full"
        ),
        "EMSI_REQUIRED_ANALYTICS_PRIVACY_ARTIFACT": (
            "privacy-evidence/required-analytics/20260621/staff-seeded-ra-consent-v1.json"
        ),
    }


def valid_capture_env() -> dict[str, str]:
    env = valid_preflight_env()
    env.update(
        {
            "EMSI_REQUIRED_ANALYTICS_ALLOW_CAPTURE": "true",
            "EMSI_REQUIRED_ANALYTICS_ADMIN_SCENARIO_STATUS": "passed",
            "EMSI_REQUIRED_ANALYTICS_ADMIN_SCENARIO_ARTIFACT": (
                "qa/required-analytics/admin-scenario-20260621.json"
            ),
            "EMSI_REQUIRED_ANALYTICS_DBT_STATUS": "passed",
            "EMSI_REQUIRED_ANALYTICS_DBT_ARTIFACT": (
                "artifacts/required-analytics/dbt-run-results-20260621.json"
            ),
            "EMSI_REQUIRED_ANALYTICS_SODA_STATUS": "passed",
            "EMSI_REQUIRED_ANALYTICS_SODA_ARTIFACT": (
                "artifacts/required-analytics/soda-raw-event-landing-20260621.json"
            ),
            "EMSI_REQUIRED_ANALYTICS_DAGSTER_STATUS": "passed",
            "EMSI_REQUIRED_ANALYTICS_DAGSTER_ARTIFACT": (
                "dagster-run:required-analytics:20260621"
            ),
            "EMSI_REQUIRED_ANALYTICS_FEEDML_COLLECTION_STATUS": "passed",
            "EMSI_REQUIRED_ANALYTICS_FEEDML_COLLECTION_ARTIFACT": (
                "reports/feedml-serving-collection-required-analytics-20260621.json"
            ),
            "EMSI_REQUIRED_ANALYTICS_CLICKHOUSE_PARITY_STATUS": "passed",
            "EMSI_REQUIRED_ANALYTICS_CLICKHOUSE_PARITY_ARTIFACT": (
                "artifacts/clickhouse-required-analytics-parity-20260621.json"
            ),
            "EMSI_REQUIRED_ANALYTICS_STOP_ROLLBACK_OUTCOME": "stopped_cleanly",
        }
    )
    return env


def valid_landing_rows() -> list[dict[str, object]]:
    return [
        landing_row(
            "admin_surface_viewed",
            {
                "admin_surface": "admin_console",
                "module_key": "users",
                "screen_key": "admin.users",
                "result_state": "loaded",
                "role_scope_key": "staff.owner",
            },
        ),
        landing_row(
            "admin_action_visibility_viewed",
            {
                "admin_surface": "admin_console",
                "module_key": "users",
                "screen_key": "admin.users",
                "action_key": "contact_reveal",
                "action_surface": "users_table",
                "result_state": "enabled",
                "role_scope_key": "staff.owner",
                "proposal_mode": "direct",
                "target_type": "user",
                "target_hash": "sha256:target_hash",
            },
        ),
        landing_row(
            "admin_reveal_audit_recorded",
            {
                "admin_surface": "admin_console",
                "module_key": "users",
                "screen_key": "admin.users",
                "reveal_field_class": "contact",
                "reveal_result": "success",
                "actor_role_scope_key": "support.global",
                "reason_length_bucket": "21_100",
                "reason_category": "support_follow_up",
                "confirmation_state": "confirmed",
                "authorization_outcome": "allowed",
                "audit_action_key": "reveal_user_contact",
                "audit_receipt_hash": "sha256:audit_receipt",
                "target_type": "user",
                "target_hash": "sha256:target_hash",
            },
        ),
        landing_row(
            "admin_note_metadata_recorded",
            {
                "admin_surface": "admin_console",
                "module_key": "applications",
                "screen_key": "admin.applications",
                "note_surface": "application_review",
                "note_type": "staff_decision_reason",
                "note_action": "created",
                "note_length_bucket": "21_100",
                "sensitivity_class": "p2",
                "redaction_class": "metadata_only",
                "has_attachment": False,
                "lifecycle_state": "active",
                "lifecycle_bucket": "same_day",
                "target_type": "application",
                "target_hash": "sha256:application_hash",
            },
        ),
    ]


def landing_row(event_name: str, payload: dict[str, object]) -> dict[str, object]:
    return {
        "event_name": event_name,
        "subject": {"user_hash": "sha256:user_hash", "session_id": "session-a"},
        "payload": payload,
    }


if __name__ == "__main__":
    unittest.main()
