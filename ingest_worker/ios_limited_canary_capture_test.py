from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from ingest_worker import ios_limited_canary_capture as capture


class IOSLimitedCanaryCaptureTest(unittest.TestCase):
    def test_preflight_blocks_when_access_inputs_are_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, capture.EXIT_BLOCKED)
        self.assertEqual(evidence["classification"], "blocked-access-gate")
        self.assertFalse(evidence["preflight"]["passed"])
        self.assertIn(
            "EMSI_DP_CANARY_APPROVAL_ID must equal "
            "EMSI-DP-P1A-IOS-CANARY-20260620-AR01",
            evidence["preflight"]["errors"],
        )

    def test_preflight_rejects_local_warehouse_dsn(self) -> None:
        env = valid_preflight_env()
        env["EMSI_DP_CANARY_WAREHOUSE_DSN"] = (
            "postgres://emsi:emsi@localhost:5432/analytics?sslmode=disable"
        )
        with patch.dict(os.environ, env, clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, capture.EXIT_BLOCKED)
        self.assertIn(
            "EMSI_DP_CANARY_WAREHOUSE_DSN must not point at local/dev",
            evidence["preflight"]["errors"],
        )

    def test_preflight_passes_with_required_production_inputs(self) -> None:
        with patch.dict(os.environ, valid_preflight_env(), clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=True)

        self.assertEqual(exit_code, 0)
        self.assertEqual(evidence["classification"], "preflight-ready")
        self.assertTrue(evidence["preflight"]["passed"])
        self.assertEqual(evidence["landing_count"], None)

    def test_capture_requires_explicit_production_opt_in_and_check_results(self) -> None:
        with patch.dict(os.environ, valid_preflight_env(), clear=True):
            exit_code, evidence = capture.collect_evidence(preflight_only=False)

        self.assertEqual(exit_code, capture.EXIT_BLOCKED)
        self.assertIn(
            "EMSI_DP_CANARY_ALLOW_PRODUCTION_CAPTURE=true is required for capture",
            evidence["preflight"]["errors"],
        )
        self.assertIn(
            "dbt status must be one of blocked, failed, passed, skipped",
            evidence["preflight"]["errors"],
        )
        self.assertIn(
            "Soda status must be one of blocked, failed, passed, skipped",
            evidence["preflight"]["errors"],
        )
        self.assertIn(
            "Dagster status must be one of blocked, failed, passed, skipped",
            evidence["preflight"]["errors"],
        )


def valid_preflight_env() -> dict[str, str]:
    return {
        "EMSI_DP_CANARY_APPROVAL_ID": capture.APPROVAL_ID,
        "EMSI_DP_CANARY_TARGET_NAME": "prod-ios-canary-a",
        "EMSI_DP_CANARY_TARGET_CLASS": "production",
        "EMSI_DP_CANARY_SEEDED_USER_REF": "seeded-ios-canary-001",
        "EMSI_DP_CANARY_SUBJECT_USER_HASH": "a" * 64,
        "EMSI_DP_CANARY_EVENT_ID_PREFIX": "ios-prod-canary-20260620-ar01",
        "EMSI_DP_CANARY_WINDOW_START": "2026-06-20T14:00:00Z",
        "EMSI_DP_CANARY_WINDOW_END": "2026-06-20T14:02:00Z",
        "EMSI_DP_CANARY_WAREHOUSE_DSN": (
            "postgres://analytics_reader:redacted@warehouse.example.com:5432/"
            "analytics?sslmode=verify-full"
        ),
        "EMSI_DP_CANARY_SHARE_ANALYTICS": "true",
        "EMSI_DP_CANARY_PERSONALIZATION_ENABLED": "true",
        "EMSI_DP_CANARY_PRIVACY_ARTIFACT": "privacy-pref-check:2026-06-20:ar01",
    }


if __name__ == "__main__":
    unittest.main()
