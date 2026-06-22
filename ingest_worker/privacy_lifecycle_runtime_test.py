from __future__ import annotations

import copy
import json
import unittest

from ingest_worker import privacy_lifecycle_runtime as runtime


class PrivacyLifecycleRuntimeTest(unittest.TestCase):
    def test_default_source_bound_packet_builds_runtime_report(self) -> None:
        report = runtime.build_runtime_report_from_path()

        self.assertEqual(
            report["classification"],
            "local_source_bound_privacy_lifecycle_runtime",
        )
        self.assertEqual(report["source_binding"]["target_class"], "local-source-bound")
        self.assertFalse(report["scope_guards"]["api_exposure_enabled"])
        self.assertFalse(report["scope_guards"]["dashboard_exposure_enabled"])
        self.assertFalse(report["scope_guards"]["clickhouse_production_promotion"])
        self.assertEqual(report["canonical_boundaries"]["warehouse"], "PostgreSQL")
        self.assertEqual(report["canonical_boundaries"]["event_backbone"], "Redpanda/Kafka API")
        self.assertIn("candidate-only", report["canonical_boundaries"]["hot_analytics"])
        self.assertGreaterEqual(len(report["retention_candidates"]), 3)
        self.assertTrue(any(check["passed"] for check in report["anonymous_candidate_checks"]))
        self.assertTrue(
            any(not check["passed"] for check in report["anonymous_candidate_checks"])
        )
        self.assertEqual(
            {
                "analytics_opt_out",
                "account_deletion",
                "recap_opt_out",
                "feed_profile_reset",
            },
            {action["request_type"] for action in report["cleanup_actions"]},
        )

    def test_source_binding_rejects_production_and_exposure_flags(self) -> None:
        packet = default_packet()
        packet["sourceBinding"]["targetClass"] = "production"
        packet["sourceBinding"]["productionCollectionEnabled"] = True
        packet["sourceBinding"]["apiExposureEnabled"] = True

        with self.assertRaises(runtime.PrivacyLifecycleRuntimeError) as err:
            runtime.build_runtime_report(packet)

        self.assertIn("targetClass must be local-source-bound", str(err.exception))

    def test_source_binding_rejects_qa_and_legacy_report_sources(self) -> None:
        packet = default_packet()
        packet["sourceBinding"]["sourceSystems"].append("emsi_qa.visual_evidence")

        with self.assertRaises(runtime.PrivacyLifecycleRuntimeError) as err:
            runtime.build_runtime_report(packet)

        self.assertIn("QA evidence or legacy report sources", str(err.exception))

    def test_packet_rejects_forbidden_payload_keys(self) -> None:
        packet = default_packet()
        packet["records"][0]["raw_user_id"] = "sha256:not-allowed"

        with self.assertRaises(runtime.PrivacyLifecycleRuntimeError) as err:
            runtime.build_runtime_report(packet)

        self.assertIn("forbidden payload key", str(err.exception))


def default_packet() -> dict[str, object]:
    return copy.deepcopy(json.loads(runtime.DEFAULT_PACKET_PATH.read_text(encoding="utf-8")))


if __name__ == "__main__":
    unittest.main()
