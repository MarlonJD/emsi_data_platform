from __future__ import annotations

import sys
import types
import unittest

if "psycopg2" not in sys.modules:
    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.Error = Exception
    sys.modules["psycopg2"] = psycopg2
    psycopg2_extras = types.ModuleType("psycopg2.extras")
    psycopg2_extras.Json = lambda value: value
    sys.modules["psycopg2.extras"] = psycopg2_extras

from ingest_worker import clickhouse_candidate_smoke


class ClickHousePromotionGateReportTest(unittest.TestCase):
    def test_missing_manifest_blocks_production_promotion(self) -> None:
        report = clickhouse_candidate_smoke.build_promotion_gate_report(
            database="emsi_hot_analytics",
            row_count=3,
            aggregate_event_count=3,
            postgres_aggregate_ms=10.1,
            clickhouse_aggregate_ms=4.2,
            parity_matched=True,
            promotion_manifest={},
        )

        self.assertFalse(report["productionPromotionReady"])
        self.assertEqual(report["productionPromotionStatus"], "blocked")
        self.assertEqual(report["canonicalWarehouse"], "postgresql")
        self.assertFalse(report["clickhouseCanonical"])
        self.assertFalse(report["clickhouseProductionEnabled"])
        self.assertTrue(report["privacyGuardrails"]["boundedColumnsOnly"])
        self.assertFalse(report["privacyGuardrails"]["rawPayloadCopied"])
        self.assertIn("analytics", report["missingOwnerApprovals"])

    def test_complete_manifest_keeps_clickhouse_noncanonical(self) -> None:
        manifest = {
            "schemaVersion": clickhouse_candidate_smoke.PROMOTION_GATE_SCHEMA_VERSION,
            "measuredNeedEvidenceId": "EMSI-CH-NEED-20260620",
            "parityEvidenceId": "EMSI-CH-PARITY-20260620",
            "rebuildFromCanonicalEvidenceId": "EMSI-CH-REBUILD-20260620",
            "retentionPolicyEvidenceId": "EMSI-CH-RETENTION-20260620",
            "backupRestoreEvidenceId": "EMSI-CH-RESTORE-20260620",
            "monitoringEvidenceId": "EMSI-CH-MONITORING-20260620",
            "vulnerabilityScanEvidenceId": "EMSI-CH-VULN-20260620",
            "provenanceEvidenceId": "EMSI-CH-PROVENANCE-20260620",
            "clickhouseCanonical": False,
            "clickhouseProductionEnabled": False,
            "ownerApprovals": {
                "analytics": "EMSI-CH-APPROVAL-ANALYTICS-20260620",
                "sre": "EMSI-CH-APPROVAL-SRE-20260620",
                "privacySecurity": "EMSI-CH-APPROVAL-PRIVSEC-20260620",
            },
        }

        report = clickhouse_candidate_smoke.build_promotion_gate_report(
            database="emsi_hot_analytics",
            row_count=3,
            aggregate_event_count=3,
            postgres_aggregate_ms=10.1,
            clickhouse_aggregate_ms=4.2,
            parity_matched=True,
            promotion_manifest=manifest,
        )

        self.assertTrue(report["productionPromotionReady"])
        self.assertEqual(report["productionPromotionStatus"], "ready-for-owner-approved-hot-analytics")
        self.assertFalse(report["clickhouseCanonical"])
        self.assertFalse(report["clickhouseProductionEnabled"])
        self.assertEqual(report["missingEvidence"], [])
        self.assertEqual(report["missingOwnerApprovals"], [])
        self.assertEqual(report["guardrailFailures"], [])

    def test_manifest_cannot_mark_clickhouse_canonical(self) -> None:
        manifest = complete_manifest()
        manifest["clickhouseCanonical"] = True

        report = clickhouse_candidate_smoke.build_promotion_gate_report(
            database="emsi_hot_analytics",
            row_count=3,
            aggregate_event_count=3,
            postgres_aggregate_ms=10.1,
            clickhouse_aggregate_ms=4.2,
            parity_matched=True,
            promotion_manifest=manifest,
        )

        self.assertFalse(report["productionPromotionReady"])
        self.assertIn("manifest_attempts_clickhouse_canonical", report["guardrailFailures"])
        self.assertFalse(report["clickhouseCanonical"])

    def test_manifest_must_explicitly_keep_clickhouse_noncanonical(self) -> None:
        manifest = complete_manifest()
        del manifest["clickhouseCanonical"]
        del manifest["clickhouseProductionEnabled"]

        report = clickhouse_candidate_smoke.build_promotion_gate_report(
            database="emsi_hot_analytics",
            row_count=3,
            aggregate_event_count=3,
            postgres_aggregate_ms=10.1,
            clickhouse_aggregate_ms=4.2,
            parity_matched=True,
            promotion_manifest=manifest,
        )

        self.assertFalse(report["productionPromotionReady"])
        self.assertIn("manifest_missing_clickhouse_noncanonical", report["guardrailFailures"])
        self.assertIn("manifest_missing_clickhouse_production_disabled", report["guardrailFailures"])
        self.assertFalse(report["clickhouseCanonical"])
        self.assertFalse(report["clickhouseProductionEnabled"])

    def test_manifest_evidence_ids_must_be_bounded_tokens(self) -> None:
        manifest = complete_manifest()
        manifest["measuredNeedEvidenceId"] = "private approval note with email user@example.com"
        manifest["ownerApprovals"]["analytics"] = "approved by jane@example.com"

        report = clickhouse_candidate_smoke.build_promotion_gate_report(
            database="emsi_hot_analytics",
            row_count=3,
            aggregate_event_count=3,
            postgres_aggregate_ms=10.1,
            clickhouse_aggregate_ms=4.2,
            parity_matched=True,
            promotion_manifest=manifest,
        )

        self.assertFalse(report["productionPromotionReady"])
        self.assertIn("unsafe_manifest_evidence_id:measuredNeedEvidenceId", report["guardrailFailures"])
        self.assertIn("unsafe_manifest_owner_approval:analytics", report["guardrailFailures"])
        self.assertEqual(report["manifestEvidence"]["evidenceIds"]["measuredNeedEvidenceId"], "[unsafe-redacted]")
        self.assertEqual(report["manifestEvidence"]["ownerApprovals"]["analytics"], "[unsafe-redacted]")


def complete_manifest() -> dict[str, object]:
    return {
        "schemaVersion": clickhouse_candidate_smoke.PROMOTION_GATE_SCHEMA_VERSION,
        "measuredNeedEvidenceId": "EMSI-CH-NEED-20260620",
        "parityEvidenceId": "EMSI-CH-PARITY-20260620",
        "rebuildFromCanonicalEvidenceId": "EMSI-CH-REBUILD-20260620",
        "retentionPolicyEvidenceId": "EMSI-CH-RETENTION-20260620",
        "backupRestoreEvidenceId": "EMSI-CH-RESTORE-20260620",
        "monitoringEvidenceId": "EMSI-CH-MONITORING-20260620",
        "vulnerabilityScanEvidenceId": "EMSI-CH-VULN-20260620",
        "provenanceEvidenceId": "EMSI-CH-PROVENANCE-20260620",
        "clickhouseCanonical": False,
        "clickhouseProductionEnabled": False,
        "ownerApprovals": {
            "analytics": "EMSI-CH-APPROVAL-ANALYTICS-20260620",
            "sre": "EMSI-CH-APPROVAL-SRE-20260620",
            "privacySecurity": "EMSI-CH-APPROVAL-PRIVSEC-20260620",
        },
    }


if __name__ == "__main__":
    unittest.main()
