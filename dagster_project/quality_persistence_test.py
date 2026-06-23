from __future__ import annotations

import unittest
from datetime import datetime, timezone

try:
    from quality_persistence import (
        QUALITY_FINDINGS_DDL,
        QUALITY_RUNS_DDL,
        UPSERT_QUALITY_FINDING_SQL,
        UPSERT_QUALITY_RUN_SQL,
        build_quality_run_result,
        persist_quality_run_result,
        redacted_text,
    )
except ModuleNotFoundError:
    from dagster_project.quality_persistence import (
        QUALITY_FINDINGS_DDL,
        QUALITY_RUNS_DDL,
        UPSERT_QUALITY_FINDING_SQL,
        UPSERT_QUALITY_RUN_SQL,
        build_quality_run_result,
        persist_quality_run_result,
        redacted_text,
    )


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: dict[str, object] | None = None) -> None:
        self.connection.executed.append((sql, params))


class FakeConnection:
    def __init__(self) -> None:
        self.executed: list[tuple[str, dict[str, object] | None]] = []

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)


class QualityPersistenceTest(unittest.TestCase):
    def test_passed_run_has_no_findings_and_redacts_output_summary(self) -> None:
        now = datetime(2026, 6, 23, 9, 0, tzinfo=timezone.utc)
        result = build_quality_run_result(
            contract_name="product_reporting_content_performance_daily.yml",
            dataset_name="analytics_postgres/analytics/analytics_mart/mart_product_reporting_content_performance_daily",
            pipeline_run_id="dagster-run-1",
            status="passed",
            started_at=now,
            finished_at=now,
            expected_check_count=8,
            observed_check_count=8,
            output="8/8 checks PASSED token secret should not persist",
        )

        self.assertEqual(result.critical_count, 0)
        self.assertEqual(result.findings, ())
        self.assertIn("[redacted]", result.raw_result_json["outputExcerpt"])
        self.assertNotIn("token", result.raw_result_json["outputExcerpt"].lower())

    def test_failed_run_creates_critical_open_finding(self) -> None:
        now = datetime(2026, 6, 23, 9, 1, tzinfo=timezone.utc)
        result = build_quality_run_result(
            contract_name="product_reporting_emoji_reaction_daily.yml",
            dataset_name="analytics_postgres/analytics/analytics_mart/mart_product_reporting_emoji_reaction_daily",
            pipeline_run_id="dagster-run-2",
            status="failed",
            started_at=now,
            finished_at=now,
            expected_check_count=6,
            observed_check_count=5,
            output="5/6 checks PASSED",
            error_message="missing required check",
        )

        self.assertEqual(result.critical_count, 1)
        self.assertEqual(len(result.findings), 1)
        self.assertEqual(result.findings[0].severity, "critical")
        self.assertEqual(result.findings[0].status, "open")
        self.assertEqual(result.findings[0].sample_count, 5)

    def test_persist_writes_schema_run_and_finding_statements(self) -> None:
        now = datetime(2026, 6, 23, 9, 2, tzinfo=timezone.utc)
        connection = FakeConnection()
        result = build_quality_run_result(
            contract_name="product_reporting_emoji_reaction_daily.yml",
            dataset_name="analytics_postgres/analytics/analytics_mart/mart_product_reporting_emoji_reaction_daily",
            pipeline_run_id="dagster-run-3",
            status="failed",
            started_at=now,
            finished_at=now,
            expected_check_count=6,
            observed_check_count=None,
            output="",
            error_message="Soda output did not expose a check count",
        )

        persist_quality_run_result(result, connect=lambda: connection)

        executed_sql = [sql for sql, _params in connection.executed]
        self.assertEqual(executed_sql[0], QUALITY_RUNS_DDL)
        self.assertEqual(executed_sql[1], QUALITY_FINDINGS_DDL)
        self.assertEqual(executed_sql[2], UPSERT_QUALITY_RUN_SQL)
        self.assertEqual(executed_sql[3], UPSERT_QUALITY_FINDING_SQL)

    def test_redacted_text_caps_and_removes_sensitive_terms(self) -> None:
        redacted = redacted_text("email token exact_gps " + ("x" * 20), limit=18)

        self.assertNotIn("email", redacted.lower())
        self.assertNotIn("token", redacted.lower())
        self.assertLessEqual(len(redacted), 18)

    def test_redacted_text_masks_sensitive_values_without_key_terms(self) -> None:
        redacted = redacted_text(
            "failed value marlon@example.com +90 555 111 22 33 "
            "Bearer abcdefghijklmnopqrstuvwxyz latitude=41.0123 longitude=29.1234 "
            "eyJaaaaaaaaaaa.bbbbbbbbbbbb.cccccccccccc"
        )

        self.assertNotIn("marlon@example.com", redacted)
        self.assertNotIn("+90 555", redacted)
        self.assertNotIn("abcdefghijklmnopqrstuvwxyz", redacted)
        self.assertNotIn("41.0123", redacted)
        self.assertNotIn("29.1234", redacted)
        self.assertNotIn("eyJaaaaaaaaaaa", redacted)


if __name__ == "__main__":
    unittest.main()
