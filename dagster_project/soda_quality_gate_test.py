from __future__ import annotations

import unittest

try:
    from soda_quality_gate import (
        PRODUCT_REPORTING_SODA_CONTRACT_NAMES,
        PRODUCT_REPORTING_SODA_EXPECTED_CHECK_COUNTS,
        PRODUCT_REPORTING_SODA_EXPECTED_TOTAL_CHECK_COUNT,
        assert_soda_contract_gate,
        observed_soda_check_count,
        validate_product_reporting_soda_contract_names,
    )
except ModuleNotFoundError:
    from dagster_project.soda_quality_gate import (
        PRODUCT_REPORTING_SODA_CONTRACT_NAMES,
        PRODUCT_REPORTING_SODA_EXPECTED_CHECK_COUNTS,
        PRODUCT_REPORTING_SODA_EXPECTED_TOTAL_CHECK_COUNT,
        assert_soda_contract_gate,
        observed_soda_check_count,
        validate_product_reporting_soda_contract_names,
    )


class SodaQualityGateTest(unittest.TestCase):
    def test_expected_product_reporting_contract_inventory(self) -> None:
        self.assertEqual(PRODUCT_REPORTING_SODA_EXPECTED_TOTAL_CHECK_COUNT, 50)
        self.assertEqual(
            tuple(PRODUCT_REPORTING_SODA_EXPECTED_CHECK_COUNTS),
            PRODUCT_REPORTING_SODA_CONTRACT_NAMES,
        )
        validate_product_reporting_soda_contract_names(PRODUCT_REPORTING_SODA_CONTRACT_NAMES)

    def test_accepts_slash_summary_when_count_matches(self) -> None:
        result = assert_soda_contract_gate(
            contract_name="product_reporting_content_performance_daily.yml",
            output="Soda contract verification\n8/8 checks PASSED",
            expected_check_count=8,
        )

        self.assertEqual(result.observed_check_count, 8)

    def test_accepts_evaluated_summary_when_count_matches(self) -> None:
        self.assertEqual(observed_soda_check_count("checks evaluated: 6\n0 checks NOT EVALUATED"), 6)

    def test_fails_when_not_evaluated_check_is_reported(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "NOT EVALUATED"):
            assert_soda_contract_gate(
                contract_name="product_reporting_emoji_reaction_daily.yml",
                output="6 checks evaluated\nNOT EVALUATED reaction_key_not_null",
                expected_check_count=6,
            )

    def test_fails_when_check_count_is_missing(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "did not expose a check count"):
            assert_soda_contract_gate(
                contract_name="product_reporting_reaction_valence_daily.yml",
                output="Soda contract verification finished",
                expected_check_count=6,
            )

    def test_fails_when_check_count_does_not_match_contract(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "expected 8 Soda checks, observed 7"):
            assert_soda_contract_gate(
                contract_name="product_reporting_feed_interest_proxy_daily.yml",
                output="7/7 checks PASSED",
                expected_check_count=8,
            )

    def test_fails_when_contract_inventory_does_not_match_expected_counts(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "missing=.*product_reporting_contract_coverage"):
            validate_product_reporting_soda_contract_names(
                PRODUCT_REPORTING_SODA_CONTRACT_NAMES[:-1]
            )


if __name__ == "__main__":
    unittest.main()
