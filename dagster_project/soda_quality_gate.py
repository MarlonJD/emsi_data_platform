from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Mapping, Sequence


PRODUCT_REPORTING_SODA_EXPECTED_CHECK_COUNTS: dict[str, int] = {
    "product_reporting_occupation_cohort_daily.yml": 8,
    "product_reporting_content_performance_daily.yml": 8,
    "product_reporting_emoji_reaction_daily.yml": 6,
    "product_reporting_reaction_valence_daily.yml": 6,
    "product_reporting_feed_interest_proxy_daily.yml": 8,
    "product_reporting_together_coordination_daily.yml": 8,
    "product_reporting_contract_coverage.yml": 6,
}
PRODUCT_REPORTING_SODA_CONTRACT_NAMES = tuple(PRODUCT_REPORTING_SODA_EXPECTED_CHECK_COUNTS)
PRODUCT_REPORTING_SODA_EXPECTED_TOTAL_CHECK_COUNT = sum(
    PRODUCT_REPORTING_SODA_EXPECTED_CHECK_COUNTS.values()
)

_SLASH_CHECK_COUNT_RE = re.compile(r"\b(\d+)\s*/\s*(\d+)\s+checks?\b", re.IGNORECASE)
_TOTAL_CHECK_COUNT_RE = re.compile(r"\btotal\s+checks?\s*[:=]\s*(\d+)\b", re.IGNORECASE)
_EVALUATED_CHECK_COUNT_RE = re.compile(
    r"\bchecks?\s+(?:evaluated|executed|run)\s*[:=]?\s*(\d+)\b",
    re.IGNORECASE,
)
_POSTFIX_EVALUATED_CHECK_COUNT_RE = re.compile(
    r"\b(\d+)\s+checks?\s+(?:evaluated|executed|run)\b",
    re.IGNORECASE,
)
_STATUS_CHECK_COUNT_RE = re.compile(
    r"\b(\d+)\s+checks?\s+(?:passed|failed|warned|not[\s_-]+evaluated)\b",
    re.IGNORECASE,
)
_CHECK_STATUS_LINE_RE = re.compile(
    r"^\s*(?:[-*]\s*)?(?:PASS(?:ED)?|FAIL(?:ED)?|WARN(?:ING)?|NOT[\s_-]+EVALUATED)\b",
    re.IGNORECASE,
)
_NOT_EVALUATED_RE = re.compile(r"\bNOT[\s_-]+EVALUATED\b", re.IGNORECASE)
_ZERO_NOT_EVALUATED_RE = re.compile(
    r"\b0\s+checks?\s+NOT[\s_-]+EVALUATED\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SodaContractGateResult:
    contract_name: str
    expected_check_count: int
    observed_check_count: int


def validate_product_reporting_soda_contract_names(contract_names: Sequence[str]) -> None:
    expected_names = set(PRODUCT_REPORTING_SODA_EXPECTED_CHECK_COUNTS)
    declared_names = set(contract_names)
    missing = sorted(expected_names - declared_names)
    unexpected = sorted(declared_names - expected_names)
    if missing or unexpected:
        raise RuntimeError(
            "Product Reporting Soda contract inventory does not match expected check counts: "
            f"missing={','.join(missing) or 'none'} unexpected={','.join(unexpected) or 'none'}"
        )


def assert_soda_contract_gate(
    *,
    contract_name: str,
    output: str,
    expected_check_count: int,
) -> SodaContractGateResult:
    if expected_check_count <= 0:
        raise RuntimeError(f"{contract_name} has no positive expected Soda check count")

    not_evaluated_markers = soda_not_evaluated_markers(output)
    if not_evaluated_markers:
        first_marker = not_evaluated_markers[0]
        raise RuntimeError(f"{contract_name} has NOT EVALUATED Soda checks: {first_marker}")

    observed_check_count = observed_soda_check_count(output)
    if observed_check_count is None:
        raise RuntimeError(f"{contract_name} Soda output did not expose a check count")
    if observed_check_count != expected_check_count:
        raise RuntimeError(
            f"{contract_name} expected {expected_check_count} Soda checks, "
            f"observed {observed_check_count}"
        )

    return SodaContractGateResult(
        contract_name=contract_name,
        expected_check_count=expected_check_count,
        observed_check_count=observed_check_count,
    )


def observed_soda_check_count(output: str) -> int | None:
    text = output or ""

    slash_matches = list(_SLASH_CHECK_COUNT_RE.finditer(text))
    if slash_matches:
        return max(int(match.group(2)) for match in slash_matches)

    for pattern in (
        _TOTAL_CHECK_COUNT_RE,
        _EVALUATED_CHECK_COUNT_RE,
        _POSTFIX_EVALUATED_CHECK_COUNT_RE,
    ):
        matches = list(pattern.finditer(text))
        if matches:
            return max(int(match.group(1)) for match in matches)

    status_counts = [int(match.group(1)) for match in _STATUS_CHECK_COUNT_RE.finditer(text)]
    if status_counts:
        return sum(status_counts)

    status_lines = [
        line for line in text.splitlines() if _CHECK_STATUS_LINE_RE.search(line)
    ]
    if status_lines:
        return len(status_lines)

    return None


def soda_not_evaluated_markers(output: str) -> list[str]:
    markers = []
    for line in (output or "").splitlines():
        if not _NOT_EVALUATED_RE.search(line):
            continue
        if _ZERO_NOT_EVALUATED_RE.search(line):
            continue
        markers.append(line.strip())
    return markers


def format_soda_check_counts(check_counts: Mapping[str, int]) -> str:
    return ",".join(
        f"{contract_name}={check_count}" for contract_name, check_count in check_counts.items()
    )
