from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

from dagster import AssetSelection, Definitions, asset, define_asset_job


WORKSPACE_DIR = Path(os.getenv("DATA_PLATFORM_WORKSPACE", "/workspace"))
DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", WORKSPACE_DIR / "dbt"))
SODA_CONFIG_PATH = Path(os.getenv("SODA_CONFIG_PATH", WORKSPACE_DIR / "soda" / "configuration.yml"))
SODA_CONTRACT_PATH = Path(
    os.getenv("SODA_CONTRACT_PATH", WORKSPACE_DIR / "soda" / "contracts" / "raw_event_landing.yml")
)

BLOCKED_IDENTIFIER_PATTERN = (
    r'("email"|"emailAddress"|"phone"|"phoneNumber"|"authorization"|"token"|'
    r'"auth_token"|"access_token"|"refresh_token"|"session_token"|"api_key"|'
    r'"cookie"|"signed_url"|"full_url"|"user_id"|"userId"|"raw_user_id"|'
    r'"actor_user_id"|"actorUserId"|"staff_id"|"target_user_id"|'
    r'"body"|"message"|"note"|"note_body"|"raw_note"|"raw_note_text"|'
    r'"internal_note"|"private_note"|"applicant_message"|"feedback_message"|'
    r'"search_text"|"raw_search_text"|"raw_text"|"raw_content"|"post_body"|'
    r'"comment_body"|"reply_body"|"dm_content"|"raw_policy_text"|'
    r'"request_body"|"response_body"|"transcript"|"screenshot"|"view_hierarchy"|'
    r'"support_payload"|"support_contact"|"contact_payload"|"reveal_payload"|'
    r'"reveal_value"|"contact_value"|"payload_value"|"exact_gps"|"gps"|'
    r'"latitude"|"longitude"|"lat"|"lon")'
)

PRODUCT_REPORTING_PHASE1_ASSETS = {
    "stage.stg_product_reporting_content_events": {
        "group": "product_reporting_stage",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "no_raw_content_text, stable_content_key, bounded_occupation_cohort, source_lineage_present",
    },
    "stage.stg_product_reporting_reactions": {
        "group": "product_reporting_stage",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "valid_reaction_action, stable_reaction_key, bounded_emoji_key, reaction_valence_present",
    },
    "stage.stg_product_reporting_feed_events": {
        "group": "product_reporting_stage",
        "cadence": "operational_ingest_15m_job",
        "freshness": "<= 15 minutes",
        "checks": "accepted_feed_event_names_only, source_completeness_label_present, interest_proxy_valence_present",
    },
    "raw_vault.hub_reporting_content": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "hub_hash_key_not_null, hub_hash_key_unique",
    },
    "raw_vault.hub_reporting_reaction": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "hub_hash_key_not_null, hub_hash_key_unique",
    },
    "raw_vault.hub_reporting_feed_item": {
        "group": "product_reporting_raw_vault",
        "cadence": "operational_ingest_15m_job",
        "freshness": "<= 15 minutes",
        "checks": "hub_hash_key_not_null, hub_hash_key_unique",
    },
    "raw_vault.link_reporting_reaction_content": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "link_hash_key_not_null, linked_hubs_present",
    },
    "raw_vault.link_reporting_feed_item_content": {
        "group": "product_reporting_raw_vault",
        "cadence": "operational_ingest_15m_job",
        "freshness": "<= 15 minutes",
        "checks": "link_hash_key_not_null, linked_hubs_present",
    },
    "raw_vault.sat_reporting_content_event": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "hashdiff_present, forbidden_raw_fields_absent",
    },
    "raw_vault.sat_reporting_reaction_event": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "hashdiff_present, forbidden_raw_fields_absent",
    },
    "raw_vault.sat_reporting_feed_serving_event": {
        "group": "product_reporting_raw_vault",
        "cadence": "operational_ingest_15m_job",
        "freshness": "<= 15 minutes",
        "checks": "hashdiff_present, no_raw_model_score, forbidden_raw_fields_absent",
    },
    "raw_vault.product_reporting_stage_reconciliation": {
        "group": "product_reporting_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "source_to_stage_counts_present, missing_business_key_count_visible",
    },
}

PRODUCT_REPORTING_PHASE2_ASSETS = {
    "business_vault.pit_reporting_content_daily": {
        "group": "product_reporting_business_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "content_key_present, daily_pit_grain, suppression_status_present",
    },
    "business_vault.br_content_reaction_daily": {
        "group": "product_reporting_business_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "reaction_bridge_grain, bounded_emoji_key, suppression_status_present",
    },
    "business_vault.br_feed_interest_proxy": {
        "group": "product_reporting_business_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "proxy_wording_present, source_completeness_label_present, suppression_status_present",
    },
    "business_vault.s_occupation_cohort_daily": {
        "group": "product_reporting_business_vault",
        "cadence": "daily_business_reporting_job",
        "freshness": "<= 24 hours",
        "checks": "unknown_bucket_present, small_cell_suppression_present, canonical_app_source_gap_labeled",
    },
    "business_vault.s_content_performance_daily": {
        "group": "product_reporting_business_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "metric_contract_ids_present, no_raw_text_columns, suppression_status_present",
    },
    "business_vault.s_emoji_usage_daily": {
        "group": "product_reporting_business_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "bounded_emoji_key, net_usage_formula_present, suppression_status_present",
    },
    "business_vault.s_reaction_valence_daily": {
        "group": "product_reporting_business_vault",
        "cadence": "daily_business_reporting_job",
        "freshness": "<= 24 hours",
        "checks": "explicit_signal_wording, valence_bucket_present, suppression_status_present",
    },
    "business_vault.product_reporting_bdv_contract_coverage": {
        "group": "product_reporting_business_vault_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "phase2_metric_contracts_covered, wording_status_present, suppression_rule_present",
    },
}

PRODUCT_REPORTING_PHASE3_ASSETS = {
    "mart.mart_product_reporting_occupation_cohort_daily": {
        "group": "product_reporting_mart",
        "cadence": "daily_business_reporting_job",
        "freshness": "<= 24 hours",
        "checks": "suppressed_rows_absent, metric_contract_ids_present, reporting_timezone_present",
    },
    "mart.mart_product_reporting_content_performance_daily": {
        "group": "product_reporting_mart",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "suppressed_rows_absent, no_raw_text_columns, most_liked_rank_score_present",
    },
    "mart.mart_product_reporting_emoji_reaction_daily": {
        "group": "product_reporting_mart",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "suppressed_rows_absent, bounded_emoji_key, direct_wording_status",
    },
    "mart.mart_product_reporting_reaction_valence_daily": {
        "group": "product_reporting_mart",
        "cadence": "daily_business_reporting_job",
        "freshness": "<= 24 hours",
        "checks": "suppressed_rows_absent, explicit_signal_wording, no_mood_claim",
    },
    "mart.mart_product_reporting_feed_interest_proxy_daily": {
        "group": "product_reporting_mart",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "suppressed_rows_absent, proxy_wording_status, source_completeness_label_present",
    },
    "mart.mart_product_reporting_contract_coverage": {
        "group": "product_reporting_mart_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "pl_metric_contracts_covered, wording_status_present, suppression_rule_present",
    },
}

PRODUCT_REPORTING_PHASE5_ASSETS = {
    "quality.product_reporting_stage_reconciliation_invariants": {
        "group": "product_reporting_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "row_count_non_negative, distinct_events_not_above_rows, missing_business_keys_bounded",
        "dbt_test": "product_reporting_stage_reconciliation_invariants",
    },
    "quality.product_reporting_bdv_formula_invariants": {
        "group": "product_reporting_business_vault_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "emoji_usage_formula, reaction_valence_formula, occupation_share_bounds",
        "dbt_test": "product_reporting_bdv_formula_invariants",
    },
    "quality.product_reporting_mart_no_duplicate_daily_keys": {
        "group": "product_reporting_mart_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "one_row_per_declared_mart_grain",
        "dbt_test": "product_reporting_mart_no_duplicate_daily_keys",
    },
    "quality.product_reporting_mart_contract_metadata_present": {
        "group": "product_reporting_mart_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "source_completeness_label_present, metric_contract_ids_present, wording_status_present, reporting_timezone_present",
        "dbt_test": "product_reporting_mart_contract_metadata_present",
    },
    "quality.product_reporting_mart_expected_contract_ids_present": {
        "group": "product_reporting_mart_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "phase0_metric_contract_ids_covered, pl_contract_status_present",
        "dbt_test": "product_reporting_mart_expected_contract_ids_present",
    },
    "quality.product_reporting_forbidden_output_columns_absent": {
        "group": "product_reporting_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "raw_content_text_absent, raw_notes_absent, contact_reveal_values_absent, tokens_absent, exact_gps_absent",
        "dbt_test": "product_reporting_forbidden_output_columns_absent",
    },
}

PRODUCT_REPORTING_ASSETS = {
    **PRODUCT_REPORTING_PHASE1_ASSETS,
    **PRODUCT_REPORTING_PHASE2_ASSETS,
    **PRODUCT_REPORTING_PHASE3_ASSETS,
    **PRODUCT_REPORTING_PHASE5_ASSETS,
}


@asset(group_name="platform_baseline")
def platform_baseline_decisions() -> dict[str, str]:
    return {
        "postgres": "baseline: postgres:18.4-alpine3.24",
        "python": "baseline: python:3.12.13-slim-bookworm",
        "dbt": "baseline: dbt-core==1.11.11 and dbt-postgres==1.10.1",
        "data_vault": "baseline: ScalefreeCOM/datavault4dbt==1.18.3",
        "soda": "local-dev: soda-core==4.14.0 and soda-postgres==4.14.0",
    }


def product_reporting_asset_contract(context: Any, asset_key: str) -> dict[str, str]:
    contract = PRODUCT_REPORTING_ASSETS[asset_key]
    metadata = {"asset_key": asset_key, **contract}
    context.add_output_metadata(metadata)
    return metadata


@asset(name="stg_product_reporting_content_events", key_prefix=["stage"], group_name="product_reporting_stage")
def stg_product_reporting_content_events_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "stage.stg_product_reporting_content_events")


@asset(name="stg_product_reporting_reactions", key_prefix=["stage"], group_name="product_reporting_stage")
def stg_product_reporting_reactions_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "stage.stg_product_reporting_reactions")


@asset(name="stg_product_reporting_feed_events", key_prefix=["stage"], group_name="product_reporting_stage")
def stg_product_reporting_feed_events_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "stage.stg_product_reporting_feed_events")


@asset(name="hub_reporting_content", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def hub_reporting_content_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.hub_reporting_content")


@asset(name="hub_reporting_reaction", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def hub_reporting_reaction_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.hub_reporting_reaction")


@asset(name="hub_reporting_feed_item", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def hub_reporting_feed_item_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.hub_reporting_feed_item")


@asset(name="link_reporting_reaction_content", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def link_reporting_reaction_content_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.link_reporting_reaction_content")


@asset(name="link_reporting_feed_item_content", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def link_reporting_feed_item_content_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.link_reporting_feed_item_content")


@asset(name="sat_reporting_content_event", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def sat_reporting_content_event_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.sat_reporting_content_event")


@asset(name="sat_reporting_reaction_event", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def sat_reporting_reaction_event_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.sat_reporting_reaction_event")


@asset(name="sat_reporting_feed_serving_event", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def sat_reporting_feed_serving_event_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.sat_reporting_feed_serving_event")


@asset(name="product_reporting_stage_reconciliation", key_prefix=["raw_vault"], group_name="product_reporting_quality")
def product_reporting_stage_reconciliation_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.product_reporting_stage_reconciliation")


@asset(name="pit_reporting_content_daily", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def pit_reporting_content_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.pit_reporting_content_daily")


@asset(name="br_content_reaction_daily", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def br_content_reaction_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.br_content_reaction_daily")


@asset(name="br_feed_interest_proxy", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def br_feed_interest_proxy_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.br_feed_interest_proxy")


@asset(name="s_occupation_cohort_daily", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def s_occupation_cohort_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.s_occupation_cohort_daily")


@asset(name="s_content_performance_daily", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def s_content_performance_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.s_content_performance_daily")


@asset(name="s_emoji_usage_daily", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def s_emoji_usage_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.s_emoji_usage_daily")


@asset(name="s_reaction_valence_daily", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def s_reaction_valence_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.s_reaction_valence_daily")


@asset(
    name="product_reporting_bdv_contract_coverage",
    key_prefix=["business_vault"],
    group_name="product_reporting_business_vault_quality",
)
def product_reporting_bdv_contract_coverage_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.product_reporting_bdv_contract_coverage")


@asset(
    name="mart_product_reporting_occupation_cohort_daily",
    key_prefix=["mart"],
    group_name="product_reporting_mart",
)
def mart_product_reporting_occupation_cohort_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "mart.mart_product_reporting_occupation_cohort_daily")


@asset(
    name="mart_product_reporting_content_performance_daily",
    key_prefix=["mart"],
    group_name="product_reporting_mart",
)
def mart_product_reporting_content_performance_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "mart.mart_product_reporting_content_performance_daily")


@asset(
    name="mart_product_reporting_emoji_reaction_daily",
    key_prefix=["mart"],
    group_name="product_reporting_mart",
)
def mart_product_reporting_emoji_reaction_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "mart.mart_product_reporting_emoji_reaction_daily")


@asset(
    name="mart_product_reporting_reaction_valence_daily",
    key_prefix=["mart"],
    group_name="product_reporting_mart",
)
def mart_product_reporting_reaction_valence_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "mart.mart_product_reporting_reaction_valence_daily")


@asset(
    name="mart_product_reporting_feed_interest_proxy_daily",
    key_prefix=["mart"],
    group_name="product_reporting_mart",
)
def mart_product_reporting_feed_interest_proxy_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "mart.mart_product_reporting_feed_interest_proxy_daily")


@asset(
    name="mart_product_reporting_contract_coverage",
    key_prefix=["mart"],
    group_name="product_reporting_mart_quality",
)
def mart_product_reporting_contract_coverage_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "mart.mart_product_reporting_contract_coverage")


@asset(
    name="product_reporting_stage_reconciliation_invariants",
    key_prefix=["quality"],
    group_name="product_reporting_quality",
)
def product_reporting_stage_reconciliation_invariants_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "quality.product_reporting_stage_reconciliation_invariants")


@asset(
    name="product_reporting_bdv_formula_invariants",
    key_prefix=["quality"],
    group_name="product_reporting_business_vault_quality",
)
def product_reporting_bdv_formula_invariants_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "quality.product_reporting_bdv_formula_invariants")


@asset(
    name="product_reporting_mart_no_duplicate_daily_keys",
    key_prefix=["quality"],
    group_name="product_reporting_mart_quality",
)
def product_reporting_mart_no_duplicate_daily_keys_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "quality.product_reporting_mart_no_duplicate_daily_keys")


@asset(
    name="product_reporting_mart_contract_metadata_present",
    key_prefix=["quality"],
    group_name="product_reporting_mart_quality",
)
def product_reporting_mart_contract_metadata_present_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "quality.product_reporting_mart_contract_metadata_present")


@asset(
    name="product_reporting_mart_expected_contract_ids_present",
    key_prefix=["quality"],
    group_name="product_reporting_mart_quality",
)
def product_reporting_mart_expected_contract_ids_present_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "quality.product_reporting_mart_expected_contract_ids_present")


@asset(
    name="product_reporting_forbidden_output_columns_absent",
    key_prefix=["quality"],
    group_name="product_reporting_quality",
)
def product_reporting_forbidden_output_columns_absent_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "quality.product_reporting_forbidden_output_columns_absent")


@asset(group_name="phase_d_local_smoke")
def analytics_raw_event_landing_smoke(context) -> dict[str, int]:
    import psycopg2

    dsn = (
        f"host={os.getenv('ANALYTICS_POSTGRES_HOST', 'analytics-postgres')} "
        f"port={os.getenv('ANALYTICS_POSTGRES_PORT', '5432')} "
        f"dbname={os.getenv('ANALYTICS_POSTGRES_DB', 'analytics')} "
        f"user={os.getenv('ANALYTICS_POSTGRES_USER', 'analytics')} "
        f"password={os.getenv('ANALYTICS_POSTGRES_PASSWORD', 'analytics_local_password')}"
    )
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  count(*)::int AS row_count,
                  (count(*) - count(DISTINCT event_id))::int AS duplicate_event_id_count,
                  count(*) FILTER (WHERE event_id IS NULL)::int AS missing_event_id_count,
                  count(*) FILTER (
                    WHERE occurred_at IS NULL
                       OR received_at IS NULL
                       OR occurred_at > received_at + interval '5 minutes'
                       OR occurred_at < timestamp with time zone '2020-01-01T00:00:00Z'
                       OR received_at > now() + interval '5 minutes'
                  )::int AS invalid_timestamp_count,
                  count(*) FILTER (
                    WHERE privacy_class NOT IN ('pseudonymous')
                  )::int AS invalid_privacy_class_count,
                  count(*) FILTER (
                    WHERE subject_user_hash LIKE '%%@%%'
                       OR subject::text ~* %s
                       OR payload::text ~* %s
                  )::int AS raw_identifier_count
                FROM analytics.raw_event_landing
                """,
                (BLOCKED_IDENTIFIER_PATTERN, BLOCKED_IDENTIFIER_PATTERN),
            )
            columns = [column[0] for column in cur.description]
            metrics = dict(zip(columns, cur.fetchone(), strict=True))

    context.add_output_metadata(metrics)
    failures = {
        key: value
        for key, value in metrics.items()
        if (key == "row_count" and value <= 0) or (key != "row_count" and value != 0)
    }
    if failures:
        raise RuntimeError(f"raw_event_landing smoke failed: {failures}")
    return metrics


@asset(group_name="phase_d_local_smoke")
def dbt_phase_d_smoke(
    context,
    analytics_raw_event_landing_smoke: dict[str, int],
) -> dict[str, str]:
    _ = analytics_raw_event_landing_smoke
    commands = [
        ["dbt", "deps", "--project-dir", str(DBT_PROJECT_DIR)],
        ["dbt", "debug", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
        [
            "dbt",
            "run",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROJECT_DIR),
            "--select",
            "tag:phase_d_smoke",
        ],
    ]
    for command in commands:
        run_command(context, command)
    return {"status": "passed", "project_dir": str(DBT_PROJECT_DIR)}


@asset(group_name="phase_d_local_smoke")
def soda_raw_event_landing_scan(
    context,
    dbt_phase_d_smoke: dict[str, str],
) -> dict[str, str]:
    _ = dbt_phase_d_smoke
    run_command(
        context,
        [
            "soda",
            "contract",
            "verify",
            "--data-source",
            str(SODA_CONFIG_PATH),
            "--contract",
            str(SODA_CONTRACT_PATH),
        ],
    )
    return {"status": "passed", "contract_path": str(SODA_CONTRACT_PATH)}


def run_command(context: Any, command: list[str]) -> None:
    context.log.info("Running local smoke command: %s", " ".join(command))
    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR))
    completed = subprocess.run(
        command,
        cwd=str(WORKSPACE_DIR),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if completed.stdout:
        context.log.info(completed.stdout)
    if completed.returncode != 0:
        raise RuntimeError(
            f"local smoke command failed with exit {completed.returncode}: {' '.join(command)}"
        )


phase_d_local_smoke_job = define_asset_job(
    name="phase_d_local_smoke_job",
    selection=AssetSelection.groups("phase_d_local_smoke"),
)

product_reporting_phase1_stage_rdv_job = define_asset_job(
    name="product_reporting_phase1_stage_rdv_job",
    selection=(
        AssetSelection.groups("product_reporting_stage")
        | AssetSelection.groups("product_reporting_raw_vault")
        | AssetSelection.groups("product_reporting_quality")
    ),
)

product_reporting_phase2_bdv_job = define_asset_job(
    name="product_reporting_phase2_bdv_job",
    selection=(
        AssetSelection.groups("product_reporting_business_vault")
        | AssetSelection.groups("product_reporting_business_vault_quality")
    ),
)

product_reporting_phase3_pl_job = define_asset_job(
    name="product_reporting_phase3_pl_job",
    selection=AssetSelection.groups("product_reporting_mart") | AssetSelection.groups("product_reporting_mart_quality"),
)

product_reporting_phase5_quality_job = define_asset_job(
    name="product_reporting_phase5_quality_job",
    selection=(
        AssetSelection.groups("product_reporting_quality")
        | AssetSelection.groups("product_reporting_business_vault_quality")
        | AssetSelection.groups("product_reporting_mart_quality")
    ),
)


defs = Definitions(
    assets=[
        platform_baseline_decisions,
        stg_product_reporting_content_events_contract,
        stg_product_reporting_reactions_contract,
        stg_product_reporting_feed_events_contract,
        hub_reporting_content_contract,
        hub_reporting_reaction_contract,
        hub_reporting_feed_item_contract,
        link_reporting_reaction_content_contract,
        link_reporting_feed_item_content_contract,
        sat_reporting_content_event_contract,
        sat_reporting_reaction_event_contract,
        sat_reporting_feed_serving_event_contract,
        product_reporting_stage_reconciliation_contract,
        pit_reporting_content_daily_contract,
        br_content_reaction_daily_contract,
        br_feed_interest_proxy_contract,
        s_occupation_cohort_daily_contract,
        s_content_performance_daily_contract,
        s_emoji_usage_daily_contract,
        s_reaction_valence_daily_contract,
        product_reporting_bdv_contract_coverage_contract,
        mart_product_reporting_occupation_cohort_daily_contract,
        mart_product_reporting_content_performance_daily_contract,
        mart_product_reporting_emoji_reaction_daily_contract,
        mart_product_reporting_reaction_valence_daily_contract,
        mart_product_reporting_feed_interest_proxy_daily_contract,
        mart_product_reporting_contract_coverage_contract,
        product_reporting_stage_reconciliation_invariants_contract,
        product_reporting_bdv_formula_invariants_contract,
        product_reporting_mart_no_duplicate_daily_keys_contract,
        product_reporting_mart_contract_metadata_present_contract,
        product_reporting_mart_expected_contract_ids_present_contract,
        product_reporting_forbidden_output_columns_absent_contract,
        analytics_raw_event_landing_smoke,
        dbt_phase_d_smoke,
        soda_raw_event_landing_scan,
    ],
    jobs=[
        phase_d_local_smoke_job,
        product_reporting_phase1_stage_rdv_job,
        product_reporting_phase2_bdv_job,
        product_reporting_phase3_pl_job,
        product_reporting_phase5_quality_job,
    ],
)
