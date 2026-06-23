from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    asset,
    define_asset_job,
    sensor,
)

from dagster_project.soda_quality_gate import (
    PRODUCT_REPORTING_SODA_CONTRACT_NAMES,
    PRODUCT_REPORTING_SODA_EXPECTED_CHECK_COUNTS,
    PRODUCT_REPORTING_SODA_EXPECTED_TOTAL_CHECK_COUNT,
    assert_soda_contract_gate,
    format_soda_check_counts,
    validate_product_reporting_soda_contract_names,
)
from dagster_project.quality_persistence import (
    build_quality_run_result,
    dataset_name_from_contract,
    persist_quality_run_result,
    utcnow,
)


WORKSPACE_DIR = Path(os.getenv("DATA_PLATFORM_WORKSPACE", "/workspace"))
DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", WORKSPACE_DIR / "dbt"))
SODA_CONFIG_PATH = Path(os.getenv("SODA_CONFIG_PATH", WORKSPACE_DIR / "soda" / "configuration.yml"))
SODA_CONTRACT_PATH = Path(
    os.getenv("SODA_CONTRACT_PATH", WORKSPACE_DIR / "soda" / "contracts" / "raw_event_landing.yml")
)
PRODUCT_REPORTING_SODA_CONTRACT_DIR = Path(
    os.getenv("PRODUCT_REPORTING_SODA_CONTRACT_DIR", WORKSPACE_DIR / "soda" / "contracts")
)
PRODUCT_REPORTING_SODA_CONTRACT_PATHS = tuple(
    PRODUCT_REPORTING_SODA_CONTRACT_DIR / contract_name
    for contract_name in PRODUCT_REPORTING_SODA_CONTRACT_NAMES
)
PRIVACY_SODA_CONTRACT_NAMES = (
    "voice_usage_session_summary.yml",
    "privacy_lifecycle_contract.yml",
    "personal_recap_monthly.yml",
)
PRIVACY_SODA_CONTRACT_PATHS = tuple(
    PRODUCT_REPORTING_SODA_CONTRACT_DIR / contract_name
    for contract_name in PRIVACY_SODA_CONTRACT_NAMES
)
PRIVACY_LIFECYCLE_DEFAULT_SOURCE_PACKET_PATH = (
    WORKSPACE_DIR / "ingest_worker" / "fixtures" / "privacy_lifecycle_source_bound_packet.json"
)
PRIVACY_REQUEST_SENSOR_INBOX_PATH = Path(
    os.getenv(
        "PRIVACY_REQUEST_SENSOR_INBOX_PATH",
        WORKSPACE_DIR / "ingest_worker" / "runtime" / "privacy_request_events.jsonl",
    )
)
PRIVACY_REQUEST_SENSOR_TYPES = (
    "account_deletion",
    "analytics_opt_out",
    "recap_opt_out",
    "feed_profile_reset",
    "kvkk_deletion_request",
)


class LocalSmokeCommandError(RuntimeError):
    def __init__(self, message: str, output: str) -> None:
        super().__init__(message)
        self.output = output

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
    r'"raw_audio"|"audio_frame"|"spoken_words"|"caption"|"summary"|"topic"|'
    r'"sentiment"|"emotion"|"voiceprint"|"speaker_embedding"|"speaker_identity"|'
    r'"speaking_timeline"|"speaking_interval"|"vad_frame_list"|'
    r'"co_participant_key"|"pairwise_duration"|"private_room_name"|'
    r'"raw_room_title"|"ocr"|"raw_filename"|'
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
    "stage.stg_product_reporting_channel_sessions": {
        "group": "product_reporting_stage",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "text_channel_session_events_only, no_raw_channel_title, bounded_duration_fields",
    },
    "stage.stg_product_reporting_event_funnel": {
        "group": "product_reporting_stage",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "event_funnel_events_only, bounded_event_fields, no_location_or_free_text",
    },
    "stage.stg_app_together_items": {
        "group": "product_reporting_stage",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "together_events_only, no_prompt_or_note_text, bounded_coordination_fields",
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
        "checks": "source_to_stage_counts_present, partition_scoped_accounting, unexplained_delta_zero",
    },
    "raw_vault.h_channel": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "hub_hash_key_not_null, hub_hash_key_unique, no_raw_channel_title",
    },
    "raw_vault.l_user_channel_session": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "link_hash_key_not_null, linked_subject_channel_session_present",
    },
    "raw_vault.s_channel_session_raw": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "hashdiff_present, bounded_channel_session_fields, forbidden_raw_fields_absent",
    },
    "raw_vault.h_event": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "hub_hash_key_not_null, hub_hash_key_unique, no_exact_location",
    },
    "raw_vault.l_event_participant": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "link_hash_key_not_null, linked_subject_event_action_present",
    },
    "raw_vault.s_event_metadata_raw": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "hashdiff_present, event_funnel_fields_bounded, no_location_or_free_text",
    },
    "raw_vault.h_together_item": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "hub_hash_key_not_null, hub_hash_key_unique",
    },
    "raw_vault.l_together_actor_target": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "link_hash_key_not_null, actor_target_keys_present",
    },
    "raw_vault.s_together_metadata_raw": {
        "group": "product_reporting_raw_vault",
        "cadence": "hourly_reporting_job",
        "freshness": "<= 1 hour",
        "checks": "hashdiff_present, no_together_prompt_or_note_text, bounded_coordination_fields",
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
    "business_vault.s_channel_session_daily": {
        "group": "product_reporting_business_vault",
        "cadence": "daily_business_reporting_job",
        "freshness": "<= 24 hours",
        "checks": "channel_session_count_and_duration, small_cell_suppression_present, bdv_only_pending_pl",
    },
    "business_vault.pit_event_daily": {
        "group": "product_reporting_business_vault",
        "cadence": "daily_business_reporting_job",
        "freshness": "<= 24 hours",
        "checks": "event_daily_pit_grain, source_completeness_label_present, suppression_status_present",
    },
    "business_vault.br_event_funnel": {
        "group": "product_reporting_business_vault",
        "cadence": "daily_business_reporting_job",
        "freshness": "<= 24 hours",
        "checks": "event_action_bridge_grain, proxy_wording_pending_pl, suppression_status_present",
    },
    "business_vault.s_event_funnel_daily": {
        "group": "product_reporting_business_vault",
        "cadence": "daily_business_reporting_job",
        "freshness": "<= 24 hours",
        "checks": "event_funnel_contract_ids_present, proxy_partial_wording, bdv_only_pending_pl",
    },
    "business_vault.pit_together_daily": {
        "group": "product_reporting_business_vault",
        "cadence": "daily_business_reporting_job",
        "freshness": "<= 24 hours",
        "checks": "together_daily_pit_grain, bounded_status_fields, suppression_status_present",
    },
    "business_vault.br_together_response_flow": {
        "group": "product_reporting_business_vault",
        "cadence": "daily_business_reporting_job",
        "freshness": "<= 24 hours",
        "checks": "response_flow_bridge_grain, no_private_affinity, suppression_status_present",
    },
    "business_vault.s_together_coordination_daily": {
        "group": "product_reporting_business_vault",
        "cadence": "daily_business_reporting_job",
        "freshness": "<= 24 hours",
        "checks": "together_metric_contract_ids_present, proxy_partial_wording, suppression_status_present",
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
    "mart.mart_product_reporting_together_coordination_daily": {
        "group": "product_reporting_mart",
        "cadence": "daily_business_reporting_job",
        "freshness": "<= 24 hours",
        "checks": "suppressed_rows_absent, proxy_partial_wording, no_private_communication_affinity",
    },
    "mart.mart_product_reporting_contract_coverage": {
        "group": "product_reporting_mart_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "pl_metric_contracts_covered, wording_status_present, suppression_rule_present",
    },
}

PRODUCT_REPORTING_PHASE5_ASSETS = {
    "raw_vault.product_reporting_partition_trust_state": {
        "group": "product_reporting_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "asset_freshness_status, partition_completeness_status, complete_through, combined_trust_status",
    },
    "quality.product_reporting_stage_reconciliation_invariants": {
        "group": "product_reporting_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "row_count_non_negative, distinct_events_not_above_rows, missing_business_keys_bounded, accepted_landing_to_stage_balances, event_id_raw_hash_contradictions_zero",
        "dbt_test": "product_reporting_stage_reconciliation_invariants",
    },
    "quality.product_reporting_stage_reconciliation_negative_fixture_guard": {
        "group": "product_reporting_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "safe_negative_fixture_detects_unexplained_delta, forced_failure_var_available",
        "dbt_test": "product_reporting_stage_reconciliation_negative_fixture_guard",
    },
    "quality.product_reporting_rdv_hub_invariants": {
        "group": "product_reporting_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "hub_hash_key_not_null, business_key_not_null, hub_hash_key_unique, business_key_hash_mapping_stable",
        "dbt_test": "product_reporting_rdv_hub_invariants",
    },
    "quality.product_reporting_rdv_link_invariants": {
        "group": "product_reporting_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "link_hash_key_not_null, participant_keys_not_null, link_hash_key_unique, linked_hubs_present, no_pairwise_voice_or_private_affinity_expansion",
        "dbt_test": "product_reporting_rdv_link_invariants",
    },
    "quality.product_reporting_rdv_satellite_invariants": {
        "group": "product_reporting_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "satellite_parent_exists, history_grain_unique, hashdiff_state_stable, load_timestamp_not_before_received_at, forbidden_satellite_fields_absent, disabled_voice_conditional_path",
        "dbt_test": "product_reporting_rdv_satellite_invariants",
    },
    "quality.product_reporting_pit_content_daily_invariants": {
        "group": "product_reporting_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "pit_daily_grain_unique, selected_satellite_not_after_as_of, late_arrival_restatement_state, deleted_or_opted_out_subjects_absent",
        "dbt_test": "product_reporting_pit_content_daily_invariants",
    },
    "quality.product_reporting_partition_trust_state_invariants": {
        "group": "product_reporting_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "freshness_does_not_hide_missing_partitions, stale_complete_partitions_fail, expected_empty_and_late_open_classified",
        "dbt_test": "product_reporting_partition_trust_state_invariants",
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
    "quality.product_reporting_soda_mart_contracts": {
        "group": "product_reporting_mart_quality",
        "cadence": "nightly_quality_job",
        "freshness": "<= 24 hours",
        "checks": "soda_contracts_for_pl_marts_pass",
        "soda_contracts": ",".join(PRODUCT_REPORTING_SODA_CONTRACT_NAMES),
    },
}

PRODUCT_REPORTING_ASSETS = {
    **PRODUCT_REPORTING_PHASE1_ASSETS,
    **PRODUCT_REPORTING_PHASE2_ASSETS,
    **PRODUCT_REPORTING_PHASE3_ASSETS,
    **PRODUCT_REPORTING_PHASE5_ASSETS,
}

PRIVACY_LIFECYCLE_ASSETS = {
    "privacy.retention_policy_registry": {
        "group": "privacy_policy",
        "cadence": "privacy_lifecycle_daily_job",
        "freshness": "<= 24 hours",
        "checks": "retention_policy_ids_present, approval_id_present, anonymize_or_delete_named",
        "status": "local_source_bound_runtime_ready",
    },
    "privacy.retention_candidates_daily": {
        "group": "privacy_retention",
        "cadence": "privacy_lifecycle_daily_job",
        "freshness": "<= 24 hours",
        "checks": "expired_partitions_identified, personal_state_classified, source_window_declared",
        "status": "local_source_bound_runtime_ready",
    },
    "privacy.anonymous_candidate_build": {
        "group": "privacy_anonymization",
        "cadence": "privacy_lifecycle_daily_job",
        "freshness": "<= 24 hours",
        "checks": "identifier_free_candidate, allowed_dimensions_only, method_version_present",
        "status": "local_source_bound_runtime_ready",
    },
    "privacy.anonymization_checks": {
        "group": "privacy_quality",
        "cadence": "privacy_lifecycle_daily_job",
        "freshness": "<= 24 hours",
        "checks": "n_thresholds, complementary_suppression, reidentification_risk_check, evidence_id_present",
        "status": "local_source_bound_runtime_ready",
    },
    "privacy.anonymous_aggregate_publish": {
        "group": "privacy_anonymization",
        "cadence": "privacy_lifecycle_daily_job",
        "freshness": "<= 24 hours",
        "checks": "publish_only_after_passed_checks, anonymous_zone_no_stable_identifier",
        "status": "local_source_bound_runtime_ready",
    },
    "privacy.anonymize_or_delete_decisions": {
        "group": "privacy_deletion",
        "cadence": "privacy_lifecycle_daily_job",
        "freshness": "<= 24 hours",
        "checks": "expired_records_have_anonymize_or_delete_decision, failed_anonymization_routes_to_delete",
        "status": "local_source_bound_runtime_ready",
    },
    "privacy.anonymize_or_delete_outcomes": {
        "group": "privacy_quality",
        "cadence": "privacy_lifecycle_daily_job",
        "freshness": "<= 24 hours",
        "checks": "anonymous_publish_or_personal_purge_recorded, lifecycle_audit_receipts_present",
        "status": "local_source_bound_runtime_ready",
    },
    "privacy.expired_personal_candidates": {
        "group": "privacy_deletion",
        "cadence": "privacy_lifecycle_daily_job",
        "freshness": "<= 24 hours",
        "checks": "expired_personal_rows_enumerated, delete_scope_partition_scoped",
        "status": "local_source_bound_runtime_ready",
    },
    "privacy.personal_data_purge": {
        "group": "privacy_deletion",
        "cadence": "privacy_lifecycle_daily_job",
        "freshness": "<= 24 hours",
        "checks": "purge_runs_even_when_anonymization_fails, late_event_resurrection_blocked",
        "status": "local_source_bound_runtime_ready",
    },
    "privacy.downstream_cleanup_checks": {
        "group": "privacy_quality",
        "cadence": "privacy_lifecycle_daily_job",
        "freshness": "<= 24 hours",
        "checks": "analytics_optout_cleanup, recap_optout_cleanup, account_deletion_cleanup, feed_profile_reset_cleanup",
        "status": "local_source_bound_runtime_ready",
    },
    "privacy.lifecycle_audit": {
        "group": "privacy_audit",
        "cadence": "privacy_lifecycle_daily_job",
        "freshness": "<= 24 hours",
        "checks": "minimum_three_year_audit, incident_record_on_anonymization_failure, evidence_id_present",
        "status": "local_source_bound_runtime_ready",
    },
    "privacy.source_bound_runtime_report": {
        "group": "privacy_quality",
        "cadence": "privacy_lifecycle_daily_job",
        "freshness": "<= 24 hours",
        "checks": "source_bound_packet_valid, local_runtime_report_passed, production_api_dashboard_clickhouse_disabled",
        "status": "local_source_bound_runtime_ready",
        "source_packet": "ingest_worker/fixtures/privacy_lifecycle_source_bound_packet.json",
    },
}

VOICE_USAGE_ASSETS = {
    "stage.stg_analytics_voice_session_summary": {
        "group": "voice_usage_stage",
        "cadence": "privacy_contract_guard_job",
        "freshness": "<= 24 hours",
        "checks": "allowed_bounded_fields_only, production_rows_absent_until_legal_go",
        "status": "contract_scaffold_only",
    },
    "raw_vault.s_voice_mic_activity_raw": {
        "group": "voice_usage_raw_vault",
        "cadence": "privacy_contract_guard_job",
        "freshness": "<= 24 hours",
        "checks": "no_timeline_no_pairwise_no_audio_content, legal_mode_disabled_before_go",
        "status": "contract_scaffold_only",
    },
    "business_vault.s_voice_speech_activity_daily": {
        "group": "voice_usage_business_vault",
        "cadence": "privacy_contract_guard_job",
        "freshness": "<= 24 hours",
        "checks": "bounded_summary_only, small_cell_suppression, no_feed_personalization_output",
        "status": "contract_scaffold_only",
    },
    "quality.voice_usage_soda_contracts": {
        "group": "privacy_quality",
        "cadence": "privacy_contract_guard_job",
        "freshness": "<= 24 hours",
        "checks": "voice_usage_session_summary_contract_declared",
        "status": "contract_declared_not_executed_without_runtime_sources",
        "soda_contracts": ",".join(PRIVACY_SODA_CONTRACT_NAMES),
    },
}

PERSONAL_RECAP_ASSETS = {
    "private.s_user_private_recap_monthly": {
        "group": "personal_recap_private",
        "cadence": "privacy_contract_guard_job",
        "freshness": "<= 24 hours",
        "checks": "default_off, opt_in_required, self_only, raw_event_retention_not_extended",
        "status": "contract_scaffold_only",
    },
    "quality.personal_recap_deletion_checks": {
        "group": "privacy_quality",
        "cadence": "privacy_contract_guard_job",
        "freshness": "<= 24 hours",
        "checks": "recap_optout_deletes_private_counters, account_deletion_deletes_generated_recap",
        "status": "contract_scaffold_only",
    },
}

PRIVACY_ANALYTICS_CONTRACT_ASSETS = {
    **PRIVACY_LIFECYCLE_ASSETS,
    **VOICE_USAGE_ASSETS,
    **PERSONAL_RECAP_ASSETS,
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


def privacy_analytics_asset_contract(context: Any, asset_key: str) -> dict[str, str]:
    contract = PRIVACY_ANALYTICS_CONTRACT_ASSETS[asset_key]
    metadata = {"asset_key": asset_key, **contract}
    if asset_key.startswith("privacy.") and asset_key != "privacy.source_bound_runtime_report":
        metadata = {
            **metadata,
            "status": "local_source_bound_runtime_ready",
            "runtime_source": "privacy.source_bound_runtime_report",
        }
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


@asset(name="stg_product_reporting_channel_sessions", key_prefix=["stage"], group_name="product_reporting_stage")
def stg_product_reporting_channel_sessions_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "stage.stg_product_reporting_channel_sessions")


@asset(name="stg_product_reporting_event_funnel", key_prefix=["stage"], group_name="product_reporting_stage")
def stg_product_reporting_event_funnel_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "stage.stg_product_reporting_event_funnel")


@asset(name="stg_app_together_items", key_prefix=["stage"], group_name="product_reporting_stage")
def stg_app_together_items_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "stage.stg_app_together_items")


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


@asset(name="h_channel", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def h_channel_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.h_channel")


@asset(name="l_user_channel_session", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def l_user_channel_session_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.l_user_channel_session")


@asset(name="s_channel_session_raw", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def s_channel_session_raw_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.s_channel_session_raw")


@asset(name="h_event", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def h_event_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.h_event")


@asset(name="l_event_participant", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def l_event_participant_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.l_event_participant")


@asset(name="s_event_metadata_raw", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def s_event_metadata_raw_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.s_event_metadata_raw")


@asset(name="h_together_item", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def h_together_item_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.h_together_item")


@asset(name="l_together_actor_target", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def l_together_actor_target_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.l_together_actor_target")


@asset(name="s_together_metadata_raw", key_prefix=["raw_vault"], group_name="product_reporting_raw_vault")
def s_together_metadata_raw_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.s_together_metadata_raw")


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


@asset(name="s_channel_session_daily", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def s_channel_session_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.s_channel_session_daily")


@asset(name="pit_event_daily", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def pit_event_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.pit_event_daily")


@asset(name="br_event_funnel", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def br_event_funnel_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.br_event_funnel")


@asset(name="s_event_funnel_daily", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def s_event_funnel_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.s_event_funnel_daily")


@asset(name="pit_together_daily", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def pit_together_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.pit_together_daily")


@asset(name="br_together_response_flow", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def br_together_response_flow_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.br_together_response_flow")


@asset(name="s_together_coordination_daily", key_prefix=["business_vault"], group_name="product_reporting_business_vault")
def s_together_coordination_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "business_vault.s_together_coordination_daily")


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
    name="mart_product_reporting_together_coordination_daily",
    key_prefix=["mart"],
    group_name="product_reporting_mart",
)
def mart_product_reporting_together_coordination_daily_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "mart.mart_product_reporting_together_coordination_daily")


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
    name="product_reporting_stage_reconciliation_negative_fixture_guard",
    key_prefix=["quality"],
    group_name="product_reporting_quality",
)
def product_reporting_stage_reconciliation_negative_fixture_guard_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(
        context,
        "quality.product_reporting_stage_reconciliation_negative_fixture_guard",
    )


@asset(
    name="product_reporting_rdv_hub_invariants",
    key_prefix=["quality"],
    group_name="product_reporting_quality",
)
def product_reporting_rdv_hub_invariants_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "quality.product_reporting_rdv_hub_invariants")


@asset(
    name="product_reporting_rdv_link_invariants",
    key_prefix=["quality"],
    group_name="product_reporting_quality",
)
def product_reporting_rdv_link_invariants_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "quality.product_reporting_rdv_link_invariants")


@asset(
    name="product_reporting_rdv_satellite_invariants",
    key_prefix=["quality"],
    group_name="product_reporting_quality",
)
def product_reporting_rdv_satellite_invariants_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "quality.product_reporting_rdv_satellite_invariants")


@asset(
    name="product_reporting_pit_content_daily_invariants",
    key_prefix=["quality"],
    group_name="product_reporting_quality",
)
def product_reporting_pit_content_daily_invariants_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "quality.product_reporting_pit_content_daily_invariants")


@asset(
    name="product_reporting_partition_trust_state",
    key_prefix=["raw_vault"],
    group_name="product_reporting_quality",
)
def product_reporting_partition_trust_state_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "raw_vault.product_reporting_partition_trust_state")


@asset(
    name="product_reporting_partition_trust_state_invariants",
    key_prefix=["quality"],
    group_name="product_reporting_quality",
)
def product_reporting_partition_trust_state_invariants_contract(context) -> dict[str, str]:
    return product_reporting_asset_contract(context, "quality.product_reporting_partition_trust_state_invariants")


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


@asset(
    name="product_reporting_soda_mart_contracts",
    key_prefix=["quality"],
    group_name="product_reporting_mart_quality",
)
def product_reporting_soda_mart_contracts(context) -> dict[str, str]:
    metadata = product_reporting_asset_contract(context, "quality.product_reporting_soda_mart_contracts")
    validate_product_reporting_soda_contract_names(PRODUCT_REPORTING_SODA_CONTRACT_NAMES)
    verified_contracts = []
    observed_check_counts = {}
    for contract_path in PRODUCT_REPORTING_SODA_CONTRACT_PATHS:
        started_at = utcnow()
        output = ""
        observed_check_count = None
        try:
            output = run_soda_contract(context, contract_path)
            gate_result = assert_soda_contract_gate(
                contract_name=contract_path.name,
                output=output,
                expected_check_count=PRODUCT_REPORTING_SODA_EXPECTED_CHECK_COUNTS[contract_path.name],
            )
            observed_check_count = gate_result.observed_check_count
        except Exception as error:
            if isinstance(error, LocalSmokeCommandError):
                output = error.output
            persist_quality_run_result(
                build_quality_run_result(
                    contract_name=contract_path.name,
                    dataset_name=dataset_name_from_contract(contract_path),
                    pipeline_run_id=getattr(context, "run_id", None),
                    status="failed",
                    started_at=started_at,
                    finished_at=utcnow(),
                    expected_check_count=PRODUCT_REPORTING_SODA_EXPECTED_CHECK_COUNTS[contract_path.name],
                    observed_check_count=observed_check_count,
                    output=output,
                    error_message=str(error),
                )
            )
            raise
        persist_quality_run_result(
            build_quality_run_result(
                contract_name=contract_path.name,
                dataset_name=dataset_name_from_contract(contract_path),
                pipeline_run_id=getattr(context, "run_id", None),
                status="passed",
                started_at=started_at,
                finished_at=utcnow(),
                expected_check_count=PRODUCT_REPORTING_SODA_EXPECTED_CHECK_COUNTS[contract_path.name],
                observed_check_count=observed_check_count,
                output=output,
            )
        )
        verified_contracts.append(contract_path.name)
        observed_check_counts[contract_path.name] = observed_check_count
    observed_total_check_count = sum(observed_check_counts.values())
    if observed_total_check_count != PRODUCT_REPORTING_SODA_EXPECTED_TOTAL_CHECK_COUNT:
        raise RuntimeError(
            "Product Reporting Soda gate expected "
            f"{PRODUCT_REPORTING_SODA_EXPECTED_TOTAL_CHECK_COUNT} total checks, "
            f"observed {observed_total_check_count}"
        )
    output_metadata = {
        **metadata,
        "status": "passed",
        "contract_count": str(len(verified_contracts)),
        "expected_contract_count": str(len(PRODUCT_REPORTING_SODA_CONTRACT_NAMES)),
        "expected_check_count": str(PRODUCT_REPORTING_SODA_EXPECTED_TOTAL_CHECK_COUNT),
        "observed_check_count": str(observed_total_check_count),
        "expected_check_counts": format_soda_check_counts(PRODUCT_REPORTING_SODA_EXPECTED_CHECK_COUNTS),
        "observed_check_counts": format_soda_check_counts(observed_check_counts),
        "critical_check_state": "all_evaluated",
        "verified_contracts": ",".join(verified_contracts),
    }
    context.add_output_metadata(output_metadata)
    return output_metadata


@asset(name="retention_policy_registry", key_prefix=["privacy"], group_name="privacy_policy")
def privacy_retention_policy_registry_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "privacy.retention_policy_registry")


@asset(name="retention_candidates_daily", key_prefix=["privacy"], group_name="privacy_retention")
def privacy_retention_candidates_daily_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "privacy.retention_candidates_daily")


@asset(name="anonymous_candidate_build", key_prefix=["privacy"], group_name="privacy_anonymization")
def privacy_anonymous_candidate_build_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "privacy.anonymous_candidate_build")


@asset(name="anonymization_checks", key_prefix=["privacy"], group_name="privacy_quality")
def privacy_anonymization_checks_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "privacy.anonymization_checks")


@asset(name="anonymous_aggregate_publish", key_prefix=["privacy"], group_name="privacy_anonymization")
def privacy_anonymous_aggregate_publish_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "privacy.anonymous_aggregate_publish")


@asset(name="anonymize_or_delete_decisions", key_prefix=["privacy"], group_name="privacy_deletion")
def privacy_anonymize_or_delete_decisions_contract(context) -> dict[str, str]:
    from ingest_worker import privacy_lifecycle_runtime

    packet_path = Path(
        os.getenv(
            "PRIVACY_LIFECYCLE_SOURCE_PACKET_PATH",
            str(PRIVACY_LIFECYCLE_DEFAULT_SOURCE_PACKET_PATH),
        )
    )
    report = privacy_lifecycle_runtime.build_runtime_report_from_path(packet_path)
    metadata = privacy_analytics_asset_contract(context, "privacy.anonymize_or_delete_decisions")
    anonymize_count = sum(1 for check in report["anonymous_candidate_checks"] if check["passed"])
    delete_count = sum(1 for check in report["anonymous_candidate_checks"] if not check["passed"])
    output_metadata = {
        **metadata,
        "status": "passed",
        "decision_policy": "anonymize_or_delete",
        "anonymize_decision_count": str(anonymize_count),
        "delete_decision_count": str(delete_count),
        "api_exposure_enabled": "false",
        "dashboard_exposure_enabled": "false",
    }
    context.add_output_metadata(output_metadata)
    return output_metadata


@asset(name="anonymize_or_delete_outcomes", key_prefix=["privacy"], group_name="privacy_quality")
def privacy_anonymize_or_delete_outcomes_contract(context) -> dict[str, str]:
    from ingest_worker import privacy_lifecycle_runtime

    packet_path = Path(
        os.getenv(
            "PRIVACY_LIFECYCLE_SOURCE_PACKET_PATH",
            str(PRIVACY_LIFECYCLE_DEFAULT_SOURCE_PACKET_PATH),
        )
    )
    report = privacy_lifecycle_runtime.build_runtime_report_from_path(packet_path)
    metadata = privacy_analytics_asset_contract(context, "privacy.anonymize_or_delete_outcomes")
    output_metadata = {
        **metadata,
        "status": "passed",
        "published_anonymous_aggregate_count": str(len(report["published_anonymous_aggregates"])),
        "purged_source_record_ref_count": str(len(report["purged_source_record_refs"])),
        "audit_receipt_count": str(len(report["lifecycle_audit_receipts"])),
        "production_collection_enabled": "false",
        "clickhouse_production_promotion": "false",
    }
    context.add_output_metadata(output_metadata)
    return output_metadata


@asset(name="expired_personal_candidates", key_prefix=["privacy"], group_name="privacy_deletion")
def privacy_expired_personal_candidates_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "privacy.expired_personal_candidates")


@asset(name="personal_data_purge", key_prefix=["privacy"], group_name="privacy_deletion")
def privacy_personal_data_purge_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "privacy.personal_data_purge")


@asset(name="downstream_cleanup_checks", key_prefix=["privacy"], group_name="privacy_quality")
def privacy_downstream_cleanup_checks_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "privacy.downstream_cleanup_checks")


@asset(name="lifecycle_audit", key_prefix=["privacy"], group_name="privacy_audit")
def privacy_lifecycle_audit_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "privacy.lifecycle_audit")


@asset(name="source_bound_runtime_report", key_prefix=["privacy"], group_name="privacy_quality")
def privacy_source_bound_runtime_report(context) -> dict[str, str]:
    from ingest_worker import privacy_lifecycle_runtime

    packet_path = Path(
        os.getenv(
            "PRIVACY_LIFECYCLE_SOURCE_PACKET_PATH",
            str(PRIVACY_LIFECYCLE_DEFAULT_SOURCE_PACKET_PATH),
        )
    )
    report = privacy_lifecycle_runtime.build_runtime_report_from_path(packet_path)
    metadata = privacy_analytics_asset_contract(context, "privacy.source_bound_runtime_report")
    output_metadata = {
        **metadata,
        "status": "passed",
        "classification": report["classification"],
        "target_name": report["source_binding"]["target_name"],
        "target_class": report["source_binding"]["target_class"],
        "source_window_start": report["source_binding"]["source_window"]["start"],
        "source_window_end": report["source_binding"]["source_window"]["end"],
        "retention_candidate_count": str(len(report["retention_candidates"])),
        "anonymous_check_count": str(len(report["anonymous_candidate_checks"])),
        "cleanup_action_count": str(len(report["cleanup_actions"])),
        "audit_receipt_count": str(len(report["lifecycle_audit_receipts"])),
        "api_exposure_enabled": "false",
        "dashboard_exposure_enabled": "false",
        "clickhouse_production_promotion": "false",
    }
    context.add_output_metadata(output_metadata)
    return output_metadata


@asset(name="stg_analytics_voice_session_summary", key_prefix=["stage"], group_name="voice_usage_stage")
def stg_analytics_voice_session_summary_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "stage.stg_analytics_voice_session_summary")


@asset(name="s_voice_mic_activity_raw", key_prefix=["raw_vault"], group_name="voice_usage_raw_vault")
def s_voice_mic_activity_raw_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "raw_vault.s_voice_mic_activity_raw")


@asset(name="s_voice_speech_activity_daily", key_prefix=["business_vault"], group_name="voice_usage_business_vault")
def s_voice_speech_activity_daily_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "business_vault.s_voice_speech_activity_daily")


@asset(name="voice_usage_soda_contracts", key_prefix=["quality"], group_name="privacy_quality")
def voice_usage_soda_contracts_contract(context) -> dict[str, str]:
    metadata = privacy_analytics_asset_contract(context, "quality.voice_usage_soda_contracts")
    output_metadata = {
        **metadata,
        "status": "declared_not_executed_without_runtime_sources",
        "contract_count": str(len(PRIVACY_SODA_CONTRACT_PATHS)),
        "declared_contracts": ",".join(contract_path.name for contract_path in PRIVACY_SODA_CONTRACT_PATHS),
    }
    context.add_output_metadata(output_metadata)
    return output_metadata


@asset(name="s_user_private_recap_monthly", key_prefix=["private"], group_name="personal_recap_private")
def s_user_private_recap_monthly_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "private.s_user_private_recap_monthly")


@asset(name="personal_recap_deletion_checks", key_prefix=["quality"], group_name="privacy_quality")
def personal_recap_deletion_checks_contract(context) -> dict[str, str]:
    return privacy_analytics_asset_contract(context, "quality.personal_recap_deletion_checks")


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
    run_soda_contract(context, SODA_CONTRACT_PATH)
    return {"status": "passed", "contract_path": str(SODA_CONTRACT_PATH)}


def run_soda_contract(context: Any, contract_path: Path) -> str:
    return run_command(
        context,
        [
            "soda",
            "contract",
            "verify",
            "--data-source",
            str(SODA_CONFIG_PATH),
            "--contract",
            str(contract_path),
        ],
    )


def run_command(context: Any, command: list[str]) -> str:
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
        raise LocalSmokeCommandError(
            f"local smoke command failed with exit {completed.returncode}: {' '.join(command)}",
            completed.stdout or "",
        )
    return completed.stdout or ""


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

privacy_lifecycle_daily_job = define_asset_job(
    name="privacy_lifecycle_daily_job",
    selection=(
        AssetSelection.groups("privacy_policy")
        | AssetSelection.groups("privacy_retention")
        | AssetSelection.groups("privacy_anonymization")
        | AssetSelection.groups("privacy_deletion")
        | AssetSelection.groups("privacy_quality")
        | AssetSelection.groups("privacy_audit")
    ),
)

privacy_contract_guard_job = define_asset_job(
    name="privacy_contract_guard_job",
    selection=(
        AssetSelection.groups("voice_usage_stage")
        | AssetSelection.groups("voice_usage_raw_vault")
        | AssetSelection.groups("voice_usage_business_vault")
        | AssetSelection.groups("personal_recap_private")
        | AssetSelection.groups("privacy_quality")
    ),
)

phase_d_local_smoke_daily_schedule = ScheduleDefinition(
    name="phase_d_local_smoke_daily_schedule",
    job=phase_d_local_smoke_job,
    cron_schedule="15 6 * * *",
    execution_timezone="Europe/Istanbul",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"emsi.local_dev": "true", "emsi.schedule": "phase_d_local_smoke_daily"},
    description=(
        "Runs the local ingest, dbt, and Soda smoke once daily while the local "
        "Dagster daemon is active."
    ),
)

privacy_lifecycle_daily_schedule = ScheduleDefinition(
    name="privacy_lifecycle_daily_schedule",
    job=privacy_lifecycle_daily_job,
    cron_schedule="30 6 * * *",
    execution_timezone="Europe/Istanbul",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"emsi.local_dev": "true", "emsi.schedule": "privacy_lifecycle_daily"},
    description=(
        "Runs the local source-bound privacy lifecycle checks once daily while "
        "the local Dagster daemon is active."
    ),
)

product_reporting_phase5_quality_daily_schedule = ScheduleDefinition(
    name="product_reporting_phase5_quality_daily_schedule",
    job=product_reporting_phase5_quality_job,
    cron_schedule="45 6 * * *",
    execution_timezone="Europe/Istanbul",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={
        "emsi.local_dev": "true",
        "emsi.schedule": "product_reporting_phase5_quality_daily",
    },
    description=(
        "Runs the Product Reporting quality contract lane once daily while the "
        "local Dagster daemon is active."
    ),
)

product_reporting_phase1_stage_rdv_quarter_hourly_schedule = ScheduleDefinition(
    name="product_reporting_phase1_stage_rdv_quarter_hourly_schedule",
    job=product_reporting_phase1_stage_rdv_job,
    cron_schedule="*/15 * * * *",
    execution_timezone="Europe/Istanbul",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={
        "emsi.local_dev": "true",
        "emsi.schedule": "product_reporting_phase1_stage_rdv_quarter_hourly",
    },
    description=(
        "Runs the Product Reporting Stage/RDV contract lane every 15 minutes "
        "while the local Dagster daemon is active."
    ),
)

product_reporting_phase2_bdv_hourly_schedule = ScheduleDefinition(
    name="product_reporting_phase2_bdv_hourly_schedule",
    job=product_reporting_phase2_bdv_job,
    cron_schedule="5 * * * *",
    execution_timezone="Europe/Istanbul",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"emsi.local_dev": "true", "emsi.schedule": "product_reporting_phase2_bdv_hourly"},
    description=(
        "Runs the Product Reporting Business Vault contract lane hourly while "
        "the local Dagster daemon is active."
    ),
)

privacy_contract_guard_hourly_schedule = ScheduleDefinition(
    name="privacy_contract_guard_hourly_schedule",
    job=privacy_contract_guard_job,
    cron_schedule="10 * * * *",
    execution_timezone="Europe/Istanbul",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"emsi.local_dev": "true", "emsi.schedule": "privacy_contract_guard_hourly"},
    description=(
        "Runs the privacy and voice contract guard hourly while the local "
        "Dagster daemon is active."
    ),
)

product_reporting_phase3_pl_daily_schedule = ScheduleDefinition(
    name="product_reporting_phase3_pl_daily_schedule",
    job=product_reporting_phase3_pl_job,
    cron_schedule="35 6 * * *",
    execution_timezone="Europe/Istanbul",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"emsi.local_dev": "true", "emsi.schedule": "product_reporting_phase3_pl_daily"},
    description=(
        "Runs the Product Reporting PL mart contract lane once daily while the "
        "local Dagster daemon is active."
    ),
)

phase_d_local_smoke_weekly_schedule = ScheduleDefinition(
    name="phase_d_local_smoke_weekly_schedule",
    job=phase_d_local_smoke_job,
    cron_schedule="0 7 * * 1",
    execution_timezone="Europe/Istanbul",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"emsi.local_dev": "true", "emsi.schedule": "phase_d_local_smoke_weekly"},
    description=(
        "Runs the local ingest, dbt, and Soda smoke weekly while the local "
        "Dagster daemon is active."
    ),
)

product_reporting_phase5_quality_weekly_schedule = ScheduleDefinition(
    name="product_reporting_phase5_quality_weekly_schedule",
    job=product_reporting_phase5_quality_job,
    cron_schedule="15 7 * * 1",
    execution_timezone="Europe/Istanbul",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"emsi.local_dev": "true", "emsi.schedule": "product_reporting_phase5_quality_weekly"},
    description=(
        "Runs the Product Reporting quality contract lane weekly while the "
        "local Dagster daemon is active."
    ),
)

product_reporting_phase5_quality_monthly_schedule = ScheduleDefinition(
    name="product_reporting_phase5_quality_monthly_schedule",
    job=product_reporting_phase5_quality_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="Europe/Istanbul",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"emsi.local_dev": "true", "emsi.schedule": "product_reporting_phase5_quality_monthly"},
    description=(
        "Runs the Product Reporting quality contract lane monthly while the "
        "local Dagster daemon is active."
    ),
)

privacy_lifecycle_monthly_schedule = ScheduleDefinition(
    name="privacy_lifecycle_monthly_schedule",
    job=privacy_lifecycle_daily_job,
    cron_schedule="30 8 1 * *",
    execution_timezone="Europe/Istanbul",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"emsi.local_dev": "true", "emsi.schedule": "privacy_lifecycle_monthly"},
    description=(
        "Runs the source-bound privacy lifecycle checks monthly while the local "
        "Dagster daemon is active."
    ),
)


@sensor(
    name="privacy_request_sensor",
    job=privacy_lifecycle_daily_job,
    minimum_interval_seconds=300,
    description=(
        "Local privacy request trigger for account deletion, analytics opt-out, "
        "recap opt-out, feed profile reset, and KVKK deletion request events."
    ),
)
def privacy_request_sensor(context: Any):
    inbox_path = PRIVACY_REQUEST_SENSOR_INBOX_PATH
    if not inbox_path.exists():
        yield SkipReason(f"privacy request inbox not found: {inbox_path}")
        return

    emitted = False
    with inbox_path.open("r", encoding="utf-8") as inbox_file:
        for line_number, raw_line in enumerate(inbox_file, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                request_event = json.loads(line)
            except json.JSONDecodeError as exc:
                context.log.warning("Skipping malformed privacy request line %s: %s", line_number, exc)
                continue

            request_type = request_event.get("request_type")
            if request_type not in PRIVACY_REQUEST_SENSOR_TYPES:
                context.log.warning(
                    "Skipping unsupported privacy request type on line %s: %s",
                    line_number,
                    request_type,
                )
                continue

            request_id = str(request_event.get("request_id") or f"line-{line_number}")
            emitted = True
            yield RunRequest(
                run_key=f"{request_type}:{request_id}",
                tags={
                    "emsi.local_dev": "true",
                    "emsi.sensor": "privacy_request_sensor",
                    "emsi.privacy_request_type": request_type,
                },
            )

    if not emitted:
        yield SkipReason("privacy request inbox contained no supported request events")


defs = Definitions(
    assets=[
        platform_baseline_decisions,
        stg_product_reporting_content_events_contract,
        stg_product_reporting_reactions_contract,
        stg_product_reporting_feed_events_contract,
        stg_product_reporting_channel_sessions_contract,
        stg_product_reporting_event_funnel_contract,
        stg_app_together_items_contract,
        hub_reporting_content_contract,
        hub_reporting_reaction_contract,
        hub_reporting_feed_item_contract,
        link_reporting_reaction_content_contract,
        link_reporting_feed_item_content_contract,
        sat_reporting_content_event_contract,
        sat_reporting_reaction_event_contract,
        sat_reporting_feed_serving_event_contract,
        product_reporting_stage_reconciliation_contract,
        h_channel_contract,
        l_user_channel_session_contract,
        s_channel_session_raw_contract,
        h_event_contract,
        l_event_participant_contract,
        s_event_metadata_raw_contract,
        h_together_item_contract,
        l_together_actor_target_contract,
        s_together_metadata_raw_contract,
        pit_reporting_content_daily_contract,
        br_content_reaction_daily_contract,
        br_feed_interest_proxy_contract,
        s_occupation_cohort_daily_contract,
        s_content_performance_daily_contract,
        s_emoji_usage_daily_contract,
        s_reaction_valence_daily_contract,
        product_reporting_bdv_contract_coverage_contract,
        s_channel_session_daily_contract,
        pit_event_daily_contract,
        br_event_funnel_contract,
        s_event_funnel_daily_contract,
        pit_together_daily_contract,
        br_together_response_flow_contract,
        s_together_coordination_daily_contract,
        mart_product_reporting_occupation_cohort_daily_contract,
        mart_product_reporting_content_performance_daily_contract,
        mart_product_reporting_emoji_reaction_daily_contract,
        mart_product_reporting_reaction_valence_daily_contract,
        mart_product_reporting_feed_interest_proxy_daily_contract,
        mart_product_reporting_together_coordination_daily_contract,
        mart_product_reporting_contract_coverage_contract,
        product_reporting_partition_trust_state_contract,
        product_reporting_stage_reconciliation_invariants_contract,
        product_reporting_stage_reconciliation_negative_fixture_guard_contract,
        product_reporting_partition_trust_state_invariants_contract,
        product_reporting_rdv_hub_invariants_contract,
        product_reporting_rdv_link_invariants_contract,
        product_reporting_rdv_satellite_invariants_contract,
        product_reporting_pit_content_daily_invariants_contract,
        product_reporting_bdv_formula_invariants_contract,
        product_reporting_mart_no_duplicate_daily_keys_contract,
        product_reporting_mart_contract_metadata_present_contract,
        product_reporting_mart_expected_contract_ids_present_contract,
        product_reporting_forbidden_output_columns_absent_contract,
        product_reporting_soda_mart_contracts,
        privacy_retention_policy_registry_contract,
        privacy_retention_candidates_daily_contract,
        privacy_anonymous_candidate_build_contract,
        privacy_anonymization_checks_contract,
        privacy_anonymous_aggregate_publish_contract,
        privacy_anonymize_or_delete_decisions_contract,
        privacy_anonymize_or_delete_outcomes_contract,
        privacy_expired_personal_candidates_contract,
        privacy_personal_data_purge_contract,
        privacy_downstream_cleanup_checks_contract,
        privacy_lifecycle_audit_contract,
        privacy_source_bound_runtime_report,
        stg_analytics_voice_session_summary_contract,
        s_voice_mic_activity_raw_contract,
        s_voice_speech_activity_daily_contract,
        voice_usage_soda_contracts_contract,
        s_user_private_recap_monthly_contract,
        personal_recap_deletion_checks_contract,
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
        privacy_lifecycle_daily_job,
        privacy_contract_guard_job,
    ],
    schedules=[
        product_reporting_phase1_stage_rdv_quarter_hourly_schedule,
        product_reporting_phase2_bdv_hourly_schedule,
        privacy_contract_guard_hourly_schedule,
        phase_d_local_smoke_daily_schedule,
        product_reporting_phase3_pl_daily_schedule,
        privacy_lifecycle_daily_schedule,
        product_reporting_phase5_quality_daily_schedule,
        phase_d_local_smoke_weekly_schedule,
        product_reporting_phase5_quality_weekly_schedule,
        product_reporting_phase5_quality_monthly_schedule,
        privacy_lifecycle_monthly_schedule,
    ],
    sensors=[
        privacy_request_sensor,
    ],
)
