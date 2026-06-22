{{ config(materialized="view", tags=["product_reporting_privacy_contract", "voice_usage_raw_vault"]) }}

select
  voice_session_hk,
  voice_session_summary_hashdiff as hashdiff,
  load_datetime,
  record_source,
  microphone_unmuted_duration_bucket,
  unmute_count_bucket,
  first_unmute_latency_bucket,
  never_unmuted,
  permission_state,
  speech_activity_duration_bucket,
  speech_activity_ratio_bucket,
  voice_speaker_activity_legal_mode,
  purpose_id,
  legal_basis_id,
  notice_version,
  retention_policy_id,
  record_source_mode,
  production_write_enabled
from {{ ref("stg_analytics_voice_session_summary") }}
