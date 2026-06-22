{{ config(materialized="view", tags=["product_reporting_privacy_contract", "voice_usage_raw_vault"]) }}

select
  voice_session_hk,
  voice_session_summary_hashdiff as hashdiff,
  load_datetime,
  record_source,
  reconnect_count_bucket,
  join_failure_reason_code,
  rtt_bucket,
  jitter_bucket,
  packet_loss_bucket,
  platform,
  app_version,
  purpose_id,
  retention_policy_id,
  record_source_mode,
  production_write_enabled
from {{ ref("stg_analytics_voice_session_summary") }}
