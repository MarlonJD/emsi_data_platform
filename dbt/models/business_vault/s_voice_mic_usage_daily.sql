{{ config(materialized="view", tags=["product_reporting_privacy_contract", "voice_usage_business_vault"]) }}

select
  reporting_date,
  room_type_key,
  platform,
  app_version,
  microphone_unmuted_duration_bucket,
  unmute_count_bucket,
  first_unmute_latency_bucket,
  permission_state,
  never_unmuted,
  count(distinct voice_session_hk) as voice_session_count,
  case
    when count(distinct subject_user_hk) >= 10 then 'reportable'
    else 'suppressed'
  end as small_cell_suppression_status,
  'direct'::text as wording_status,
  'voice_mic_usage_daily'::text as metric_contract_id,
  'current_effective_default_false_until_legal_go'::text as source_completeness_label,
  'Europe/Istanbul'::text as reporting_timezone
from {{ ref("stg_analytics_voice_session_summary") }}
group by
  reporting_date,
  room_type_key,
  platform,
  app_version,
  microphone_unmuted_duration_bucket,
  unmute_count_bucket,
  first_unmute_latency_bucket,
  permission_state,
  never_unmuted
