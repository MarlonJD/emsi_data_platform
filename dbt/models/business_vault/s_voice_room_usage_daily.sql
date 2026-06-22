{{ config(materialized="view", tags=["product_reporting_privacy_contract", "voice_usage_business_vault"]) }}

select
  reporting_date,
  room_type_key,
  platform,
  app_version,
  count(distinct voice_session_hk) as voice_session_count,
  count(distinct voice_room_hk) as voice_room_count,
  count(distinct subject_user_hk) as distinct_participant_count,
  case
    when count(distinct subject_user_hk) >= 10 then 'reportable'
    else 'suppressed'
  end as small_cell_suppression_status,
  'direct'::text as wording_status,
  'voice_room_usage_daily'::text as metric_contract_id,
  'partial_until_source_bound_runtime_enabled'::text as source_completeness_label,
  'Europe/Istanbul'::text as reporting_timezone
from {{ ref("stg_analytics_voice_session_summary") }}
group by reporting_date, room_type_key, platform, app_version
