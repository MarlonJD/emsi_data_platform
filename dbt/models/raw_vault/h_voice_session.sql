{{ config(materialized="view", tags=["product_reporting_privacy_contract", "voice_usage_raw_vault"]) }}

select distinct
  voice_session_hk,
  voice_session_key as voice_session_business_key,
  load_datetime,
  record_source
from {{ ref("stg_analytics_voice_session_summary") }}
where voice_session_key is not null
