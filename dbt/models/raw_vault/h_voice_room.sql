{{ config(materialized="view", tags=["product_reporting_privacy_contract", "voice_usage_raw_vault"]) }}

select distinct
  voice_room_hk,
  room_key as voice_room_business_key,
  load_datetime,
  record_source
from {{ ref("stg_analytics_voice_session_summary") }}
where room_key is not null
