{{ config(materialized="view", tags=["product_reporting_privacy_contract", "voice_usage_raw_vault"]) }}

select distinct
  md5(concat_ws(':', voice_session_hk, voice_room_hk)) as voice_session_room_lhk,
  voice_session_hk,
  voice_room_hk,
  load_datetime,
  record_source
from {{ ref("stg_analytics_voice_session_summary") }}
where voice_session_hk is not null
  and voice_room_hk is not null
