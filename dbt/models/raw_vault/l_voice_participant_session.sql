{{ config(materialized="view", tags=["product_reporting_privacy_contract", "voice_usage_raw_vault"]) }}

select distinct
  md5(concat_ws(':', subject_user_hk, voice_session_hk)) as voice_participant_session_lhk,
  subject_user_hk,
  voice_session_hk,
  voice_participant_session_hk,
  load_datetime,
  record_source
from {{ ref("stg_analytics_voice_session_summary") }}
where subject_user_hk is not null
  and voice_session_hk is not null
