{{ config(tags=["product_reporting_privacy_contract", "product_reporting_quality"]) }}

select
  event_id,
  voice_speaker_activity_legal_mode,
  record_source_mode,
  production_write_enabled
from {{ ref("stg_analytics_voice_session_summary") }}
where production_write_enabled = true
   or (
    voice_speaker_activity_legal_mode = 'disabled'
    and record_source_mode not in ('synthetic_fixture', 'local_contract_test')
  )
