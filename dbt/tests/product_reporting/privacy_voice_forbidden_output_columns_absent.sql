{{ config(tags=["product_reporting_privacy_contract", "product_reporting_quality"]) }}

select
  table_schema,
  table_name,
  column_name
from information_schema.columns
where lower(table_schema) in (
    lower('{{ target.schema }}_stage'),
    lower('{{ target.schema }}_raw_vault'),
    lower('{{ target.schema }}_business_vault')
  )
  and (
    table_name like '%voice%'
    or table_name like '%recap%'
    or table_name like '%privacy%'
  )
  and (
    lower(column_name) in (
      'audio_frame',
      'spoken_words',
      'topic',
      'sentiment',
      'emotion',
      'voiceprint',
      'speaker_embedding',
      'speaker_identity',
      'speaking_timeline',
      'speaking_interval',
      'vad_frame_list',
      'co_participant_key',
      'pairwise_duration',
      'private_room_name',
      'exact_gps',
      'request_body',
      'response_body',
      'screenshot',
      'ocr',
      'raw_filename'
    )
    or lower(column_name) like '%transcript%'
  )
