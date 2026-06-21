{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

select
  table_schema,
  table_name,
  column_name
from information_schema.columns
where lower(table_schema) in (
    lower('{{ target.schema }}_stage'),
    lower('{{ target.schema }}_raw_vault'),
    lower('{{ target.schema }}_business_vault'),
    lower('{{ target.schema }}_mart')
  )
  and (
    table_name like '%product_reporting%'
    or table_name like '%reporting_%'
  )
  and (
    lower(column_name) in (
      'raw_content',
      'raw_text',
      'body',
      'message',
      'note',
      'note_body',
      'raw_note',
      'raw_note_text',
      'internal_note',
      'private_note',
      'applicant_message',
      'feedback_message',
      'search_text',
      'raw_search_text',
      'post_body',
      'comment_body',
      'reply_body',
      'dm_content',
      'transcript',
      'screenshot',
      'request_body',
      'response_body',
      'contact_value',
      'reveal_value',
      'payload_value',
      'exact_gps',
      'gps',
      'latitude',
      'longitude',
      'lat',
      'lon'
    )
    or lower(column_name) like '%token%'
    or lower(column_name) like '%transcript%'
    or lower(column_name) like '%screenshot%'
  )
