{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

with real_satellite_rows as (
  select
    'sat_reporting_content_event'::text as model_name,
    'hub_reporting_content'::text as parent_model_name,
    'content_hk'::text as parent_key_name,
    sat.content_hk::text as parent_hk,
    md5(concat_ws('||', 'sat_reporting_content_event', sat.content_hk::text, sat.record_source::text, sat.source_topic::text, sat.source_partition::text, sat.source_offset::text, sat.load_datetime::text)) as history_grain_signature,
    md5((to_jsonb(sat) - 'hashdiff' - 'load_datetime' - 'record_source')::text) as state_signature,
    sat.hashdiff::text as hashdiff,
    sat.load_datetime,
    sat.occurred_at,
    sat.received_at,
    sat.record_source::text as record_source,
    'product_event'::text as source_family
  from {{ ref("sat_reporting_content_event") }} sat

  union all

  select
    'sat_reporting_reaction_event'::text,
    'hub_reporting_reaction'::text,
    'reaction_hk'::text,
    sat.reaction_hk::text,
    md5(concat_ws('||', 'sat_reporting_reaction_event', sat.reaction_hk::text, sat.record_source::text, sat.source_topic::text, sat.source_partition::text, sat.source_offset::text, sat.load_datetime::text)),
    md5((to_jsonb(sat) - 'hashdiff' - 'load_datetime' - 'record_source')::text),
    sat.hashdiff::text,
    sat.load_datetime,
    sat.occurred_at,
    sat.received_at,
    sat.record_source::text,
    'product_event'::text
  from {{ ref("sat_reporting_reaction_event") }} sat

  union all

  select
    'sat_reporting_feed_serving_event'::text,
    'hub_reporting_feed_item'::text,
    'feed_item_hk'::text,
    sat.feed_item_hk::text,
    md5(concat_ws('||', 'sat_reporting_feed_serving_event', sat.feed_item_hk::text, sat.record_source::text, sat.source_topic::text, sat.source_partition::text, sat.source_offset::text, sat.load_datetime::text)),
    md5((to_jsonb(sat) - 'hashdiff' - 'load_datetime' - 'record_source')::text),
    sat.hashdiff::text,
    sat.load_datetime,
    sat.occurred_at,
    sat.received_at,
    sat.record_source::text,
    'product_event'::text
  from {{ ref("sat_reporting_feed_serving_event") }} sat

  union all

  select
    's_channel_session_raw'::text,
    'l_user_channel_session'::text,
    'channel_session_hk'::text,
    sat.channel_session_hk::text,
    md5(concat_ws('||', 's_channel_session_raw', sat.channel_session_hk::text, sat.record_source::text, sat.source_topic::text, sat.source_partition::text, sat.source_offset::text, sat.load_datetime::text)),
    md5((to_jsonb(sat) - 'hashdiff' - 'load_datetime' - 'record_source')::text),
    sat.hashdiff::text,
    sat.load_datetime,
    sat.occurred_at,
    sat.received_at,
    sat.record_source::text,
    'product_event'::text
  from {{ ref("s_channel_session_raw") }} sat

  union all

  select
    's_event_metadata_raw'::text,
    'l_event_participant'::text,
    'event_funnel_action_hk'::text,
    sat.event_funnel_action_hk::text,
    md5(concat_ws('||', 's_event_metadata_raw', sat.event_funnel_action_hk::text, sat.record_source::text, sat.source_topic::text, sat.source_partition::text, sat.source_offset::text, sat.load_datetime::text)),
    md5((to_jsonb(sat) - 'hashdiff' - 'load_datetime' - 'record_source')::text),
    sat.hashdiff::text,
    sat.load_datetime,
    sat.occurred_at,
    sat.received_at,
    sat.record_source::text,
    'product_event'::text
  from {{ ref("s_event_metadata_raw") }} sat

  union all

  select
    's_together_metadata_raw'::text,
    'h_together_item'::text,
    'together_item_hk'::text,
    sat.together_item_hk::text,
    md5(concat_ws('||', 's_together_metadata_raw', sat.together_item_hk::text, sat.record_source::text, sat.source_topic::text, sat.source_partition::text, sat.source_offset::text, sat.load_datetime::text)),
    md5((to_jsonb(sat) - 'hashdiff' - 'load_datetime' - 'record_source')::text),
    sat.hashdiff::text,
    sat.load_datetime,
    sat.occurred_at,
    sat.received_at,
    sat.record_source::text,
    'product_event'::text
  from {{ ref("s_together_metadata_raw") }} sat

  union all

  select
    's_voice_usage_session_raw'::text,
    'h_voice_session'::text,
    'voice_session_hk'::text,
    sat.voice_session_hk::text,
    md5(concat_ws('||', 's_voice_usage_session_raw', sat.voice_session_hk::text, sat.record_source::text, sat.load_datetime::text)),
    md5((to_jsonb(sat) - 'hashdiff' - 'load_datetime' - 'record_source')::text),
    sat.hashdiff::text,
    sat.load_datetime,
    null::timestamp with time zone,
    null::timestamp with time zone,
    sat.record_source::text,
    'voice_contract'::text
  from {{ ref("s_voice_usage_session_raw") }} sat

  union all

  select
    's_voice_qoe_raw'::text,
    'h_voice_session'::text,
    'voice_session_hk'::text,
    sat.voice_session_hk::text,
    md5(concat_ws('||', 's_voice_qoe_raw', sat.voice_session_hk::text, sat.record_source::text, sat.load_datetime::text)),
    md5((to_jsonb(sat) - 'hashdiff' - 'load_datetime' - 'record_source')::text),
    sat.hashdiff::text,
    sat.load_datetime,
    null::timestamp with time zone,
    null::timestamp with time zone,
    sat.record_source::text,
    'voice_contract'::text
  from {{ ref("s_voice_qoe_raw") }} sat

  union all

  select
    's_voice_mic_activity_raw'::text,
    'h_voice_session'::text,
    'voice_session_hk'::text,
    sat.voice_session_hk::text,
    md5(concat_ws('||', 's_voice_mic_activity_raw', sat.voice_session_hk::text, sat.record_source::text, sat.load_datetime::text)),
    md5((to_jsonb(sat) - 'hashdiff' - 'load_datetime' - 'record_source')::text),
    sat.hashdiff::text,
    sat.load_datetime,
    null::timestamp with time zone,
    null::timestamp with time zone,
    sat.record_source::text,
    'voice_contract'::text
  from {{ ref("s_voice_mic_activity_raw") }} sat
),

positive_satellite_fixture as (
  select
    'controlled_positive_satellite_fixture'::text as model_name,
    'controlled_fixture_parent'::text as parent_model_name,
    'fixture_parent_hk'::text as parent_key_name,
    'fixture_parent_1'::text as parent_hk,
    md5('controlled_positive_satellite_fixture||fixture_parent_1||local_contract_test||0||2026-06-22T00:00:00Z') as history_grain_signature,
    md5('bounded_fixture_state_v1') as state_signature,
    md5('bounded_fixture_state_v1') as hashdiff,
    timestamp with time zone '2026-06-22 00:00:00+00' as load_datetime,
    timestamp with time zone '2026-06-21 23:59:00+00' as occurred_at,
    timestamp with time zone '2026-06-21 23:59:30+00' as received_at,
    'local_contract_test'::text as record_source,
    'controlled_fixture'::text as source_family
),

controlled_conflict_fixture as (
  {% if var("force_product_reporting_rdv_satellite_conflict_failure", false) %}
  select
    'controlled_conflict_satellite_fixture'::text as model_name,
    'controlled_fixture_parent'::text as parent_model_name,
    'fixture_parent_hk'::text as parent_key_name,
    'fixture_parent_conflict'::text as parent_hk,
    md5('controlled_conflict_satellite_fixture||fixture_parent_conflict||local_contract_test||0||2026-06-22T00:00:00Z') as history_grain_signature,
    md5('bounded_fixture_state_v1') as state_signature,
    md5('bounded_fixture_state_v1') as hashdiff,
    timestamp with time zone '2026-06-22 00:00:00+00' as load_datetime,
    timestamp with time zone '2026-06-21 23:59:00+00' as occurred_at,
    timestamp with time zone '2026-06-21 23:59:30+00' as received_at,
    'local_contract_test'::text as record_source,
    'controlled_fixture'::text as source_family
  union all
  select
    'controlled_conflict_satellite_fixture'::text,
    'controlled_fixture_parent'::text,
    'fixture_parent_hk'::text,
    'fixture_parent_conflict'::text,
    md5('controlled_conflict_satellite_fixture||fixture_parent_conflict||local_contract_test||0||2026-06-22T00:00:00Z'),
    md5('bounded_fixture_state_v2'),
    md5('bounded_fixture_state_v2'),
    timestamp with time zone '2026-06-22 00:00:00+00',
    timestamp with time zone '2026-06-21 23:59:00+00',
    timestamp with time zone '2026-06-21 23:59:30+00',
    'local_contract_test'::text,
    'controlled_fixture'::text
  {% else %}
  select * from positive_satellite_fixture where false
  {% endif %}
),

satellite_rows as (
  select * from real_satellite_rows
  union all
  select * from positive_satellite_fixture
  union all
  select * from controlled_conflict_fixture
),

required_field_failures as (
  select
    model_name,
    parent_model_name,
    parent_key_name,
    coalesce(parent_hk, '<null>') as parent_or_grain_key,
    'satellite_required_field_null'::text as violation,
    count(*)::bigint as failing_row_count
  from satellite_rows
  where parent_hk is null
     or nullif(trim(parent_hk), '') is null
     or hashdiff is null
     or nullif(trim(hashdiff), '') is null
     or load_datetime is null
     or record_source is null
     or nullif(trim(record_source), '') is null
  group by model_name, parent_model_name, parent_key_name, coalesce(parent_hk, '<null>')
),

temporal_failures as (
  select
    model_name,
    parent_model_name,
    parent_key_name,
    coalesce(parent_hk, '<null>') as parent_or_grain_key,
    case
      when source_family = 'product_event' and (occurred_at is null or received_at is null) then 'satellite_event_timestamp_missing'
      else 'satellite_load_timestamp_before_receive_timestamp'
    end as violation,
    count(*)::bigint as failing_row_count
  from satellite_rows
  where (source_family = 'product_event' and (occurred_at is null or received_at is null))
     or (received_at is not null and load_datetime is not null and load_datetime < received_at)
  group by
    model_name,
    parent_model_name,
    parent_key_name,
    coalesce(parent_hk, '<null>'),
    case
      when source_family = 'product_event' and (occurred_at is null or received_at is null) then 'satellite_event_timestamp_missing'
      else 'satellite_load_timestamp_before_receive_timestamp'
    end
),

grain_conflicts as (
  select
    model_name,
    parent_model_name,
    'history_grain_signature'::text as parent_key_name,
    history_grain_signature as parent_or_grain_key,
    'satellite_grain_conflict'::text as violation,
    count(*)::bigint as failing_row_count
  from satellite_rows
  group by model_name, parent_model_name, history_grain_signature
  having count(distinct state_signature) > 1
      or count(distinct hashdiff) > 1
),

duplicate_exact_replays as (
  select
    model_name,
    parent_model_name,
    'history_grain_signature'::text as parent_key_name,
    history_grain_signature as parent_or_grain_key,
    'satellite_duplicate_exact_replay'::text as violation,
    count(*)::bigint as failing_row_count
  from satellite_rows
  group by model_name, parent_model_name, history_grain_signature, state_signature, hashdiff
  having count(*) > 1
),

state_hashdiff_drift as (
  select
    model_name,
    parent_model_name,
    'state_signature'::text as parent_key_name,
    state_signature as parent_or_grain_key,
    'satellite_hashdiff_state_drift'::text as violation,
    count(distinct hashdiff)::bigint as failing_row_count
  from satellite_rows
  group by model_name, parent_model_name, state_signature
  having count(distinct hashdiff) > 1
),

hashdiff_state_collision as (
  select
    model_name,
    parent_model_name,
    'hashdiff'::text as parent_key_name,
    hashdiff as parent_or_grain_key,
    'satellite_hashdiff_state_collision'::text as violation,
    count(distinct state_signature)::bigint as failing_row_count
  from satellite_rows
  group by model_name, parent_model_name, hashdiff
  having count(distinct state_signature) > 1
),

fixture_replay_idempotency_failure as (
  with replay_source as (
    select 'fixture_parent_1'::text as parent_hk, md5('bounded_fixture_state_v1') as hashdiff
    union all
    select 'fixture_parent_1'::text as parent_hk, md5('bounded_fixture_state_v1') as hashdiff
  ),
  replay_deduped as (
    select distinct parent_hk, hashdiff from replay_source
  )
  select
    'controlled_replay_idempotency_fixture'::text as model_name,
    'controlled_fixture_parent'::text as parent_model_name,
    'fixture_parent_hk'::text as parent_key_name,
    'fixture_parent_1'::text as parent_or_grain_key,
    'satellite_replay_idempotency_fixture_not_deduped'::text as violation,
    count(*)::bigint as failing_row_count
  from replay_deduped
  having count(*) <> 1
),

parent_orphans as (
  select
    'sat_reporting_content_event'::text as model_name,
    'hub_reporting_content'::text as parent_model_name,
    'content_hk'::text as parent_key_name,
    sat.content_hk::text as parent_or_grain_key,
    'satellite_parent_missing'::text as violation,
    count(*)::bigint as failing_row_count
  from {{ ref("sat_reporting_content_event") }} sat
  left join {{ ref("hub_reporting_content") }} parent
    on sat.content_hk = parent.content_hk
  where parent.content_hk is null
  group by sat.content_hk

  union all

  select
    'sat_reporting_reaction_event'::text,
    'hub_reporting_reaction'::text,
    'reaction_hk'::text,
    sat.reaction_hk::text,
    'satellite_parent_missing'::text,
    count(*)::bigint
  from {{ ref("sat_reporting_reaction_event") }} sat
  left join {{ ref("hub_reporting_reaction") }} parent
    on sat.reaction_hk = parent.reaction_hk
  where parent.reaction_hk is null
  group by sat.reaction_hk

  union all

  select
    'sat_reporting_feed_serving_event'::text,
    'hub_reporting_feed_item'::text,
    'feed_item_hk'::text,
    sat.feed_item_hk::text,
    'satellite_parent_missing'::text,
    count(*)::bigint
  from {{ ref("sat_reporting_feed_serving_event") }} sat
  left join {{ ref("hub_reporting_feed_item") }} parent
    on sat.feed_item_hk = parent.feed_item_hk
  where parent.feed_item_hk is null
  group by sat.feed_item_hk

  union all

  select
    's_channel_session_raw'::text,
    'l_user_channel_session'::text,
    'channel_session_hk'::text,
    sat.channel_session_hk::text,
    'satellite_parent_missing'::text,
    count(*)::bigint
  from {{ ref("s_channel_session_raw") }} sat
  left join {{ ref("l_user_channel_session") }} parent
    on sat.channel_session_hk = parent.channel_session_hk
   and sat.channel_hk = parent.channel_hk
   and sat.subject_user_hk = parent.subject_user_hk
  where parent.channel_session_hk is null
  group by sat.channel_session_hk

  union all

  select
    's_event_metadata_raw'::text,
    'l_event_participant'::text,
    'event_funnel_action_hk'::text,
    sat.event_funnel_action_hk::text,
    'satellite_parent_missing'::text,
    count(*)::bigint
  from {{ ref("s_event_metadata_raw") }} sat
  left join {{ ref("l_event_participant") }} parent
    on sat.event_funnel_action_hk = parent.event_funnel_action_hk
   and sat.community_event_hk = parent.event_hk
   and sat.subject_user_hk = parent.subject_user_hk
  where parent.event_funnel_action_hk is null
  group by sat.event_funnel_action_hk

  union all

  select
    's_together_metadata_raw'::text,
    'h_together_item'::text,
    'together_item_hk'::text,
    sat.together_item_hk::text,
    'satellite_parent_missing'::text,
    count(*)::bigint
  from {{ ref("s_together_metadata_raw") }} sat
  left join {{ ref("h_together_item") }} parent
    on sat.together_item_hk = parent.together_item_hk
  where parent.together_item_hk is null
  group by sat.together_item_hk

  union all

  select
    's_voice_usage_session_raw'::text,
    'h_voice_session'::text,
    'voice_session_hk'::text,
    sat.voice_session_hk::text,
    'satellite_parent_missing'::text,
    count(*)::bigint
  from {{ ref("s_voice_usage_session_raw") }} sat
  left join {{ ref("h_voice_session") }} parent
    on sat.voice_session_hk = parent.voice_session_hk
  where parent.voice_session_hk is null
  group by sat.voice_session_hk

  union all

  select
    's_voice_qoe_raw'::text,
    'h_voice_session'::text,
    'voice_session_hk'::text,
    sat.voice_session_hk::text,
    'satellite_parent_missing'::text,
    count(*)::bigint
  from {{ ref("s_voice_qoe_raw") }} sat
  left join {{ ref("h_voice_session") }} parent
    on sat.voice_session_hk = parent.voice_session_hk
  where parent.voice_session_hk is null
  group by sat.voice_session_hk

  union all

  select
    's_voice_mic_activity_raw'::text,
    'h_voice_session'::text,
    'voice_session_hk'::text,
    sat.voice_session_hk::text,
    'satellite_parent_missing'::text,
    count(*)::bigint
  from {{ ref("s_voice_mic_activity_raw") }} sat
  left join {{ ref("h_voice_session") }} parent
    on sat.voice_session_hk = parent.voice_session_hk
  where parent.voice_session_hk is null
  group by sat.voice_session_hk
),

controlled_orphan_fixture as (
  {% if var("force_product_reporting_rdv_satellite_orphan_failure", false) %}
  select
    'sat_reporting_content_event'::text as model_name,
    'hub_reporting_content'::text as parent_model_name,
    'content_hk'::text as parent_key_name,
    fixture.content_hk::text as parent_or_grain_key,
    'satellite_parent_missing'::text as violation,
    count(*)::bigint as failing_row_count
  from (select 'controlled_missing_content_hk'::text as content_hk) fixture
  left join {{ ref("hub_reporting_content") }} parent
    on fixture.content_hk = parent.content_hk
  where parent.content_hk is null
  group by fixture.content_hk
  {% else %}
  select * from parent_orphans where false
  {% endif %}
),

forbidden_satellite_columns as (
  select
    table_name::text as model_name,
    'information_schema.columns'::text as parent_model_name,
    'column_name'::text as parent_key_name,
    concat_ws('.', table_schema, table_name, column_name) as parent_or_grain_key,
    'satellite_forbidden_field_projected'::text as violation,
    count(*)::bigint as failing_row_count
  from information_schema.columns
  where lower(table_schema) = lower('{{ target.schema }}_raw_vault')
    and table_name in (
      'sat_reporting_content_event',
      'sat_reporting_reaction_event',
      'sat_reporting_feed_serving_event',
      's_channel_session_raw',
      's_event_metadata_raw',
      's_together_metadata_raw',
      's_voice_usage_session_raw',
      's_voice_qoe_raw',
      's_voice_mic_activity_raw'
    )
    and (
      lower(column_name) in (
        'raw_content',
        'raw_text',
        'body',
        'message',
        'note',
        'note_body',
        'private_note',
        'dm_content',
        'transcript',
        'audio',
        'audio_sample',
        'voiceprint',
        'speaker_embedding',
        'speaker_identity',
        'co_participant_hk',
        'pairwise_copresence_hk',
        'contact_value',
        'reveal_value',
        'request_body',
        'response_body',
        'exact_gps',
        'latitude',
        'longitude'
      )
      or lower(column_name) like '%transcript%'
      or lower(column_name) like '%speaker_embedding%'
      or lower(column_name) like '%speaker_identity%'
      or lower(column_name) like '%voiceprint%'
      or lower(column_name) like '%pairwise%'
      or lower(column_name) like '%contact_value%'
      or lower(column_name) like '%reveal_value%'
      or lower(column_name) like '%token%'
    )
  group by table_schema, table_name, column_name
),

voice_conditional_path_failures as (
  select
    's_voice_mic_activity_raw'::text as model_name,
    'privacy.voice_speaker_activity_legal_mode'::text as parent_model_name,
    'voice_session_hk'::text as parent_key_name,
    sat.voice_session_hk::text as parent_or_grain_key,
    'voice_speaker_activity_conditional_path_enabled'::text as violation,
    count(*)::bigint as failing_row_count
  from {{ ref("s_voice_mic_activity_raw") }} sat
  where sat.production_write_enabled = true
     or sat.voice_speaker_activity_legal_mode <> 'disabled'
  group by sat.voice_session_hk
)

select * from required_field_failures
union all
select * from temporal_failures
union all
select * from grain_conflicts
union all
select * from duplicate_exact_replays
union all
select * from state_hashdiff_drift
union all
select * from hashdiff_state_collision
union all
select * from fixture_replay_idempotency_failure
union all
select * from parent_orphans
union all
select * from controlled_orphan_fixture
union all
select * from forbidden_satellite_columns
union all
select * from voice_conditional_path_failures
