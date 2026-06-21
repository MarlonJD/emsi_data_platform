{{ config(materialized="view", tags=["product_reporting_phase1", "product_reporting_stage"]) }}

with source as (
  select
    event_id,
    event_name,
    event_version,
    occurred_at,
    received_at,
    producer,
    privacy_class,
    consent_scope,
    subject_user_hash,
    subject_session_id,
    source_topic,
    source_partition,
    source_offset,
    raw_record_sha256,
    payload_sha256,
    landed_at,
    payload
  from {{ source("analytics", "raw_event_landing") }}
  where event_name in (
    'reaction_added',
    'reaction_removed',
    'reply_created',
    'post_impression',
    'feed_item_impression',
    'feed_item_viewable_impression',
    'feed_item_open',
    'feed_item_like',
    'feed_item_reply',
    'feed_item_join',
    'feed_item_share',
    'feed_item_copy_link',
    'feed_item_hide',
    'feed_item_show_less',
    'feed_item_not_interested',
    'feed_item_mute_author',
    'feed_item_mute_channel'
  )
)

select
  event_id,
  md5(event_id) as event_hk,
  md5(
    concat_ws(
      '||',
      coalesce(payload ->> 'post_id', ''),
      coalesce(payload ->> 'item_id', ''),
      coalesce(payload ->> 'entity_id', '')
    )
  ) as content_hk,
  coalesce(payload ->> 'post_id', payload ->> 'item_id', payload ->> 'entity_id') as content_business_key,
  coalesce(payload ->> 'item_type', payload ->> 'target_type', 'post') as content_type,
  nullif(payload ->> 'channel_id', '') as channel_business_key,
  md5(nullif(payload ->> 'channel_id', '')) as channel_hk,
  (occurred_at at time zone 'Europe/Istanbul')::date as reporting_date,
  event_name,
  event_version,
  occurred_at,
  received_at,
  producer,
  privacy_class,
  consent_scope,
  subject_user_hash,
  md5(subject_user_hash) as subject_user_hk,
  subject_session_id,
  payload ->> 'surface' as surface,
  payload ->> 'source' as action_source,
  payload ->> 'kind' as feed_kind,
  payload ->> 'feed_mode' as feed_mode,
  payload ->> 'reason_key' as reason_key,
  nullif(
    regexp_replace(
      lower(coalesce(payload ->> 'subject_occupation_cohort_key', payload ->> 'occupation_cohort_key')),
      '[^a-z0-9_:-]',
      '',
      'g'
    ),
    ''
  ) as occupation_cohort_key,
  nullif(
    regexp_replace(lower(coalesce(payload ->> 'emoji_key', payload ->> 'reaction_key')), '[^a-z0-9_:-]', '', 'g'),
    ''
  ) as emoji_key,
  case
    when event_name in ('reaction_added', 'feed_item_like', 'feed_item_reply', 'feed_item_join', 'feed_item_share')
      then 'positive'
    when event_name in (
      'reaction_removed',
      'feed_item_hide',
      'feed_item_show_less',
      'feed_item_not_interested',
      'feed_item_mute_author',
      'feed_item_mute_channel'
    )
      then 'negative'
    else 'neutral'
  end as reaction_valence,
  source_topic,
  source_partition,
  source_offset,
  raw_record_sha256,
  payload_sha256,
  landed_at,
  landed_at as load_datetime,
  'analytics.raw_event_landing'::text as record_source,
  md5(
    concat_ws(
      '||',
      event_name,
      event_version::text,
      occurred_at::text,
      received_at::text,
      producer,
      privacy_class,
      coalesce(subject_user_hash, ''),
      coalesce(subject_session_id, ''),
      coalesce(payload ->> 'surface', ''),
      coalesce(payload ->> 'source', ''),
      coalesce(payload ->> 'kind', ''),
      coalesce(payload ->> 'feed_mode', ''),
      coalesce(payload ->> 'subject_occupation_cohort_key', ''),
      coalesce(payload ->> 'occupation_cohort_key', ''),
      coalesce(payload ->> 'emoji_key', ''),
      coalesce(payload ->> 'reaction_key', ''),
      payload_sha256,
      raw_record_sha256
    )
  ) as content_event_hashdiff,
  case
    when event_name like 'feed_item_%' then 'accepted_feed_event'
    when event_name in ('reaction_added', 'reaction_removed') then 'accepted_reaction_event'
    when event_name = 'reply_created' then 'accepted_reply_event'
    when event_name = 'post_impression' then 'accepted_impression_event'
    else 'accepted_content_event'
  end as source_completeness_input
from source
where coalesce(payload ->> 'post_id', payload ->> 'item_id', payload ->> 'entity_id') is not null
