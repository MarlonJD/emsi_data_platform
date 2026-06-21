{{ config(materialized="view", tags=["product_reporting_phase1", "product_reporting_stage"]) }}

with source as (
  select
    *
  from {{ ref("stg_product_reporting_content_events") }}
  where event_name in ('reaction_added', 'reaction_removed', 'feed_item_like')
)

select
  event_id,
  event_hk,
  md5(
    concat_ws(
      '||',
      event_id,
      content_business_key,
      coalesce(emoji_key, ''),
      coalesce(feed_kind, event_name),
      coalesce(payload_sha256, '')
    )
  ) as reaction_hk,
  content_hk,
  content_business_key,
  content_type,
  channel_hk,
  channel_business_key,
  reporting_date,
  case
    when event_name = 'reaction_removed' then 'removed'
    else 'added'
  end as reaction_action,
  coalesce(feed_kind, event_name) as reaction_kind,
  coalesce(emoji_key, nullif(regexp_replace(coalesce(feed_kind, event_name), '^feed_item_', ''), '')) as reaction_key,
  emoji_key,
  emoji_key as emoji_business_key,
  reaction_valence,
  occupation_cohort_key,
  event_name,
  event_version,
  occurred_at,
  received_at,
  producer,
  privacy_class,
  consent_scope,
  subject_user_hash,
  subject_user_hk,
  subject_session_id,
  surface,
  action_source,
  feed_mode,
  reason_key,
  source_topic,
  source_partition,
  source_offset,
  raw_record_sha256,
  payload_sha256,
  landed_at,
  load_datetime,
  record_source,
  md5(
    concat_ws(
      '||',
      event_name,
      event_version::text,
      occurred_at::text,
      received_at::text,
      content_business_key,
      coalesce(occupation_cohort_key, ''),
      coalesce(emoji_key, ''),
      coalesce(reaction_valence, ''),
      coalesce(feed_kind, event_name),
      coalesce(surface, ''),
      coalesce(action_source, ''),
      coalesce(feed_mode, ''),
      payload_sha256,
      raw_record_sha256
    )
  ) as reaction_hashdiff
from source
