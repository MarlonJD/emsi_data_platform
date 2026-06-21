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
  where event_name like 'feed_item_%'
)

select
  event_id,
  md5(event_id) as event_hk,
  md5(concat_ws(':', event_id, coalesce(payload ->> 'item_id', payload ->> 'entity_id'))) as feed_item_hk,
  md5(coalesce(payload ->> 'request_id', payload ->> 'candidate_set_id', event_id)) as feed_request_hk,
  md5(coalesce(payload ->> 'item_id', payload ->> 'entity_id')) as content_hk,
  coalesce(payload ->> 'item_id', payload ->> 'entity_id') as content_business_key,
  concat_ws(':', event_id, coalesce(payload ->> 'item_id', payload ->> 'entity_id')) as feed_item_business_key,
  payload ->> 'item_type' as item_type,
  payload ->> 'kind' as feed_kind,
  payload ->> 'feed_mode' as feed_mode,
  payload ->> 'surface' as surface,
  payload ->> 'source_bucket' as source_bucket,
  payload ->> 'reason_key' as reason_key,
  payload ->> 'candidate_set_id' as candidate_set_id,
  payload ->> 'model_version' as model_version,
  payload ->> 'feature_schema_version' as feature_schema_version,
  payload ->> 'score_bucket' as score_bucket,
  payload ->> 'fallback_reason' as fallback_reason,
  payload ->> 'rollback_state' as rollback_state,
  payload ->> 'assignment_bucket' as assignment_bucket,
  payload ->> 'holdout_bucket' as holdout_bucket,
  payload ->> 'shadow_mode' as shadow_mode,
  nullif(
    regexp_replace(
      lower(coalesce(payload ->> 'subject_occupation_cohort_key', payload ->> 'occupation_cohort_key')),
      '[^a-z0-9_:-]',
      '',
      'g'
    ),
    ''
  ) as occupation_cohort_key,
  case
    when event_name in ('feed_item_open', 'feed_item_like', 'feed_item_reply', 'feed_item_join', 'feed_item_share')
      then 'positive_interest'
    when event_name in (
      'feed_item_hide',
      'feed_item_show_less',
      'feed_item_not_interested',
      'feed_item_mute_author',
      'feed_item_mute_channel'
    )
      then 'negative_interest'
    else 'neutral_interest'
  end as interest_proxy_valence,
  case when nullif(payload ->> 'position', '') ~ '^[0-9]+$' then (payload ->> 'position')::integer end as position,
  case when nullif(payload ->> 'page_depth', '') ~ '^[0-9]+$' then (payload ->> 'page_depth')::integer end as page_depth,
  case when nullif(payload ->> 'page_size', '') ~ '^[0-9]+$' then (payload ->> 'page_size')::integer end as page_size,
  case
    when nullif(payload ->> 'visible_duration_ms', '') ~ '^[0-9]+$'
      then (payload ->> 'visible_duration_ms')::integer
  end as visible_duration_ms,
  case
    when nullif(payload ->> 'visible_percent_max', '') ~ '^[0-9]+$'
      then (payload ->> 'visible_percent_max')::integer
  end as visible_percent_max,
  case when nullif(payload ->> 'dwell_ms', '') ~ '^[0-9]+$' then (payload ->> 'dwell_ms')::integer end as dwell_ms,
  payload ->> 'action_source' as action_source,
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
      coalesce(payload ->> 'item_id', ''),
      coalesce(payload ->> 'kind', ''),
      coalesce(payload ->> 'feed_mode', ''),
      coalesce(payload ->> 'surface', ''),
      coalesce(payload ->> 'source_bucket', ''),
      coalesce(payload ->> 'reason_key', ''),
      coalesce(payload ->> 'candidate_set_id', ''),
      coalesce(payload ->> 'rollback_state', ''),
      coalesce(payload ->> 'assignment_bucket', ''),
      coalesce(payload ->> 'holdout_bucket', ''),
      coalesce(payload ->> 'shadow_mode', ''),
      coalesce(payload ->> 'subject_occupation_cohort_key', ''),
      coalesce(payload ->> 'occupation_cohort_key', ''),
      payload_sha256,
      raw_record_sha256
    )
  ) as feed_event_hashdiff,
  case
    when payload ? 'visible_duration_ms'
      and payload ? 'visible_percent_max'
      and payload ? 'dwell_ms'
      and payload ? 'source_bucket'
      and payload ? 'reason_key'
      and payload ? 'candidate_set_id'
      and (payload ? 'subject_occupation_cohort_key' or payload ? 'occupation_cohort_key')
      then 'source_complete_with_occupation_cohort'
    when payload ? 'visible_duration_ms'
      and payload ? 'visible_percent_max'
      and payload ? 'dwell_ms'
      and payload ? 'source_bucket'
      and payload ? 'reason_key'
      and payload ? 'candidate_set_id'
      then 'source_complete_candidate'
    else 'partial'
  end as source_completeness_input
from source
where coalesce(payload ->> 'item_id', payload ->> 'entity_id') is not null
