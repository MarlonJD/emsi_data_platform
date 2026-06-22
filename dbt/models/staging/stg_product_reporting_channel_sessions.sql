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
  where event_name in ('channel_session_started', 'channel_session_ended')
)

select
  event_id,
  md5(event_id) as event_hk,
  md5(nullif(payload ->> 'channel_id', '')) as channel_hk,
  nullif(payload ->> 'channel_id', '') as channel_business_key,
  md5(
    concat_ws(
      ':',
      coalesce(subject_session_id, event_id),
      nullif(payload ->> 'channel_id', ''),
      (occurred_at at time zone 'Europe/Istanbul')::date::text
    )
  ) as channel_session_hk,
  concat_ws(
    ':',
    coalesce(subject_session_id, event_id),
    nullif(payload ->> 'channel_id', ''),
    (occurred_at at time zone 'Europe/Istanbul')::date::text
  ) as channel_session_business_key,
  case
    when event_name = 'channel_session_started' then 'started'
    when event_name = 'channel_session_ended' then 'ended'
    else 'unknown'
  end as channel_session_action,
  payload ->> 'source' as entry_source,
  payload ->> 'reason' as exit_reason,
  case
    when nullif(payload ->> 'duration_ms', '') ~ '^[0-9]+$' then (payload ->> 'duration_ms')::integer
    when nullif(payload ->> 'duration_sec', '') ~ '^[0-9]+$' then (payload ->> 'duration_sec')::integer * 1000
  end as duration_ms,
  case
    when nullif(payload ->> 'unread_count_on_enter', '') ~ '^[0-9]+$'
      then (payload ->> 'unread_count_on_enter')::integer
  end as unread_count_on_enter,
  case when nullif(payload ->> 'posts_seen', '') ~ '^[0-9]+$' then (payload ->> 'posts_seen')::integer end
    as posts_seen_count,
  case
    when nullif(payload ->> 'comments_written', '') ~ '^[0-9]+$'
      then (payload ->> 'comments_written')::integer
  end as comments_written_count,
  case
    when nullif(payload ->> 'reactions_added', '') ~ '^[0-9]+$'
      then (payload ->> 'reactions_added')::integer
  end as reactions_added_count,
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
      nullif(payload ->> 'channel_id', ''),
      coalesce(payload ->> 'source', ''),
      coalesce(payload ->> 'reason', ''),
      coalesce(payload ->> 'duration_ms', ''),
      coalesce(payload ->> 'duration_sec', ''),
      payload_sha256,
      raw_record_sha256
    )
  ) as channel_session_hashdiff,
  case
    when event_name = 'channel_session_ended'
      and (
        nullif(payload ->> 'duration_ms', '') ~ '^[0-9]+$'
        or nullif(payload ->> 'duration_sec', '') ~ '^[0-9]+$'
      )
      then 'source_complete'
    else 'partial'
  end as source_completeness_input
from source
where nullif(payload ->> 'channel_id', '') is not null
