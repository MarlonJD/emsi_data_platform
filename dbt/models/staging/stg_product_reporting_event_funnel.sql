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
    'event_card_impression',
    'event_viewed',
    'event_detail_engagement',
    'event_interested',
    'event_uninterested',
    'event_join_attempted',
    'event_joined',
    'event_join_failed',
    'event_join_abandoned',
    'event_left',
    'event_cancel_reason_submitted',
    'event_reminder_set',
    'event_calendar_added',
    'event_shared',
    'event_invite_sent',
    'event_waitlist_joined',
    'event_waitlist_left',
    'event_check_in',
    'event_attendance_confirmed',
    'event_no_show_inferred'
  )
)

select
  event_id,
  md5(event_id) as event_hk,
  md5(coalesce(payload ->> 'event_id', payload ->> 'community_event_id', payload ->> 'entity_id')) as community_event_hk,
  coalesce(payload ->> 'event_id', payload ->> 'community_event_id', payload ->> 'entity_id')
    as community_event_business_key,
  md5(
    concat_ws(
      ':',
      event_id,
      coalesce(payload ->> 'event_id', payload ->> 'community_event_id', payload ->> 'entity_id'),
      event_name
    )
  ) as event_funnel_action_hk,
  regexp_replace(event_name, '^event_', '') as event_funnel_action,
  payload ->> 'surface' as surface,
  payload ->> 'source' as action_source,
  payload ->> 'request_id' as request_id,
  payload ->> 'reason_key' as reason_key,
  payload ->> 'capacity_state' as capacity_state,
  payload ->> 'previous_participation_state' as previous_participation_state,
  payload ->> 'participation_state' as participation_state,
  payload ->> 'seconds_until_start_bucket' as seconds_until_start_bucket,
  case when nullif(payload ->> 'rank', '') ~ '^[0-9]+$' then (payload ->> 'rank')::integer end as rank,
  case
    when nullif(payload ->> 'visible_pct', '') ~ '^[0-9]+$' then (payload ->> 'visible_pct')::integer
  end as visible_pct,
  case
    when nullif(payload ->> 'visible_ms', '') ~ '^[0-9]+$' then (payload ->> 'visible_ms')::integer
  end as visible_ms,
  case
    when nullif(payload ->> 'duration_ms', '') ~ '^[0-9]+$' then (payload ->> 'duration_ms')::integer
  end as duration_ms,
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
      coalesce(payload ->> 'event_id', ''),
      coalesce(payload ->> 'community_event_id', ''),
      coalesce(payload ->> 'entity_id', ''),
      coalesce(payload ->> 'surface', ''),
      coalesce(payload ->> 'source', ''),
      coalesce(payload ->> 'request_id', ''),
      coalesce(payload ->> 'reason_key', ''),
      coalesce(payload ->> 'capacity_state', ''),
      coalesce(payload ->> 'participation_state', ''),
      payload_sha256,
      raw_record_sha256
    )
  ) as event_funnel_hashdiff,
  case
    when event_name = 'event_card_impression'
      and payload ? 'visible_pct'
      and payload ? 'visible_ms'
      then 'source_complete'
    when event_name in ('event_joined', 'event_left', 'event_interested', 'event_uninterested')
      and payload ? 'source'
      then 'source_complete'
    else 'partial'
  end as source_completeness_input
from source
where coalesce(payload ->> 'event_id', payload ->> 'community_event_id', payload ->> 'entity_id') is not null
