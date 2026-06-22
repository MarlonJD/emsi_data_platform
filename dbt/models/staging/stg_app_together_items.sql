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
  where event_name like 'together_%'
)

select
  event_id,
  md5(event_id) as event_hk,
  md5(
    coalesce(
      payload ->> 'together_item_id',
      payload ->> 'item_id',
      payload ->> 'post_id',
      payload ->> 'activity_id',
      event_id
    )
  ) as together_item_hk,
  coalesce(
    payload ->> 'together_item_id',
    payload ->> 'item_id',
    payload ->> 'post_id',
    payload ->> 'activity_id',
    event_id
  ) as together_item_business_key,
  md5(
    coalesce(
      payload ->> 'target_id',
      payload ->> 'linked_event_id',
      payload ->> 'channel_id',
      payload ->> 'post_id',
      payload ->> 'activity_id',
      payload ->> 'together_item_id',
      event_id
    )
  ) as together_target_hk,
  coalesce(
    payload ->> 'target_id',
    payload ->> 'linked_event_id',
    payload ->> 'channel_id',
    payload ->> 'post_id',
    payload ->> 'activity_id',
    payload ->> 'together_item_id',
    event_id
  ) as together_target_business_key,
  md5(coalesce(payload ->> 'linked_event_id', payload ->> 'event_id')) as linked_event_hk,
  coalesce(payload ->> 'linked_event_id', payload ->> 'event_id') as linked_event_business_key,
  md5(nullif(payload ->> 'channel_id', '')) as channel_hk,
  nullif(payload ->> 'channel_id', '') as channel_business_key,
  regexp_replace(event_name, '^together_', '') as together_action,
  payload ->> 'activity_type' as activity_type,
  payload ->> 'visibility' as visibility,
  payload ->> 'status' as together_status,
  payload ->> 'source' as action_source,
  payload ->> 'response_type' as response_type,
  payload ->> 'report_category' as report_category,
  case
    when nullif(payload ->> 'invite_count', '') ~ '^[0-9]+$' then (payload ->> 'invite_count')::integer
  end as invite_count,
  case
    when nullif(payload ->> 'response_count', '') ~ '^[0-9]+$' then (payload ->> 'response_count')::integer
  end as response_count,
  payload ->> 'response_latency_bucket' as response_latency_bucket,
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
      coalesce(payload ->> 'together_item_id', ''),
      coalesce(payload ->> 'activity_type', ''),
      coalesce(payload ->> 'visibility', ''),
      coalesce(payload ->> 'status', ''),
      coalesce(payload ->> 'source', ''),
      coalesce(payload ->> 'response_type', ''),
      coalesce(payload ->> 'report_category', ''),
      payload_sha256,
      raw_record_sha256
    )
  ) as together_metadata_hashdiff,
  case
    when event_name in ('together_created', 'together_response_added')
      and (payload ? 'activity_type' or payload ? 'status')
      then 'source_complete'
    else 'partial'
  end as source_completeness_input
from source
