{{ config(materialized="view", tags=["phase_d_smoke", "analytics_events"]) }}

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
    raw_record_bytes,
    payload_sha256,
    landed_at
  from {{ source("analytics", "raw_event_landing") }}
)

select
  event_id,
  md5(event_id) as event_hk,
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
  raw_record_bytes,
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
      consent_scope::text,
      subject_user_hash,
      coalesce(subject_session_id, ''),
      payload_sha256,
      raw_record_sha256
    )
  ) as event_hashdiff
from source
