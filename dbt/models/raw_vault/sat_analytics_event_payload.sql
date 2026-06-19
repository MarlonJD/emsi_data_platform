{{ config(materialized="view", tags=["phase_d_smoke", "data_vault"]) }}

select
  event_hk,
  event_hashdiff as hashdiff,
  load_datetime,
  record_source,
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
from {{ ref("stg_analytics_events") }}
