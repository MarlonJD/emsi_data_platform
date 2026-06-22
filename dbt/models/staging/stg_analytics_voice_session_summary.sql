{{ config(materialized="view", tags=["product_reporting_privacy_contract", "voice_usage_stage"]) }}

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
  where event_name = 'voice_usage_session_summary'
)

select
  event_id,
  md5(event_id) as event_hk,
  md5(payload ->> 'voice_session_key') as voice_session_hk,
  md5(payload ->> 'room_key') as voice_room_hk,
  md5(concat_ws(':', subject_user_hash, payload ->> 'voice_session_key')) as voice_participant_session_hk,
  payload ->> 'voice_session_key' as voice_session_key,
  payload ->> 'room_key' as room_key,
  payload ->> 'room_type_key' as room_type_key,
  payload ->> 'session_duration_bucket' as session_duration_bucket,
  payload ->> 'microphone_unmuted_duration_bucket' as microphone_unmuted_duration_bucket,
  payload ->> 'unmute_count_bucket' as unmute_count_bucket,
  payload ->> 'first_unmute_latency_bucket' as first_unmute_latency_bucket,
  case
    when lower(coalesce(payload ->> 'never_unmuted', 'false')) in ('true', 'false')
      then (payload ->> 'never_unmuted')::boolean
    else false
  end as never_unmuted,
  payload ->> 'permission_state' as permission_state,
  payload ->> 'speech_activity_duration_bucket' as speech_activity_duration_bucket,
  payload ->> 'speech_activity_ratio_bucket' as speech_activity_ratio_bucket,
  payload ->> 'reconnect_count_bucket' as reconnect_count_bucket,
  payload ->> 'join_failure_reason_code' as join_failure_reason_code,
  payload ->> 'rtt_bucket' as rtt_bucket,
  payload ->> 'jitter_bucket' as jitter_bucket,
  payload ->> 'packet_loss_bucket' as packet_loss_bucket,
  payload ->> 'platform' as platform,
  payload ->> 'app_version' as app_version,
  coalesce(payload ->> 'purpose_id', 'analytics.voice_usage_measurement.v1') as purpose_id,
  payload ->> 'legal_basis_id' as legal_basis_id,
  payload ->> 'notice_version' as notice_version,
  coalesce(payload ->> 'retention_policy_id', 'voice-summary-30d-anonymize-or-delete-v1') as retention_policy_id,
  coalesce(payload ->> 'record_source_mode', 'unknown') as record_source_mode,
  case
    when lower(coalesce(payload ->> 'production_write_enabled', 'false')) in ('true', 'false')
      then (payload ->> 'production_write_enabled')::boolean
    else false
  end as production_write_enabled,
  'disabled'::text as voice_speaker_activity_legal_mode,
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
      coalesce(payload ->> 'voice_session_key', ''),
      coalesce(payload ->> 'room_key', ''),
      coalesce(payload ->> 'room_type_key', ''),
      coalesce(payload ->> 'session_duration_bucket', ''),
      coalesce(payload ->> 'microphone_unmuted_duration_bucket', ''),
      coalesce(payload ->> 'unmute_count_bucket', ''),
      coalesce(payload ->> 'first_unmute_latency_bucket', ''),
      coalesce(payload ->> 'permission_state', ''),
      coalesce(payload ->> 'speech_activity_duration_bucket', ''),
      coalesce(payload ->> 'speech_activity_ratio_bucket', ''),
      coalesce(payload ->> 'reconnect_count_bucket', ''),
      coalesce(payload ->> 'join_failure_reason_code', ''),
      coalesce(payload ->> 'rtt_bucket', ''),
      coalesce(payload ->> 'jitter_bucket', ''),
      coalesce(payload ->> 'packet_loss_bucket', ''),
      payload_sha256,
      raw_record_sha256
    )
  ) as voice_session_summary_hashdiff
from source
where payload ? 'voice_session_key'
  and payload ? 'room_key'
  and payload ? 'room_type_key'
