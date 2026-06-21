{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

select
  content_hk,
  content_event_hashdiff as hashdiff,
  load_datetime,
  record_source,
  event_id,
  event_name,
  event_version,
  reporting_date,
  occurred_at,
  received_at,
  content_type,
  channel_business_key,
  privacy_class,
  consent_scope,
  surface,
  action_source,
  feed_kind,
  feed_mode,
  reason_key,
  occupation_cohort_key,
  emoji_key,
  reaction_valence,
  source_topic,
  source_partition,
  source_offset,
  raw_record_sha256,
  payload_sha256,
  source_completeness_input
from {{ ref("stg_product_reporting_content_events") }}
where content_hk is not null
