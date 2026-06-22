{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

with reconciliation_failures as (
  select
    source_model,
    reporting_date,
    accepted_source_count,
    accepted_source_distinct_event_count,
    row_count,
    distinct_event_count,
    expected_excluded_count,
    expected_rejected_count,
    expected_dlq_count,
    expected_deduplicated_replay_count,
    expected_suppression_count,
    missing_business_key_count,
    unexplained_delta,
    reconciliation_status
  from {{ ref("product_reporting_stage_reconciliation") }}
  where row_count < 0
     or distinct_event_count < 0
     or accepted_source_count < 0
     or accepted_source_distinct_event_count < 0
     or expected_excluded_count < 0
     or expected_rejected_count < 0
     or expected_dlq_count < 0
     or expected_deduplicated_replay_count < 0
     or expected_suppression_count < 0
     or missing_business_key_count < 0
     or distinct_event_count > row_count
     or accepted_source_distinct_event_count > accepted_source_count
     or missing_business_key_count > row_count
     or accepted_source_count <> (
       row_count
       + expected_excluded_count
       + expected_rejected_count
       + expected_dlq_count
       + expected_deduplicated_replay_count
       + expected_suppression_count
       + unexplained_delta
     )
     or unexplained_delta <> 0
     or reconciliation_status <> 'explained'
),

raw_landing_versions as (
  select
    event_id::text as event_id,
    raw_record_sha256::text as raw_record_sha256,
    occurred_at
  from {{ source("analytics", "raw_event_landing") }}
  where event_id is not null
    and raw_record_sha256 is not null

  union all

  select
    'controlled_replay_same_hash'::text as event_id,
    'controlled_raw_record_hash'::text as raw_record_sha256,
    timestamp with time zone '2026-06-22 12:00:00+00' as occurred_at

  union all

  select
    'controlled_replay_same_hash'::text as event_id,
    'controlled_raw_record_hash'::text as raw_record_sha256,
    timestamp with time zone '2026-06-22 12:01:00+00' as occurred_at

  {% if var("force_product_reporting_landing_contradiction_failure", false) %}
  union all

  select
    'controlled_conflicting_event_id'::text as event_id,
    'controlled_raw_record_hash_a'::text as raw_record_sha256,
    timestamp with time zone '2026-06-22 12:00:00+00' as occurred_at

  union all

  select
    'controlled_conflicting_event_id'::text as event_id,
    'controlled_raw_record_hash_b'::text as raw_record_sha256,
    timestamp with time zone '2026-06-22 12:01:00+00' as occurred_at
  {% endif %}
),

event_id_contradiction_failures as (
  select
    'analytics.raw_event_landing'::text as source_model,
    (min(occurred_at) at time zone 'Europe/Istanbul')::date as reporting_date,
    count(*)::bigint as accepted_source_count,
    count(distinct event_id)::bigint as accepted_source_distinct_event_count,
    count(*)::bigint as row_count,
    count(distinct event_id)::bigint as distinct_event_count,
    0::bigint as expected_excluded_count,
    0::bigint as expected_rejected_count,
    0::bigint as expected_dlq_count,
    0::bigint as expected_deduplicated_replay_count,
    0::bigint as expected_suppression_count,
    0::bigint as missing_business_key_count,
    count(distinct raw_record_sha256)::bigint as unexplained_delta,
    'event_id_raw_hash_contradiction'::text as reconciliation_status
  from raw_landing_versions
  group by event_id
  having count(distinct raw_record_sha256) > 1
)

select * from reconciliation_failures
union all
select * from event_id_contradiction_failures
