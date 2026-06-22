{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

{% if var("force_product_reporting_stage_reconciliation_negative_failure", false) %}

select
  'forced_negative_fixture_failure'::text as violation,
  1::bigint as violation_count

{% else %}

with negative_fixture as (
  select
    'negative_fixture'::text as source_model,
    date '2026-06-22' as reporting_date,
    3::bigint as accepted_source_count,
    3::bigint as accepted_source_distinct_event_count,
    1::bigint as row_count,
    1::bigint as distinct_event_count,
    1::bigint as expected_excluded_count,
    0::bigint as expected_rejected_count,
    0::bigint as expected_dlq_count,
    0::bigint as expected_deduplicated_replay_count,
    0::bigint as expected_suppression_count,
    0::bigint as missing_business_key_count,
    1::bigint as unexplained_delta,
    'unexplained_delta'::text as reconciliation_status
),

detected as (
  select count(*)::bigint as violation_count
  from negative_fixture
  where accepted_source_count <> (
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
)

select
  'negative_fixture_not_detected'::text as violation,
  violation_count
from detected
where violation_count <> 1

{% endif %}
