{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

with real_trust_rows as (
  select
    asset_key,
    partition_key,
    reporting_date,
    freshness_status,
    freshness_reason_code,
    completeness_status,
    completeness_reason_code,
    observed_row_count,
    observed_distinct_key_count,
    expected_target_min_row_count,
    expected_minimum_state,
    late_arrival_state,
    trust_status,
    trust_reason_code,
    asset_required_for_publication
  from {{ ref("product_reporting_partition_trust_state") }}
),

synthetic_trust_rows as (
  select
    'quality_fixture.fresh_complete_partition'::text as asset_key,
    '2026-06-22'::text as partition_key,
    date '2026-06-22' as reporting_date,
    'FRESH'::text as freshness_status,
    'within_target'::text as freshness_reason_code,
    'COMPLETE'::text as completeness_status,
    'partition_structurally_complete'::text as completeness_reason_code,
    3::bigint as observed_row_count,
    3::bigint as observed_distinct_key_count,
    3::bigint as expected_target_min_row_count,
    'source_rows_required'::text as expected_minimum_state,
    'closed_partition'::text as late_arrival_state,
    'TRUSTED'::text as trust_status,
    'fresh_and_complete'::text as trust_reason_code,
    true as asset_required_for_publication

  union all

  select
    'quality_fixture.expected_empty_day'::text,
    '2026-06-21'::text,
    date '2026-06-21',
    'UNAVAILABLE'::text,
    'no_freshness_timestamp'::text,
    'EXPECTED_EMPTY'::text,
    'expected_empty_proven_by_source_observation'::text,
    0::bigint,
    0::bigint,
    0::bigint,
    'no_activity_partition'::text,
    'closed_partition'::text,
    'EXPECTED_EMPTY'::text,
    'expected_empty_partition'::text,
    true

  union all

  select
    'quality_fixture.late_arrival_open_partition'::text,
    '2026-06-22'::text,
    date '2026-06-22',
    'FRESH'::text,
    'within_target'::text,
    'LATE_OPEN'::text,
    'partition_inside_late_arrival_window'::text,
    0::bigint,
    0::bigint,
    2::bigint,
    'source_rows_required'::text,
    'inside_late_arrival_window'::text,
    'LATE_OPEN'::text,
    'late_arrival_window_open'::text,
    true

  {% if var("force_product_reporting_partition_trust_stale_failure", false) %}
  union all

  select
    'quality_fixture.stale_complete_partition'::text,
    '2026-06-20'::text,
    date '2026-06-20',
    'STALE'::text,
    'freshness_target_exceeded'::text,
    'COMPLETE'::text,
    'partition_structurally_complete'::text,
    1::bigint,
    1::bigint,
    1::bigint,
    'source_rows_required'::text,
    'closed_partition'::text,
    'COMPLETE_BUT_STALE'::text,
    'complete_partition_stale_asset'::text,
    true
  {% endif %}

  {% if var("force_product_reporting_partition_trust_recent_incomplete_failure", false) %}
  union all

  select
    'quality_fixture.recent_row_incomplete_expected_partition'::text,
    '2026-06-20'::text,
    date '2026-06-20',
    'FRESH'::text,
    'within_target'::text,
    'PARTIAL'::text,
    'observed_rows_below_contract_minimum'::text,
    1::bigint,
    1::bigint,
    3::bigint,
    'source_rows_required'::text,
    'closed_partition'::text,
    'FRESH_BUT_PARTIAL'::text,
    'fresh_asset_incomplete_partition'::text,
    true
  {% endif %}

  {% if var("force_product_reporting_partition_trust_missing_required_partition_failure", false) %}
  union all

  select
    'mart.mart_product_reporting_together_coordination_daily'::text,
    '2026-06-20'::text,
    date '2026-06-20',
    'FRESH'::text,
    'within_target'::text,
    'MISSING'::text,
    'required_partition_missing'::text,
    0::bigint,
    0::bigint,
    1::bigint,
    'reportable_rows_from_bdv'::text,
    'closed_partition'::text,
    'FRESH_BUT_PARTIAL'::text,
    'fresh_asset_incomplete_partition'::text,
    true
  {% endif %}
),

trust_rows as (
  select * from real_trust_rows
  union all
  select * from synthetic_trust_rows
),

invalid_status_values as (
  select
    asset_key,
    partition_key,
    reporting_date,
    freshness_status,
    completeness_status,
    trust_status,
    'invalid_status_value'::text as violation,
    trust_reason_code
  from trust_rows
  where freshness_status not in ('FRESH', 'STALE', 'CLOCK_SKEW', 'UNAVAILABLE')
     or completeness_status not in (
       'COMPLETE',
       'PARTIAL',
       'EXPECTED_EMPTY',
       'MISSING',
       'LATE_OPEN',
       'UNAVAILABLE_SOURCE_DEPENDENCY'
     )
     or trust_status not in (
       'TRUSTED',
       'FRESH_BUT_PARTIAL',
       'COMPLETE_BUT_STALE',
       'LATE_OPEN',
       'EXPECTED_EMPTY',
       'UNAVAILABLE',
       'FAILED'
     )
),

required_trust_failures as (
  select
    asset_key,
    partition_key,
    reporting_date,
    freshness_status,
    completeness_status,
    trust_status,
    'required_partition_not_trusted'::text as violation,
    trust_reason_code
  from trust_rows
  where asset_required_for_publication
    and trust_status in ('FRESH_BUT_PARTIAL', 'COMPLETE_BUT_STALE', 'UNAVAILABLE', 'FAILED')
),

freshness_hidden_partition_failures as (
  select
    asset_key,
    partition_key,
    reporting_date,
    freshness_status,
    completeness_status,
    trust_status,
    'freshness_hidden_missing_or_partial_partition'::text as violation,
    trust_reason_code
  from trust_rows
  where freshness_status = 'FRESH'
    and completeness_status in ('PARTIAL', 'MISSING', 'UNAVAILABLE_SOURCE_DEPENDENCY')
    and trust_status = 'TRUSTED'
),

expected_empty_failures as (
  select
    asset_key,
    partition_key,
    reporting_date,
    freshness_status,
    completeness_status,
    trust_status,
    'expected_empty_classification_failed'::text as violation,
    trust_reason_code
  from trust_rows
  where asset_key = 'quality_fixture.expected_empty_day'
    and trust_status <> 'EXPECTED_EMPTY'
),

late_open_failures as (
  select
    asset_key,
    partition_key,
    reporting_date,
    freshness_status,
    completeness_status,
    trust_status,
    'late_open_classification_failed'::text as violation,
    trust_reason_code
  from trust_rows
  where asset_key = 'quality_fixture.late_arrival_open_partition'
    and (
      trust_status <> 'LATE_OPEN'
      or completeness_status not in ('LATE_OPEN', 'PARTIAL')
    )
)

select * from invalid_status_values
union all
select * from required_trust_failures
union all
select * from freshness_hidden_partition_failures
union all
select * from expected_empty_failures
union all
select * from late_open_failures
