{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

with real_pit_rows as (
  select
    'pit_reporting_content_daily'::text as model_name,
    content_hk::text as content_hk,
    reporting_date::date as reporting_date,
    as_of_datetime,
    late_arriving_event_count::bigint as late_arriving_event_count,
    restatement_state::text as restatement_state,
    deleted_or_opted_out_subject_count::bigint as deleted_or_opted_out_subject_count
  from {{ ref("pit_reporting_content_daily") }}
),

positive_pit_fixture as (
  select
    'controlled_positive_pit_fixture'::text as model_name,
    'pit_fixture_content_ok'::text as content_hk,
    date '2026-06-22' as reporting_date,
    timestamp with time zone '2026-06-22 20:00:00+00' as as_of_datetime,
    0::bigint as late_arriving_event_count,
    'current_as_of_reporting_date'::text as restatement_state,
    0::bigint as deleted_or_opted_out_subject_count
),

controlled_duplicate_pit_fixture as (
  {% if var("force_product_reporting_pit_duplicate_grain_failure", false) %}
  select
    'controlled_duplicate_pit_fixture'::text as model_name,
    'pit_fixture_content_duplicate'::text as content_hk,
    date '2026-06-22' as reporting_date,
    timestamp with time zone '2026-06-22 20:00:00+00' as as_of_datetime,
    0::bigint as late_arriving_event_count,
    'current_as_of_reporting_date'::text as restatement_state,
    0::bigint as deleted_or_opted_out_subject_count
  union all
  select
    'controlled_duplicate_pit_fixture'::text,
    'pit_fixture_content_duplicate'::text,
    date '2026-06-22',
    timestamp with time zone '2026-06-22 20:00:00+00',
    0::bigint,
    'current_as_of_reporting_date'::text,
    0::bigint
  {% else %}
  select * from positive_pit_fixture where false
  {% endif %}
),

controlled_future_pit_fixture as (
  {% if var("force_product_reporting_pit_future_satellite_failure", false) %}
  select
    'controlled_future_pit_fixture'::text as model_name,
    'pit_fixture_content_future'::text as content_hk,
    date '2026-06-22' as reporting_date,
    timestamp with time zone '2026-06-22 18:00:00+00' as as_of_datetime,
    0::bigint as late_arriving_event_count,
    'current_as_of_reporting_date'::text as restatement_state,
    0::bigint as deleted_or_opted_out_subject_count
  {% else %}
  select * from positive_pit_fixture where false
  {% endif %}
),

controlled_late_pit_fixture as (
  {% if var("force_product_reporting_pit_late_arrival_failure", false) %}
  select
    'controlled_late_pit_fixture'::text as model_name,
    'pit_fixture_content_late'::text as content_hk,
    date '2026-06-22' as reporting_date,
    timestamp with time zone '2026-06-23 08:00:00+00' as as_of_datetime,
    0::bigint as late_arriving_event_count,
    'current_as_of_reporting_date'::text as restatement_state,
    0::bigint as deleted_or_opted_out_subject_count
  {% else %}
  select * from positive_pit_fixture where false
  {% endif %}
),

controlled_privacy_pit_fixture as (
  {% if var("force_product_reporting_pit_privacy_reappearance_failure", false) %}
  select
    'controlled_privacy_pit_fixture'::text as model_name,
    'pit_fixture_content_privacy'::text as content_hk,
    date '2026-06-22' as reporting_date,
    timestamp with time zone '2026-06-22 20:00:00+00' as as_of_datetime,
    0::bigint as late_arriving_event_count,
    'current_as_of_reporting_date'::text as restatement_state,
    0::bigint as deleted_or_opted_out_subject_count
  {% else %}
  select * from positive_pit_fixture where false
  {% endif %}
),

pit_rows as (
  select * from real_pit_rows
  union all
  select * from positive_pit_fixture
  union all
  select * from controlled_duplicate_pit_fixture
  union all
  select * from controlled_future_pit_fixture
  union all
  select * from controlled_late_pit_fixture
  union all
  select * from controlled_privacy_pit_fixture
),

real_satellite_versions as (
  select
    'sat_reporting_content_event'::text as model_name,
    content_hk::text as content_hk,
    reporting_date::date as reporting_date,
    load_datetime,
    occurred_at,
    consent_scope::text as consent_scope_text
  from {{ ref("sat_reporting_content_event") }}
  where content_hk is not null
),

positive_satellite_fixture as (
  select
    'controlled_positive_satellite_fixture'::text as model_name,
    'pit_fixture_content_ok'::text as content_hk,
    date '2026-06-22' as reporting_date,
    timestamp with time zone '2026-06-22 20:00:00+00' as load_datetime,
    timestamp with time zone '2026-06-22 18:00:00+00' as occurred_at,
    '["analytics"]'::text as consent_scope_text
),

controlled_future_satellite_fixture as (
  {% if var("force_product_reporting_pit_future_satellite_failure", false) %}
  select
    'controlled_future_satellite_fixture'::text as model_name,
    'pit_fixture_content_future'::text as content_hk,
    date '2026-06-22' as reporting_date,
    timestamp with time zone '2026-06-22 20:00:00+00' as load_datetime,
    timestamp with time zone '2026-06-22 17:00:00+00' as occurred_at,
    '["analytics"]'::text as consent_scope_text
  {% else %}
  select * from positive_satellite_fixture where false
  {% endif %}
),

controlled_late_satellite_fixture as (
  {% if var("force_product_reporting_pit_late_arrival_failure", false) %}
  select
    'controlled_late_satellite_fixture'::text as model_name,
    'pit_fixture_content_late'::text as content_hk,
    date '2026-06-22' as reporting_date,
    timestamp with time zone '2026-06-23 08:00:00+00' as load_datetime,
    timestamp with time zone '2026-06-22 18:00:00+00' as occurred_at,
    '["analytics"]'::text as consent_scope_text
  {% else %}
  select * from positive_satellite_fixture where false
  {% endif %}
),

controlled_privacy_satellite_fixture as (
  {% if var("force_product_reporting_pit_privacy_reappearance_failure", false) %}
  select
    'controlled_privacy_satellite_fixture'::text as model_name,
    'pit_fixture_content_privacy'::text as content_hk,
    date '2026-06-22' as reporting_date,
    timestamp with time zone '2026-06-22 20:00:00+00' as load_datetime,
    timestamp with time zone '2026-06-22 18:00:00+00' as occurred_at,
    '["analytics_opt_out"]'::text as consent_scope_text
  {% else %}
  select * from positive_satellite_fixture where false
  {% endif %}
),

satellite_versions as (
  select * from real_satellite_versions
  union all
  select * from positive_satellite_fixture
  union all
  select * from controlled_future_satellite_fixture
  union all
  select * from controlled_late_satellite_fixture
  union all
  select * from controlled_privacy_satellite_fixture
),

duplicate_grain_failures as (
  select
    'pit_reporting_content_daily'::text as model_name,
    concat_ws('||', content_hk, reporting_date::text) as grain_key,
    'pit_duplicate_daily_grain'::text as violation,
    count(*)::bigint as failing_row_count
  from pit_rows
  group by content_hk, reporting_date
  having count(*) > 1
),

required_field_failures as (
  select
    model_name,
    concat_ws('||', coalesce(content_hk, '<null>'), coalesce(reporting_date::text, '<null>')) as grain_key,
    'pit_required_field_null'::text as violation,
    count(*)::bigint as failing_row_count
  from pit_rows
  where content_hk is null
     or nullif(trim(content_hk), '') is null
     or reporting_date is null
     or as_of_datetime is null
     or late_arriving_event_count is null
     or restatement_state is null
     or deleted_or_opted_out_subject_count is null
  group by model_name, content_hk, reporting_date
),

satellite_future_leakage_failures as (
  select
    pit.model_name,
    concat_ws('||', pit.content_hk, pit.reporting_date::text) as grain_key,
    'pit_selected_satellite_after_as_of'::text as violation,
    count(*)::bigint as failing_row_count
  from pit_rows pit
  join satellite_versions sat
    on sat.content_hk = pit.content_hk
   and sat.reporting_date = pit.reporting_date
  where sat.load_datetime > pit.as_of_datetime
     or (sat.occurred_at at time zone 'Europe/Istanbul')::date <> pit.reporting_date
  group by pit.model_name, pit.content_hk, pit.reporting_date
),

recomputed_satellite_state as (
  select
    pit.model_name,
    pit.content_hk,
    pit.reporting_date,
    pit.late_arriving_event_count,
    pit.restatement_state,
    pit.deleted_or_opted_out_subject_count,
    coalesce(
      count(*) filter (
        where (sat.load_datetime at time zone 'Europe/Istanbul')::date > pit.reporting_date
      ),
      0
    )::bigint as expected_late_arriving_event_count,
    coalesce(
      count(*) filter (
        where sat.content_hk is not null
          and (
            lower(coalesce(sat.consent_scope_text, '')) not like '%"analytics"%'
            or lower(coalesce(sat.consent_scope_text, '')) like '%analytics_opt_out%'
            or lower(coalesce(sat.consent_scope_text, '')) like '%account_deletion%'
            or lower(coalesce(sat.consent_scope_text, '')) like '%privacy_deletion%'
            or lower(coalesce(sat.consent_scope_text, '')) like '%deleted%'
            or lower(coalesce(sat.consent_scope_text, '')) like '%opted_out%'
            or lower(coalesce(sat.consent_scope_text, '')) like '%opt_out%'
          )
      ),
      0
    )::bigint as expected_deleted_or_opted_out_subject_count
  from pit_rows pit
  left join satellite_versions sat
    on sat.content_hk = pit.content_hk
   and sat.reporting_date = pit.reporting_date
  group by
    pit.model_name,
    pit.content_hk,
    pit.reporting_date,
    pit.late_arriving_event_count,
    pit.restatement_state,
    pit.deleted_or_opted_out_subject_count
),

late_arrival_state_failures as (
  select
    model_name,
    concat_ws('||', content_hk, reporting_date::text) as grain_key,
    'pit_late_arrival_not_surfaced_or_blocked'::text as violation,
    greatest(
      abs(late_arriving_event_count - expected_late_arriving_event_count),
      1
    )::bigint as failing_row_count
  from recomputed_satellite_state
  where late_arriving_event_count <> expected_late_arriving_event_count
     or (
       expected_late_arriving_event_count > 0
       and restatement_state <> 'restated_after_reporting_date'
     )
     or (
       expected_late_arriving_event_count = 0
       and restatement_state <> 'current_as_of_reporting_date'
     )
),

privacy_reappearance_failures as (
  select
    model_name,
    concat_ws('||', content_hk, reporting_date::text) as grain_key,
    'pit_deleted_or_opted_out_subject_reappeared'::text as violation,
    greatest(
      expected_deleted_or_opted_out_subject_count,
      deleted_or_opted_out_subject_count,
      1
    )::bigint as failing_row_count
  from recomputed_satellite_state
  where expected_deleted_or_opted_out_subject_count <> 0
     or deleted_or_opted_out_subject_count <> 0
)

select * from duplicate_grain_failures
union all
select * from required_field_failures
union all
select * from satellite_future_leakage_failures
union all
select * from late_arrival_state_failures
union all
select * from privacy_reappearance_failures
