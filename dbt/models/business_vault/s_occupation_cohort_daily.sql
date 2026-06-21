{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

with actor_cohorts as (
  select reporting_date, subject_user_hk, occupation_cohort_key, load_datetime
  from {{ ref("sat_reporting_content_event") }}
  where subject_user_hk is not null

  union

  select reporting_date, subject_user_hk, occupation_cohort_key, load_datetime
  from {{ ref("sat_reporting_reaction_event") }}
  where subject_user_hk is not null

  union

  select reporting_date, subject_user_hk, occupation_cohort_key, load_datetime
  from {{ ref("sat_reporting_feed_serving_event") }}
  where subject_user_hk is not null
),
daily as (
  select
    reporting_date,
    coalesce(occupation_cohort_key, 'unknown') as occupation_cohort_key,
    count(distinct subject_user_hk)::bigint as distinct_user_count,
    max(load_datetime) as load_datetime
  from actor_cohorts
  group by reporting_date, coalesce(occupation_cohort_key, 'unknown')
),
totals as (
  select
    reporting_date,
    count(distinct subject_user_hk)::bigint as total_user_count
  from actor_cohorts
  group by reporting_date
)

select
  daily.reporting_date,
  daily.occupation_cohort_key,
  daily.distinct_user_count,
  totals.total_user_count,
  case
    when totals.total_user_count > 0
      then daily.distinct_user_count::numeric / totals.total_user_count::numeric
  end as occupation_cohort_share,
  case
    when daily.distinct_user_count >= 10 and totals.total_user_count >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  'partial'::text as source_completeness_label,
  'analytics_event_cohort_only_pending_canonical_app_sources'::text as source_completeness_detail,
  'occupation_cohort_user_count_daily,occupation_cohort_share_daily'::text as metric_contract_ids,
  'direct'::text as wording_status,
  daily.load_datetime,
  'analytics.raw_event_landing'::text as record_source
from daily
join totals using (reporting_date)
