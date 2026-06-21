{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

select
  content_hk,
  reporting_date,
  max(content_business_key) as content_business_key,
  coalesce(max(content_type), 'unknown') as content_type,
  max(channel_business_key) as channel_business_key,
  coalesce(
    max(occupation_cohort_key) filter (where occupation_cohort_key is not null),
    'unknown'
  ) as occupation_cohort_key,
  min(occurred_at) as first_event_at,
  max(occurred_at) as latest_event_at,
  max(load_datetime) as load_datetime,
  count(*)::bigint as content_event_count,
  count(distinct subject_user_hk)::bigint as distinct_actor_count,
  case
    when count(*) filter (where source_completeness_input is not null) > 0 then 'partial'
    else 'unavailable'
  end as source_completeness_label,
  case
    when count(distinct subject_user_hk) >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  'analytics.raw_event_landing'::text as record_source
from {{ ref("sat_reporting_content_event") }}
where content_hk is not null
group by content_hk, reporting_date
