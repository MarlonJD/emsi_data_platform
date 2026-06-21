{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

with aggregated as (
  select
    reporting_date,
    coalesce(emoji_key, 'unknown') as emoji_key,
    coalesce(occupation_cohort_key, 'unknown') as occupation_cohort_key,
    count(*) filter (where reaction_action = 'added')::bigint as reaction_added_count,
    count(*) filter (where reaction_action = 'removed')::bigint as reaction_removed_count,
    count(distinct subject_user_hk)::bigint as distinct_actor_count,
    max(load_datetime) as load_datetime
  from {{ ref("sat_reporting_reaction_event") }}
  group by reporting_date, coalesce(emoji_key, 'unknown'), coalesce(occupation_cohort_key, 'unknown')
)

select
  reporting_date,
  emoji_key,
  occupation_cohort_key,
  reaction_added_count,
  reaction_removed_count,
  (reaction_added_count - reaction_removed_count)::bigint as emoji_usage_count,
  distinct_actor_count,
  case
    when distinct_actor_count >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  'partial'::text as source_completeness_label,
  'emoji_usage_count_daily'::text as metric_contract_id,
  'direct'::text as wording_status,
  load_datetime,
  'analytics.raw_event_landing'::text as record_source
from aggregated
