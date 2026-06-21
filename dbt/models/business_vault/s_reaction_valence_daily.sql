{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

select
  reporting_date,
  coalesce(reaction_valence, 'unknown') as reaction_valence,
  coalesce(occupation_cohort_key, 'unknown') as occupation_cohort_key,
  count(*)::bigint as reaction_valence_count,
  count(*) filter (where reaction_action = 'added')::bigint as added_reaction_count,
  count(*) filter (where reaction_action = 'removed')::bigint as removed_reaction_count,
  count(distinct subject_user_hk)::bigint as distinct_actor_count,
  case
    when count(distinct subject_user_hk) >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  'partial'::text as source_completeness_label,
  'reaction_valence_count_daily,positive_reaction_day_daily,negative_reaction_day_daily'::text
    as metric_contract_ids,
  'explicit-signal-only'::text as wording_status,
  max(load_datetime) as load_datetime,
  'analytics.raw_event_landing'::text as record_source
from {{ ref("sat_reporting_reaction_event") }}
group by reporting_date, coalesce(reaction_valence, 'unknown'), coalesce(occupation_cohort_key, 'unknown')
