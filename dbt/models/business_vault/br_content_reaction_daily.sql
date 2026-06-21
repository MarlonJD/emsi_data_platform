{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

select
  reporting_date,
  content_hk,
  max(content_business_key) as content_business_key,
  coalesce(max(occupation_cohort_key) filter (where occupation_cohort_key is not null), 'unknown')
    as occupation_cohort_key,
  coalesce(reaction_key, 'unknown') as reaction_key,
  coalesce(emoji_key, 'unknown') as emoji_key,
  coalesce(reaction_valence, 'unknown') as reaction_valence,
  count(*) filter (where reaction_action = 'added')::bigint as reaction_added_count,
  count(*) filter (where reaction_action = 'removed')::bigint as reaction_removed_count,
  (
    count(*) filter (where reaction_action = 'added')
    - count(*) filter (where reaction_action = 'removed')
  )::bigint as net_reaction_count,
  count(distinct subject_user_hk)::bigint as distinct_actor_count,
  case
    when count(distinct subject_user_hk) >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  max(load_datetime) as load_datetime,
  'analytics.raw_event_landing'::text as record_source
from {{ ref("sat_reporting_reaction_event") }}
where content_hk is not null
group by reporting_date, content_hk, coalesce(reaction_key, 'unknown'),
  coalesce(emoji_key, 'unknown'), coalesce(reaction_valence, 'unknown')
