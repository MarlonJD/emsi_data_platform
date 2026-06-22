{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

select
  reporting_date,
  community_event_hk as event_hk,
  max(community_event_business_key) as event_business_key,
  min(occurred_at) as first_event_at,
  max(occurred_at) as latest_event_at,
  count(*)::bigint as event_funnel_event_count,
  count(distinct event_funnel_action)::bigint as distinct_action_count,
  count(distinct subject_user_hk)::bigint as distinct_actor_count,
  case
    when count(*) filter (where source_completeness_input = 'source_complete') > 0 then 'partial'
    else 'unavailable'
  end as source_completeness_label,
  case
    when count(distinct subject_user_hk) >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  max(load_datetime) as load_datetime,
  'analytics.raw_event_landing'::text as record_source
from {{ ref("s_event_metadata_raw") }}
where community_event_hk is not null
group by reporting_date, community_event_hk
