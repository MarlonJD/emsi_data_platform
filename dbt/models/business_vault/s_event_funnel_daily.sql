{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

select
  reporting_date,
  event_funnel_action,
  sum(action_count)::bigint as action_count,
  sum(positive_intent_count)::bigint as positive_intent_count,
  sum(dropoff_or_negative_count)::bigint as dropoff_or_negative_count,
  count(distinct event_hk)::bigint as distinct_event_count,
  sum(distinct_actor_count)::bigint as actor_event_count,
  case
    when sum(distinct_actor_count) >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  'partial'::text as source_completeness_label,
  'event_funnel_action_count_daily,event_join_conversion_proxy_daily'::text as metric_contract_ids,
  'proxy/partial'::text as wording_status,
  max(load_datetime) as load_datetime,
  'analytics.raw_event_landing'::text as record_source
from {{ ref("br_event_funnel") }}
group by reporting_date, event_funnel_action
