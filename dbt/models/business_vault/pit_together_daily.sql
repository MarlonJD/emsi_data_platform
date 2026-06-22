{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

select
  reporting_date,
  together_item_hk,
  max(together_item_business_key) as together_item_business_key,
  coalesce(max(activity_type), 'unknown') as activity_type,
  coalesce(max(visibility), 'unknown') as visibility,
  coalesce(max(together_status), 'unknown') as together_status,
  max(channel_business_key) as channel_business_key,
  min(occurred_at) as first_event_at,
  max(occurred_at) as latest_event_at,
  count(*)::bigint as together_event_count,
  count(distinct subject_user_hk)::bigint as distinct_actor_count,
  coalesce(sum(invite_count), 0)::bigint as invite_count_total,
  coalesce(sum(response_count), 0)::bigint as response_count_total,
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
from {{ ref("s_together_metadata_raw") }}
where together_item_hk is not null
group by reporting_date, together_item_hk
