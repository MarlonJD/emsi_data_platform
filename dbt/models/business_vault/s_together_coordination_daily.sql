{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

with aggregated as (
  select
    reporting_date,
    coalesce(activity_type, 'unknown') as activity_type,
    coalesce(visibility, 'unknown') as visibility,
    coalesce(together_status, 'unknown') as together_status,
    coalesce(channel_business_key, 'unknown') as channel_business_key,
    count(distinct together_item_hk)::bigint as together_item_count,
    count(*) filter (where together_action = 'created')::bigint as together_created_count,
    count(*) filter (where together_action = 'invite_sent')::bigint as invite_sent_count,
    count(*) filter (where together_action in ('shared', 'share_link_created'))::bigint as share_count,
    count(*) filter (where together_action = 'opened')::bigint as opened_count,
    count(*) filter (where together_action = 'response_added')::bigint as response_added_count,
    count(*) filter (where together_action = 'reported')::bigint as reported_count,
    coalesce(sum(invite_count), 0)::bigint as invite_count_total,
    coalesce(sum(response_count), 0)::bigint as response_count_total,
    count(distinct subject_user_hk)::bigint as distinct_actor_count,
    max(load_datetime) as load_datetime
  from {{ ref("s_together_metadata_raw") }}
  where together_item_hk is not null
  group by
    reporting_date,
    coalesce(activity_type, 'unknown'),
    coalesce(visibility, 'unknown'),
    coalesce(together_status, 'unknown'),
    coalesce(channel_business_key, 'unknown')
)

select
  reporting_date,
  activity_type,
  visibility,
  together_status,
  channel_business_key,
  together_item_count,
  together_created_count,
  invite_sent_count,
  share_count,
  opened_count,
  response_added_count,
  reported_count,
  invite_count_total,
  response_count_total,
  (
    together_created_count
    + response_added_count * 2
    + opened_count
    + share_count
    - reported_count * 3
  )::bigint as together_coordination_success_proxy_score,
  distinct_actor_count,
  case
    when distinct_actor_count >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  'partial'::text as source_completeness_label,
  'together_items_created_count_daily,together_response_count_daily,together_coordination_success_proxy_daily'::text
    as metric_contract_ids,
  'proxy/partial'::text as wording_status,
  load_datetime,
  'analytics.raw_event_landing'::text as record_source
from aggregated
