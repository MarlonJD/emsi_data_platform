{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

select
  reporting_date,
  together_item_hk,
  max(together_item_business_key) as together_item_business_key,
  coalesce(response_type, 'unknown') as response_type,
  coalesce(together_status, 'unknown') as together_status,
  count(*) filter (where together_action = 'invite_sent')::bigint as invite_sent_count,
  count(*) filter (where together_action in ('shared', 'share_link_created'))::bigint as share_count,
  count(*) filter (where together_action = 'opened')::bigint as opened_count,
  count(*) filter (where together_action = 'response_added')::bigint as response_added_count,
  count(*) filter (where together_action = 'reported')::bigint as reported_count,
  count(distinct subject_user_hk)::bigint as distinct_actor_count,
  case
    when count(distinct subject_user_hk) >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  max(load_datetime) as load_datetime,
  'analytics.raw_event_landing'::text as record_source
from {{ ref("s_together_metadata_raw") }}
where together_item_hk is not null
group by reporting_date, together_item_hk, coalesce(response_type, 'unknown'), coalesce(together_status, 'unknown')
