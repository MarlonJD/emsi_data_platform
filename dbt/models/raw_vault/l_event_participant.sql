{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

select
  md5(concat_ws('||', subject_user_hk, community_event_hk, event_funnel_action_hk)) as link_hk,
  subject_user_hk,
  community_event_hk as event_hk,
  event_funnel_action_hk,
  min(load_datetime) as load_datetime,
  min(record_source) as record_source
from {{ ref("stg_product_reporting_event_funnel") }}
where subject_user_hk is not null
  and community_event_hk is not null
  and event_funnel_action_hk is not null
group by subject_user_hk, community_event_hk, event_funnel_action_hk
