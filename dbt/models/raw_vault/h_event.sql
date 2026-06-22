{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

with hub_source as (
  select
    community_event_hk as event_hk,
    community_event_business_key as event_business_key,
    load_datetime,
    record_source
  from {{ ref("stg_product_reporting_event_funnel") }}
  where community_event_hk is not null
    and community_event_business_key is not null

  union all

  select
    linked_event_hk as event_hk,
    linked_event_business_key as event_business_key,
    load_datetime,
    record_source
  from {{ ref("stg_app_together_items") }}
  where linked_event_hk is not null
    and linked_event_business_key is not null
)

select
  event_hk,
  event_business_key,
  min(load_datetime) as load_datetime,
  min(record_source) as record_source
from hub_source
group by event_hk, event_business_key
