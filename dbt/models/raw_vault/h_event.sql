{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

select distinct
  community_event_hk as event_hk,
  community_event_business_key as event_business_key,
  load_datetime,
  record_source
from {{ ref("stg_product_reporting_event_funnel") }}
where community_event_hk is not null
  and community_event_business_key is not null

union

select distinct
  linked_event_hk as event_hk,
  linked_event_business_key as event_business_key,
  load_datetime,
  record_source
from {{ ref("stg_app_together_items") }}
where linked_event_hk is not null
  and linked_event_business_key is not null
