{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

with hub_source as (
  select
    together_item_hk,
    together_item_business_key,
    load_datetime,
    record_source
  from {{ ref("stg_app_together_items") }}
  where together_item_hk is not null
    and together_item_business_key is not null
)

select
  together_item_hk,
  together_item_business_key,
  min(load_datetime) as load_datetime,
  min(record_source) as record_source
from hub_source
group by together_item_hk, together_item_business_key
