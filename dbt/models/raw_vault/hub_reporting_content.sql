{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

with hub_source as (
  select
    content_hk,
    content_business_key,
    load_datetime,
    record_source
  from {{ ref("stg_product_reporting_content_events") }}
  where content_hk is not null
    and content_business_key is not null
)

select
  content_hk,
  content_business_key,
  min(load_datetime) as load_datetime,
  min(record_source) as record_source
from hub_source
group by content_hk, content_business_key
