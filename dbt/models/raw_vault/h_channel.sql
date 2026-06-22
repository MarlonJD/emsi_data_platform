{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

with hub_source as (
  select
    channel_hk,
    channel_business_key,
    load_datetime,
    record_source
  from {{ ref("stg_product_reporting_channel_sessions") }}
  where channel_hk is not null
    and channel_business_key is not null

  union all

  select
    channel_hk,
    channel_business_key,
    load_datetime,
    record_source
  from {{ ref("stg_product_reporting_content_events") }}
  where channel_hk is not null
    and channel_business_key is not null

  union all

  select
    channel_hk,
    channel_business_key,
    load_datetime,
    record_source
  from {{ ref("stg_app_together_items") }}
  where channel_hk is not null
    and channel_business_key is not null
)

select
  channel_hk,
  channel_business_key,
  min(load_datetime) as load_datetime,
  min(record_source) as record_source
from hub_source
group by channel_hk, channel_business_key
