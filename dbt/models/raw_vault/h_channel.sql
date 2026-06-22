{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

select distinct
  channel_hk,
  channel_business_key,
  load_datetime,
  record_source
from {{ ref("stg_product_reporting_channel_sessions") }}
where channel_hk is not null
  and channel_business_key is not null

union

select distinct
  channel_hk,
  channel_business_key,
  load_datetime,
  record_source
from {{ ref("stg_product_reporting_content_events") }}
where channel_hk is not null
  and channel_business_key is not null

union

select distinct
  channel_hk,
  channel_business_key,
  load_datetime,
  record_source
from {{ ref("stg_app_together_items") }}
where channel_hk is not null
  and channel_business_key is not null
