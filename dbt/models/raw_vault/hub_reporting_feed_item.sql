{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

select distinct
  feed_item_hk,
  feed_item_business_key,
  load_datetime,
  record_source
from {{ ref("stg_product_reporting_feed_events") }}
where feed_item_hk is not null
  and feed_item_business_key is not null
