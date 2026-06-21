{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

select distinct
  md5(concat_ws('||', feed_item_hk, content_hk)) as link_hk,
  feed_item_hk,
  content_hk,
  load_datetime,
  record_source
from {{ ref("stg_product_reporting_feed_events") }}
where feed_item_hk is not null
  and content_hk is not null
