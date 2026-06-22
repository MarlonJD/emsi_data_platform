{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

select distinct
  together_item_hk,
  together_item_business_key,
  load_datetime,
  record_source
from {{ ref("stg_app_together_items") }}
where together_item_hk is not null
  and together_item_business_key is not null
