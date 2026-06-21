{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

select distinct
  content_hk,
  content_business_key,
  load_datetime,
  record_source
from {{ ref("stg_product_reporting_content_events") }}
where content_hk is not null
  and content_business_key is not null
