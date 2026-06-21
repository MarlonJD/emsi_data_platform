{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

select distinct
  reaction_hk,
  event_id as reaction_business_key,
  load_datetime,
  record_source
from {{ ref("stg_product_reporting_reactions") }}
where reaction_hk is not null
  and event_id is not null
