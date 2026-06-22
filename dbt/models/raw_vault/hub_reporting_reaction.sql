{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

with hub_source as (
  select
    reaction_hk,
    event_id as reaction_business_key,
    load_datetime,
    record_source
  from {{ ref("stg_product_reporting_reactions") }}
  where reaction_hk is not null
    and event_id is not null
)

select
  reaction_hk,
  reaction_business_key,
  min(load_datetime) as load_datetime,
  min(record_source) as record_source
from hub_source
group by reaction_hk, reaction_business_key
