{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

select distinct
  md5(concat_ws('||', reaction_hk, content_hk)) as link_hk,
  reaction_hk,
  content_hk,
  load_datetime,
  record_source
from {{ ref("stg_product_reporting_reactions") }}
where reaction_hk is not null
  and content_hk is not null
