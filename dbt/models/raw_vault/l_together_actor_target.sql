{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

select distinct
  md5(concat_ws('||', subject_user_hk, together_item_hk, together_target_hk)) as link_hk,
  subject_user_hk,
  together_item_hk,
  together_target_hk,
  load_datetime,
  record_source
from {{ ref("stg_app_together_items") }}
where subject_user_hk is not null
  and together_item_hk is not null
  and together_target_hk is not null
