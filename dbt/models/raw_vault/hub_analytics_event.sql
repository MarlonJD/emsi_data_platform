{{ config(materialized="view", tags=["phase_d_smoke", "data_vault"]) }}

select distinct
  event_hk,
  event_id as event_business_key,
  load_datetime,
  record_source
from {{ ref("stg_analytics_events") }}
where event_id is not null
