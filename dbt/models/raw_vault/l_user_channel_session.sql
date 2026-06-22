{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault"]) }}

select
  md5(concat_ws('||', subject_user_hk, channel_hk, channel_session_hk)) as link_hk,
  subject_user_hk,
  channel_hk,
  channel_session_hk,
  min(load_datetime) as load_datetime,
  min(record_source) as record_source
from {{ ref("stg_product_reporting_channel_sessions") }}
where subject_user_hk is not null
  and channel_hk is not null
  and channel_session_hk is not null
group by subject_user_hk, channel_hk, channel_session_hk
