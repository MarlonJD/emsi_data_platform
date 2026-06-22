{{ config(materialized="view", tags=["product_reporting_phase3", "mart"]) }}

select
  reporting_date,
  activity_type,
  visibility,
  together_status,
  channel_business_key,
  together_item_count,
  together_created_count,
  invite_sent_count,
  share_count,
  opened_count,
  response_added_count,
  reported_count,
  invite_count_total,
  response_count_total,
  together_coordination_success_proxy_score,
  source_completeness_label,
  metric_contract_ids,
  wording_status,
  'Europe/Istanbul'::text as reporting_timezone,
  load_datetime as refreshed_at
from {{ ref("s_together_coordination_daily") }}
where small_cell_suppression_status = 'reportable'
