{{ config(materialized="view", tags=["product_reporting_phase3", "mart"]) }}

select
  reporting_date,
  content_hk as content_reporting_key,
  content_business_key,
  content_type,
  channel_business_key,
  occupation_cohort_key,
  positive_reaction_count,
  like_like_reaction_count,
  reply_count,
  share_or_open_count,
  hide_or_report_count,
  content_performance_score,
  most_liked_rank_score,
  source_completeness_label,
  metric_contract_ids,
  wording_status,
  latest_event_at,
  'Europe/Istanbul'::text as reporting_timezone,
  load_datetime as refreshed_at
from {{ ref("s_content_performance_daily") }}
where small_cell_suppression_status = 'reportable'
