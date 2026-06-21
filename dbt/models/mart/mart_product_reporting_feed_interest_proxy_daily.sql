{{ config(materialized="view", tags=["product_reporting_phase3", "mart"]) }}

select
  reporting_date,
  feed_item_hk as feed_item_reporting_key,
  content_business_key,
  occupation_cohort_key,
  feed_mode,
  surface,
  qualified_impression_count,
  positive_action_count,
  negative_action_count,
  dwell_ms_total,
  feed_interest_proxy_score,
  source_completeness_label,
  source_completeness_detail,
  'feed_interest_proxy_score_daily,feed_interest_source_completeness_daily'::text as metric_contract_ids,
  'proxy/partial'::text as wording_status,
  'Europe/Istanbul'::text as reporting_timezone,
  load_datetime as refreshed_at
from {{ ref("br_feed_interest_proxy") }}
where small_cell_suppression_status = 'reportable'
