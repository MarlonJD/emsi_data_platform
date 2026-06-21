{{ config(materialized="view", tags=["product_reporting_phase3", "mart"]) }}

select
  reporting_date,
  emoji_key,
  occupation_cohort_key,
  reaction_added_count,
  reaction_removed_count,
  emoji_usage_count,
  source_completeness_label,
  metric_contract_id,
  wording_status,
  'Europe/Istanbul'::text as reporting_timezone,
  load_datetime as refreshed_at
from {{ ref("s_emoji_usage_daily") }}
where small_cell_suppression_status = 'reportable'
