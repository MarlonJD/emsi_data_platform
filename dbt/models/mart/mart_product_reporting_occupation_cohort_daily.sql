{{ config(materialized="view", tags=["product_reporting_phase3", "mart"]) }}

select
  reporting_date,
  occupation_cohort_key,
  distinct_user_count,
  total_user_count,
  occupation_cohort_share,
  source_completeness_label,
  source_completeness_detail,
  metric_contract_ids,
  wording_status,
  'Europe/Istanbul'::text as reporting_timezone,
  load_datetime as refreshed_at
from {{ ref("s_occupation_cohort_daily") }}
where small_cell_suppression_status = 'reportable'
