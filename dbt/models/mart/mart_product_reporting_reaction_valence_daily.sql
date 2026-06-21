{{ config(materialized="view", tags=["product_reporting_phase3", "mart"]) }}

select
  reporting_date,
  reaction_valence,
  occupation_cohort_key,
  reaction_valence_count,
  added_reaction_count,
  removed_reaction_count,
  source_completeness_label,
  metric_contract_ids,
  wording_status,
  'Europe/Istanbul'::text as reporting_timezone,
  load_datetime as refreshed_at
from {{ ref("s_reaction_valence_daily") }}
where small_cell_suppression_status = 'reportable'
