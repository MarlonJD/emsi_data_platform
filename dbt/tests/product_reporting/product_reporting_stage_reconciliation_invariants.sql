{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

select
  source_model,
  reporting_date,
  accepted_source_count,
  accepted_source_distinct_event_count,
  row_count,
  distinct_event_count,
  expected_excluded_count,
  expected_rejected_count,
  expected_dlq_count,
  expected_deduplicated_replay_count,
  expected_suppression_count,
  missing_business_key_count,
  unexplained_delta,
  reconciliation_status
from {{ ref("product_reporting_stage_reconciliation") }}
where row_count < 0
   or distinct_event_count < 0
   or accepted_source_count < 0
   or accepted_source_distinct_event_count < 0
   or expected_excluded_count < 0
   or expected_rejected_count < 0
   or expected_dlq_count < 0
   or expected_deduplicated_replay_count < 0
   or expected_suppression_count < 0
   or missing_business_key_count < 0
   or distinct_event_count > row_count
   or accepted_source_distinct_event_count > accepted_source_count
   or missing_business_key_count > row_count
   or accepted_source_count <> (
     row_count
     + expected_excluded_count
     + expected_rejected_count
     + expected_dlq_count
     + expected_deduplicated_replay_count
     + expected_suppression_count
     + unexplained_delta
   )
   or unexplained_delta <> 0
   or reconciliation_status <> 'explained'
