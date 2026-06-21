{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

select
  source_model,
  row_count,
  distinct_event_count,
  missing_business_key_count
from {{ ref("product_reporting_stage_reconciliation") }}
where row_count < 0
   or distinct_event_count < 0
   or missing_business_key_count < 0
   or distinct_event_count > row_count
   or missing_business_key_count > row_count
