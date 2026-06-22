{{ config(materialized="view", tags=["product_reporting_phase3", "mart", "quality"]) }}

select
  metric_contract_id,
  business_vault_model,
  expected_grain,
  source_completeness_label,
  suppression_rule,
  wording_status,
  coverage_status,
  product_layer_contract_status
from {{ ref("product_reporting_bdv_contract_coverage") }}
