{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

with expected(metric_contract_id) as (
  values
    ('occupation_cohort_user_count_daily'),
    ('occupation_cohort_share_daily'),
    ('most_liked_content_daily'),
    ('content_performance_score_daily'),
    ('emoji_usage_count_daily'),
    ('reaction_valence_count_daily'),
    ('feed_interest_proxy_score_daily'),
    ('feed_interest_source_completeness_daily'),
    ('channel_session_count_daily'),
    ('channel_session_duration_ms_daily'),
    ('event_funnel_action_count_daily'),
    ('event_join_conversion_proxy_daily'),
    ('together_items_created_count_daily'),
    ('together_response_count_daily'),
    ('together_coordination_success_proxy_daily')
),
coverage as (
  select
    metric_contract_id,
    business_vault_model,
    expected_grain,
    source_completeness_label,
    suppression_rule,
    wording_status,
    coverage_status,
    product_layer_contract_status
  from {{ ref("mart_product_reporting_contract_coverage") }}
)

select
  'missing_contract'::text as violation,
  expected.metric_contract_id,
  null::text as detail
from expected
left join coverage using (metric_contract_id)
where coverage.metric_contract_id is null

union all

select
  'invalid_contract_metadata'::text as violation,
  metric_contract_id,
  concat_ws(
    '||',
    business_vault_model,
    expected_grain,
    source_completeness_label,
    suppression_rule,
    wording_status,
    coverage_status,
    product_layer_contract_status
  ) as detail
from coverage
where nullif(business_vault_model, '') is null
   or nullif(expected_grain, '') is null
   or source_completeness_label not in ('source_complete', 'partial', 'unavailable')
   or nullif(suppression_rule, '') is null
   or nullif(wording_status, '') is null
   or coverage_status <> 'covered_in_phase2_bdv'
   or product_layer_contract_status not in ('product_reporting_phase3_pl', 'bdv_only_pending_pl')
