{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault", "quality"]) }}

select
  metric_contract_id,
  business_vault_model,
  expected_grain,
  source_completeness_label,
  suppression_rule,
  wording_status,
  product_layer_contract_status,
  'covered_in_phase2_bdv'::text as coverage_status
from (
  values
    (
      'occupation_cohort_user_count_daily',
      's_occupation_cohort_daily',
      'reporting_date x occupation_cohort_key',
      'partial',
      'suppress_before_pl when distinct_user_count < 10',
      'direct',
      'product_reporting_phase3_pl'
    ),
    (
      'occupation_cohort_share_daily',
      's_occupation_cohort_daily',
      'reporting_date x occupation_cohort_key',
      'partial',
      'suppress_before_pl when numerator or denominator < 10',
      'direct',
      'product_reporting_phase3_pl'
    ),
    (
      'most_liked_content_daily',
      's_content_performance_daily',
      'reporting_date x content_hk',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'direct',
      'product_reporting_phase3_pl'
    ),
    (
      'content_performance_score_daily',
      's_content_performance_daily',
      'reporting_date x content_hk',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'proxy/partial',
      'product_reporting_phase3_pl'
    ),
    (
      'emoji_usage_count_daily',
      's_emoji_usage_daily',
      'reporting_date x emoji_key',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'direct',
      'product_reporting_phase3_pl'
    ),
    (
      'reaction_valence_count_daily',
      's_reaction_valence_daily',
      'reporting_date x reaction_valence',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'explicit-signal-only',
      'product_reporting_phase3_pl'
    ),
    (
      'feed_interest_proxy_score_daily',
      'br_feed_interest_proxy',
      'reporting_date x content_business_key',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'proxy/partial',
      'product_reporting_phase3_pl'
    ),
    (
      'feed_interest_source_completeness_daily',
      'br_feed_interest_proxy',
      'reporting_date x content_business_key or reporting_date x feed_mode',
      'partial',
      'no user-level output',
      'partial',
      'product_reporting_phase3_pl'
    ),
    (
      'channel_session_count_daily',
      's_channel_session_daily',
      'reporting_date x channel_hk',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'direct',
      'bdv_only_pending_pl'
    ),
    (
      'channel_session_duration_ms_daily',
      's_channel_session_daily',
      'reporting_date x channel_hk',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'direct',
      'bdv_only_pending_pl'
    ),
    (
      'event_funnel_action_count_daily',
      's_event_funnel_daily',
      'reporting_date x event_funnel_action',
      'partial',
      'suppress_before_pl when actor_event_count < 10',
      'proxy',
      'bdv_only_pending_pl'
    ),
    (
      'event_join_conversion_proxy_daily',
      's_event_funnel_daily',
      'reporting_date x event_funnel_action',
      'partial',
      'suppress_before_pl when actor_event_count < 10',
      'proxy',
      'bdv_only_pending_pl'
    ),
    (
      'together_items_created_count_daily',
      's_together_coordination_daily',
      'reporting_date x activity_type x visibility x status x channel',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'proxy',
      'product_reporting_phase3_pl'
    ),
    (
      'together_response_count_daily',
      's_together_coordination_daily',
      'reporting_date x activity_type x visibility x status x channel',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'proxy',
      'product_reporting_phase3_pl'
    ),
    (
      'together_coordination_success_proxy_daily',
      's_together_coordination_daily',
      'reporting_date x activity_type x visibility x status x channel',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'proxy/partial',
      'product_reporting_phase3_pl'
    )
) as coverage(
  metric_contract_id,
  business_vault_model,
  expected_grain,
  source_completeness_label,
  suppression_rule,
  wording_status,
  product_layer_contract_status
)
