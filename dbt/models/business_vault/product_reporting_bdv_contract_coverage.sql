{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault", "quality"]) }}

select
  metric_contract_id,
  business_vault_model,
  expected_grain,
  source_completeness_label,
  suppression_rule,
  wording_status,
  'covered_in_phase2_bdv'::text as coverage_status
from (
  values
    (
      'occupation_cohort_user_count_daily',
      's_occupation_cohort_daily',
      'reporting_date x occupation_cohort_key',
      'partial',
      'suppress_before_pl when distinct_user_count < 10',
      'direct'
    ),
    (
      'occupation_cohort_share_daily',
      's_occupation_cohort_daily',
      'reporting_date x occupation_cohort_key',
      'partial',
      'suppress_before_pl when numerator or denominator < 10',
      'direct'
    ),
    (
      'most_liked_content_daily',
      's_content_performance_daily',
      'reporting_date x content_hk',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'direct'
    ),
    (
      'content_performance_score_daily',
      's_content_performance_daily',
      'reporting_date x content_hk',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'proxy'
    ),
    (
      'emoji_usage_count_daily',
      's_emoji_usage_daily',
      'reporting_date x emoji_key',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'direct'
    ),
    (
      'reaction_valence_count_daily',
      's_reaction_valence_daily',
      'reporting_date x reaction_valence',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'explicit-signal-only'
    ),
    (
      'feed_interest_proxy_score_daily',
      'br_feed_interest_proxy',
      'reporting_date x content_business_key',
      'partial',
      'suppress_before_pl when distinct_actor_count < 10',
      'proxy'
    ),
    (
      'feed_interest_source_completeness_daily',
      'br_feed_interest_proxy',
      'reporting_date x content_business_key or reporting_date x feed_mode',
      'partial',
      'no user-level output',
      'partial'
    )
) as coverage(
  metric_contract_id,
  business_vault_model,
  expected_grain,
  source_completeness_label,
  suppression_rule,
  wording_status
)
