{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

select
  's_emoji_usage_daily'::text as model_name,
  reporting_date::text as reporting_date,
  concat_ws('||', emoji_key, occupation_cohort_key) as grain_key,
  'emoji_usage_count_mismatch'::text as violation,
  emoji_usage_count::numeric as observed_value,
  (reaction_added_count - reaction_removed_count)::numeric as expected_value
from {{ ref("s_emoji_usage_daily") }}
where emoji_usage_count <> reaction_added_count - reaction_removed_count
   or reaction_added_count < 0
   or reaction_removed_count < 0

union all

select
  's_reaction_valence_daily'::text as model_name,
  reporting_date::text as reporting_date,
  concat_ws('||', reaction_valence, occupation_cohort_key) as grain_key,
  'reaction_valence_count_mismatch'::text as violation,
  reaction_valence_count::numeric as observed_value,
  (added_reaction_count + removed_reaction_count)::numeric as expected_value
from {{ ref("s_reaction_valence_daily") }}
where reaction_valence_count <> added_reaction_count + removed_reaction_count
   or added_reaction_count < 0
   or removed_reaction_count < 0

union all

select
  's_occupation_cohort_daily'::text as model_name,
  reporting_date::text as reporting_date,
  occupation_cohort_key as grain_key,
  'occupation_share_out_of_range'::text as violation,
  occupation_cohort_share::numeric as observed_value,
  null::numeric as expected_value
from {{ ref("s_occupation_cohort_daily") }}
where occupation_cohort_share < 0
   or occupation_cohort_share > 1
   or distinct_user_count < 0
   or total_user_count < 0
   or distinct_user_count > total_user_count
