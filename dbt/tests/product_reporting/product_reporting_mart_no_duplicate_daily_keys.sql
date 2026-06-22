{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

select
  'mart_product_reporting_occupation_cohort_daily'::text as model_name,
  reporting_date::text as reporting_date,
  occupation_cohort_key::text as grain_key,
  count(*)::bigint as row_count
from {{ ref("mart_product_reporting_occupation_cohort_daily") }}
group by reporting_date, occupation_cohort_key
having count(*) > 1

union all

select
  'mart_product_reporting_content_performance_daily'::text as model_name,
  reporting_date::text as reporting_date,
  content_reporting_key::text as grain_key,
  count(*)::bigint as row_count
from {{ ref("mart_product_reporting_content_performance_daily") }}
group by reporting_date, content_reporting_key
having count(*) > 1

union all

select
  'mart_product_reporting_emoji_reaction_daily'::text as model_name,
  reporting_date::text as reporting_date,
  concat_ws('||', emoji_key, occupation_cohort_key) as grain_key,
  count(*)::bigint as row_count
from {{ ref("mart_product_reporting_emoji_reaction_daily") }}
group by reporting_date, emoji_key, occupation_cohort_key
having count(*) > 1

union all

select
  'mart_product_reporting_reaction_valence_daily'::text as model_name,
  reporting_date::text as reporting_date,
  concat_ws('||', reaction_valence, occupation_cohort_key) as grain_key,
  count(*)::bigint as row_count
from {{ ref("mart_product_reporting_reaction_valence_daily") }}
group by reporting_date, reaction_valence, occupation_cohort_key
having count(*) > 1

union all

select
  'mart_product_reporting_feed_interest_proxy_daily'::text as model_name,
  reporting_date::text as reporting_date,
  feed_item_reporting_key::text as grain_key,
  count(*)::bigint as row_count
from {{ ref("mart_product_reporting_feed_interest_proxy_daily") }}
group by reporting_date, feed_item_reporting_key
having count(*) > 1

union all

select
  'mart_product_reporting_together_coordination_daily'::text as model_name,
  reporting_date::text as reporting_date,
  concat_ws('||', activity_type, visibility, together_status, channel_business_key) as grain_key,
  count(*)::bigint as row_count
from {{ ref("mart_product_reporting_together_coordination_daily") }}
group by reporting_date, activity_type, visibility, together_status, channel_business_key
having count(*) > 1
