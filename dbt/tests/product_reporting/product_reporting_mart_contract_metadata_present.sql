{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

with mart_rows as (
  select
    'mart_product_reporting_occupation_cohort_daily'::text as model_name,
    reporting_date::text as reporting_date,
    occupation_cohort_key::text as grain_key,
    source_completeness_label,
    metric_contract_ids,
    wording_status,
    reporting_timezone
  from {{ ref("mart_product_reporting_occupation_cohort_daily") }}

  union all

  select
    'mart_product_reporting_content_performance_daily'::text as model_name,
    reporting_date::text as reporting_date,
    content_reporting_key::text as grain_key,
    source_completeness_label,
    metric_contract_ids,
    wording_status,
    reporting_timezone
  from {{ ref("mart_product_reporting_content_performance_daily") }}

  union all

  select
    'mart_product_reporting_emoji_reaction_daily'::text as model_name,
    reporting_date::text as reporting_date,
    concat_ws('||', emoji_key, occupation_cohort_key) as grain_key,
    source_completeness_label,
    metric_contract_id as metric_contract_ids,
    wording_status,
    reporting_timezone
  from {{ ref("mart_product_reporting_emoji_reaction_daily") }}

  union all

  select
    'mart_product_reporting_reaction_valence_daily'::text as model_name,
    reporting_date::text as reporting_date,
    concat_ws('||', reaction_valence, occupation_cohort_key) as grain_key,
    source_completeness_label,
    metric_contract_ids,
    wording_status,
    reporting_timezone
  from {{ ref("mart_product_reporting_reaction_valence_daily") }}

  union all

  select
    'mart_product_reporting_feed_interest_proxy_daily'::text as model_name,
    reporting_date::text as reporting_date,
    feed_item_reporting_key::text as grain_key,
    source_completeness_label,
    metric_contract_ids,
    wording_status,
    reporting_timezone
  from {{ ref("mart_product_reporting_feed_interest_proxy_daily") }}

  union all

  select
    'mart_product_reporting_together_coordination_daily'::text as model_name,
    reporting_date::text as reporting_date,
    concat_ws('||', activity_type, visibility, together_status, channel_business_key) as grain_key,
    source_completeness_label,
    metric_contract_ids,
    wording_status,
    reporting_timezone
  from {{ ref("mart_product_reporting_together_coordination_daily") }}
)

select *
from mart_rows
where source_completeness_label not in ('source_complete', 'partial', 'unavailable')
   or nullif(metric_contract_ids, '') is null
   or wording_status not in ('direct', 'proxy', 'proxy/partial', 'explicit-signal-only', 'partial')
   or reporting_timezone <> 'Europe/Istanbul'
