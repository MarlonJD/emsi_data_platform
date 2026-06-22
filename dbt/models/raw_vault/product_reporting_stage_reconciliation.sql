{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault", "quality"]) }}

select
  'stg_product_reporting_content_events'::text as source_model,
  count(*)::bigint as row_count,
  count(distinct event_id)::bigint as distinct_event_count,
  count(*) filter (where content_business_key is null)::bigint as missing_business_key_count
from {{ ref("stg_product_reporting_content_events") }}

union all

select
  'stg_product_reporting_reactions'::text as source_model,
  count(*)::bigint as row_count,
  count(distinct event_id)::bigint as distinct_event_count,
  count(*) filter (where reaction_hk is null or content_hk is null)::bigint as missing_business_key_count
from {{ ref("stg_product_reporting_reactions") }}

union all

select
  'stg_product_reporting_feed_events'::text as source_model,
  count(*)::bigint as row_count,
  count(distinct event_id)::bigint as distinct_event_count,
  count(*) filter (where feed_item_hk is null or content_hk is null)::bigint as missing_business_key_count
from {{ ref("stg_product_reporting_feed_events") }}

union all

select
  'stg_product_reporting_channel_sessions'::text as source_model,
  count(*)::bigint as row_count,
  count(distinct event_id)::bigint as distinct_event_count,
  count(*) filter (where channel_hk is null or channel_session_hk is null)::bigint
    as missing_business_key_count
from {{ ref("stg_product_reporting_channel_sessions") }}

union all

select
  'stg_product_reporting_event_funnel'::text as source_model,
  count(*)::bigint as row_count,
  count(distinct event_id)::bigint as distinct_event_count,
  count(*) filter (where community_event_hk is null or event_funnel_action_hk is null)::bigint
    as missing_business_key_count
from {{ ref("stg_product_reporting_event_funnel") }}

union all

select
  'stg_app_together_items'::text as source_model,
  count(*)::bigint as row_count,
  count(distinct event_id)::bigint as distinct_event_count,
  count(*) filter (where together_item_hk is null or together_target_hk is null)::bigint
    as missing_business_key_count
from {{ ref("stg_app_together_items") }}
