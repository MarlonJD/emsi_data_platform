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
