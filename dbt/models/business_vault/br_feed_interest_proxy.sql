{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

with aggregated as (
  select
    reporting_date,
    feed_item_hk,
    content_business_key,
    coalesce(max(occupation_cohort_key) filter (where occupation_cohort_key is not null), 'unknown')
      as occupation_cohort_key,
    coalesce(max(feed_mode), 'unknown') as feed_mode,
    coalesce(max(surface), 'unknown') as surface,
    count(*) filter (
      where event_name in ('feed_item_impression', 'feed_item_viewable_impression')
    )::bigint as qualified_impression_count,
    count(*) filter (
      where event_name in (
        'feed_item_open',
        'feed_item_like',
        'feed_item_reply',
        'feed_item_join',
        'feed_item_share',
        'feed_item_copy_link'
      )
    )::bigint as positive_action_count,
    count(*) filter (
      where event_name in (
        'feed_item_hide',
        'feed_item_show_less',
        'feed_item_not_interested',
        'feed_item_mute_author',
        'feed_item_mute_channel'
      )
    )::bigint as negative_action_count,
    coalesce(sum(dwell_ms), 0)::bigint as dwell_ms_total,
    count(distinct subject_user_hk)::bigint as distinct_actor_count,
    count(*) filter (where source_completeness_input = 'source_complete_with_occupation_cohort')::bigint
      as source_complete_with_cohort_count,
    max(load_datetime) as load_datetime
  from {{ ref("sat_reporting_feed_serving_event") }}
  where feed_item_hk is not null
  group by reporting_date, feed_item_hk, content_business_key
)

select
  reporting_date,
  feed_item_hk,
  content_business_key,
  occupation_cohort_key,
  feed_mode,
  surface,
  qualified_impression_count,
  positive_action_count,
  negative_action_count,
  dwell_ms_total,
  (
    positive_action_count
    + case when dwell_ms_total > 0 then 1 else 0 end
    - negative_action_count * 3
  )::bigint as feed_interest_proxy_score,
  'partial'::text as source_completeness_label,
  case
    when source_complete_with_cohort_count > 0 then 'accepted_feed_events_with_occupation_cohort'
    else 'accepted_feed_events_only'
  end as source_completeness_detail,
  distinct_actor_count,
  case
    when distinct_actor_count >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  load_datetime,
  'analytics.raw_event_landing'::text as record_source
from aggregated
