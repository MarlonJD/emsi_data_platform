{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

with aggregated as (
  select
    reporting_date,
    content_hk,
    max(content_business_key) as content_business_key,
    coalesce(max(content_type), 'unknown') as content_type,
    max(channel_business_key) as channel_business_key,
    coalesce(max(occupation_cohort_key) filter (where occupation_cohort_key is not null), 'unknown')
      as occupation_cohort_key,
    count(*) filter (
      where event_name in ('reaction_added', 'feed_item_like')
        and reaction_valence = 'positive'
    )::bigint as positive_reaction_count,
    count(*) filter (
      where event_name = 'feed_item_like'
        or (event_name = 'reaction_added' and coalesce(emoji_key, '') in ('like', 'thumbs_up', 'thumbsup'))
    )::bigint as like_like_reaction_count,
    count(*) filter (where event_name in ('reply_created', 'feed_item_reply'))::bigint as reply_count,
    count(*) filter (
      where event_name in ('feed_item_open', 'feed_item_join', 'feed_item_share', 'feed_item_copy_link')
    )::bigint as share_or_open_count,
    count(*) filter (
      where event_name in (
        'feed_item_hide',
        'feed_item_show_less',
        'feed_item_not_interested',
        'feed_item_mute_author',
        'feed_item_mute_channel'
      )
    )::bigint as hide_or_report_count,
    count(distinct subject_user_hk)::bigint as distinct_actor_count,
    max(occurred_at) as latest_event_at,
    max(load_datetime) as load_datetime
  from {{ ref("sat_reporting_content_event") }}
  where content_hk is not null
  group by reporting_date, content_hk
)

select
  reporting_date,
  content_hk,
  content_business_key,
  content_type,
  channel_business_key,
  occupation_cohort_key,
  positive_reaction_count,
  like_like_reaction_count,
  reply_count,
  share_or_open_count,
  hide_or_report_count,
  (
    positive_reaction_count
    + reply_count * 2
    + share_or_open_count
    - hide_or_report_count * 3
  )::bigint as content_performance_score,
  (
    positive_reaction_count * 1000000
    + like_like_reaction_count * 10000
    + reply_count * 100
  )::bigint as most_liked_rank_score,
  distinct_actor_count,
  case
    when distinct_actor_count >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  'partial'::text as source_completeness_label,
  'most_liked_content_daily,content_performance_score_daily'::text as metric_contract_ids,
  'direct/proxy'::text as wording_status,
  latest_event_at,
  load_datetime,
  'analytics.raw_event_landing'::text as record_source
from aggregated
