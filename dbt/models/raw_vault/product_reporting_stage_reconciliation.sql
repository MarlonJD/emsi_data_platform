{{ config(materialized="view", tags=["product_reporting_phase1", "data_vault", "quality"]) }}

with raw_landing as (
  select
    event_id,
    event_name,
    occurred_at,
    payload
  from {{ source("analytics", "raw_event_landing") }}
),

source_counts as (
  select
    'stg_product_reporting_content_events'::text as source_model,
    (occurred_at at time zone 'Europe/Istanbul')::date as reporting_date,
    count(*)::bigint as accepted_source_count,
    count(distinct event_id)::bigint as accepted_source_distinct_event_count,
    count(*) filter (
      where coalesce(payload ->> 'post_id', payload ->> 'item_id', payload ->> 'entity_id') is null
    )::bigint as expected_excluded_count
  from raw_landing
  where event_name in (
    'reaction_added',
    'reaction_removed',
    'reply_created',
    'post_impression',
    'feed_item_impression',
    'feed_item_viewable_impression',
    'feed_item_open',
    'feed_item_like',
    'feed_item_reply',
    'feed_item_join',
    'feed_item_share',
    'feed_item_copy_link',
    'feed_item_hide',
    'feed_item_show_less',
    'feed_item_not_interested',
    'feed_item_mute_author',
    'feed_item_mute_channel'
  )
  group by 1, 2

  union all

  select
    'stg_product_reporting_reactions'::text as source_model,
    (occurred_at at time zone 'Europe/Istanbul')::date as reporting_date,
    count(*)::bigint as accepted_source_count,
    count(distinct event_id)::bigint as accepted_source_distinct_event_count,
    count(*) filter (
      where coalesce(payload ->> 'post_id', payload ->> 'item_id', payload ->> 'entity_id') is null
    )::bigint as expected_excluded_count
  from raw_landing
  where event_name in ('reaction_added', 'reaction_removed', 'feed_item_like')
  group by 1, 2

  union all

  select
    'stg_product_reporting_feed_events'::text as source_model,
    (occurred_at at time zone 'Europe/Istanbul')::date as reporting_date,
    count(*)::bigint as accepted_source_count,
    count(distinct event_id)::bigint as accepted_source_distinct_event_count,
    count(*) filter (
      where coalesce(payload ->> 'item_id', payload ->> 'entity_id') is null
    )::bigint as expected_excluded_count
  from raw_landing
  where event_name like 'feed_item_%'
  group by 1, 2

  union all

  select
    'stg_product_reporting_channel_sessions'::text as source_model,
    (occurred_at at time zone 'Europe/Istanbul')::date as reporting_date,
    count(*)::bigint as accepted_source_count,
    count(distinct event_id)::bigint as accepted_source_distinct_event_count,
    count(*) filter (where nullif(payload ->> 'channel_id', '') is null)::bigint as expected_excluded_count
  from raw_landing
  where event_name in ('channel_session_started', 'channel_session_ended')
  group by 1, 2

  union all

  select
    'stg_product_reporting_event_funnel'::text as source_model,
    (occurred_at at time zone 'Europe/Istanbul')::date as reporting_date,
    count(*)::bigint as accepted_source_count,
    count(distinct event_id)::bigint as accepted_source_distinct_event_count,
    count(*) filter (
      where coalesce(payload ->> 'event_id', payload ->> 'community_event_id', payload ->> 'entity_id') is null
    )::bigint as expected_excluded_count
  from raw_landing
  where event_name in (
    'event_card_impression',
    'event_viewed',
    'event_detail_engagement',
    'event_interested',
    'event_uninterested',
    'event_join_attempted',
    'event_joined',
    'event_join_failed',
    'event_join_abandoned',
    'event_left',
    'event_cancel_reason_submitted',
    'event_reminder_set',
    'event_calendar_added',
    'event_shared',
    'event_invite_sent',
    'event_waitlist_joined',
    'event_waitlist_left',
    'event_check_in',
    'event_attendance_confirmed',
    'event_no_show_inferred'
  )
  group by 1, 2

  union all

  select
    'stg_app_together_items'::text as source_model,
    (occurred_at at time zone 'Europe/Istanbul')::date as reporting_date,
    count(*)::bigint as accepted_source_count,
    count(distinct event_id)::bigint as accepted_source_distinct_event_count,
    0::bigint as expected_excluded_count
  from raw_landing
  where event_name like 'together_%'
  group by 1, 2
),

target_counts as (
  select
    'stg_product_reporting_content_events'::text as source_model,
    reporting_date,
    count(*)::bigint as row_count,
    count(distinct event_id)::bigint as distinct_event_count,
    count(*) filter (where content_business_key is null)::bigint as missing_business_key_count
  from {{ ref("stg_product_reporting_content_events") }}
  group by 1, 2

  union all

  select
    'stg_product_reporting_reactions'::text as source_model,
    reporting_date,
    count(*)::bigint as row_count,
    count(distinct event_id)::bigint as distinct_event_count,
    count(*) filter (where reaction_hk is null or content_hk is null)::bigint as missing_business_key_count
  from {{ ref("stg_product_reporting_reactions") }}
  group by 1, 2

  union all

  select
    'stg_product_reporting_feed_events'::text as source_model,
    reporting_date,
    count(*)::bigint as row_count,
    count(distinct event_id)::bigint as distinct_event_count,
    count(*) filter (where feed_item_hk is null or content_hk is null)::bigint as missing_business_key_count
  from {{ ref("stg_product_reporting_feed_events") }}
  group by 1, 2

  union all

  select
    'stg_product_reporting_channel_sessions'::text as source_model,
    reporting_date,
    count(*)::bigint as row_count,
    count(distinct event_id)::bigint as distinct_event_count,
    count(*) filter (where channel_hk is null or channel_session_hk is null)::bigint as missing_business_key_count
  from {{ ref("stg_product_reporting_channel_sessions") }}
  group by 1, 2

  union all

  select
    'stg_product_reporting_event_funnel'::text as source_model,
    reporting_date,
    count(*)::bigint as row_count,
    count(distinct event_id)::bigint as distinct_event_count,
    count(*) filter (
      where community_event_hk is null or event_funnel_action_hk is null
    )::bigint as missing_business_key_count
  from {{ ref("stg_product_reporting_event_funnel") }}
  group by 1, 2

  union all

  select
    'stg_app_together_items'::text as source_model,
    reporting_date,
    count(*)::bigint as row_count,
    count(distinct event_id)::bigint as distinct_event_count,
    count(*) filter (
      where together_item_hk is null or together_target_hk is null
    )::bigint as missing_business_key_count
  from {{ ref("stg_app_together_items") }}
  group by 1, 2
),

reconciled as (
  select
    coalesce(source_counts.source_model, target_counts.source_model) as source_model,
    coalesce(source_counts.reporting_date, target_counts.reporting_date) as reporting_date,
    'Europe/Istanbul'::text as reporting_timezone,
    'accepted_landing_to_stage'::text as accounting_scope,
    'analytics.raw_event_landing'::text as source_relation,
    coalesce(source_counts.source_model, target_counts.source_model) as target_relation,
    coalesce(source_counts.accepted_source_count, 0)::bigint as accepted_source_count,
    coalesce(source_counts.accepted_source_distinct_event_count, 0)::bigint
      as accepted_source_distinct_event_count,
    coalesce(target_counts.row_count, 0)::bigint as row_count,
    coalesce(target_counts.distinct_event_count, 0)::bigint as distinct_event_count,
    coalesce(source_counts.expected_excluded_count, 0)::bigint as expected_excluded_count,
    0::bigint as expected_rejected_count,
    0::bigint as expected_dlq_count,
    0::bigint as expected_deduplicated_replay_count,
    0::bigint as expected_suppression_count,
    coalesce(target_counts.missing_business_key_count, 0)::bigint as missing_business_key_count
  from source_counts
  full outer join target_counts
    on source_counts.source_model = target_counts.source_model
   and source_counts.reporting_date = target_counts.reporting_date
)

select
  source_model,
  reporting_date,
  reporting_timezone,
  accounting_scope,
  source_relation,
  target_relation,
  accepted_source_count,
  accepted_source_distinct_event_count,
  row_count,
  distinct_event_count,
  expected_excluded_count,
  expected_rejected_count,
  expected_dlq_count,
  expected_deduplicated_replay_count,
  expected_suppression_count,
  missing_business_key_count,
  (
    accepted_source_count
    - row_count
    - expected_excluded_count
    - expected_rejected_count
    - expected_dlq_count
    - expected_deduplicated_replay_count
    - expected_suppression_count
  )::bigint as unexplained_delta,
  case
    when (
      accepted_source_count
      - row_count
      - expected_excluded_count
      - expected_rejected_count
      - expected_dlq_count
      - expected_deduplicated_replay_count
      - expected_suppression_count
    ) = 0
      and missing_business_key_count = 0
      then 'explained'
    else 'unexplained_delta'
  end as reconciliation_status
from reconciled
