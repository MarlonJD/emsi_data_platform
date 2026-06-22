{{ config(materialized="view", tags=["product_reporting_phase5", "data_vault", "quality"]) }}

{% set evaluated_at_var = var("product_reporting_quality_evaluated_at", none) %}

with settings as (
  select
    {% if evaluated_at_var %}
    timestamp with time zone '{{ evaluated_at_var }}'
    {% else %}
    current_timestamp
    {% endif %} as evaluated_at,
    'Europe/Istanbul'::text as reporting_timezone
),

asset_contracts as (
  select *
  from (
    values
      ('stage.stg_product_reporting_content_events', 'Stage', 'load_time', 'load_datetime', 60, 5, 180, 'one_to_one_partition', true),
      ('stage.stg_product_reporting_reactions', 'Stage', 'load_time', 'load_datetime', 60, 5, 180, 'one_to_one_partition', true),
      ('stage.stg_product_reporting_feed_events', 'Stage', 'load_time', 'load_datetime', 15, 5, 180, 'one_to_one_partition', true),
      ('stage.stg_product_reporting_channel_sessions', 'Stage', 'load_time', 'load_datetime', 60, 5, 180, 'one_to_one_partition', true),
      ('stage.stg_product_reporting_event_funnel', 'Stage', 'load_time', 'load_datetime', 60, 5, 180, 'one_to_one_partition', true),
      ('stage.stg_app_together_items', 'Stage', 'load_time', 'load_datetime', 60, 5, 180, 'one_to_one_partition', true),
      ('raw_vault.sat_reporting_content_event', 'RDV satellite', 'load_time', 'load_datetime', 60, 5, 180, 'one_to_one_partition', true),
      ('raw_vault.sat_reporting_reaction_event', 'RDV satellite', 'load_time', 'load_datetime', 60, 5, 180, 'one_to_one_partition', true),
      ('raw_vault.sat_reporting_feed_serving_event', 'RDV satellite', 'load_time', 'load_datetime', 15, 5, 180, 'one_to_one_partition', true),
      ('raw_vault.s_channel_session_raw', 'RDV satellite', 'load_time', 'load_datetime', 60, 5, 180, 'one_to_one_partition', true),
      ('raw_vault.s_event_metadata_raw', 'RDV satellite', 'load_time', 'load_datetime', 60, 5, 180, 'one_to_one_partition', true),
      ('raw_vault.s_together_metadata_raw', 'RDV satellite', 'load_time', 'load_datetime', 60, 5, 180, 'one_to_one_partition', true),
      ('business_vault.pit_reporting_content_daily', 'BDV daily', 'as_of_time', 'as_of_datetime', 60, 5, 180, 'aggregate_partition', true),
      ('business_vault.br_content_reaction_daily', 'BDV daily', 'load_time', 'load_datetime', 60, 5, 180, 'aggregate_partition', true),
      ('business_vault.s_occupation_cohort_daily', 'BDV daily', 'load_time', 'load_datetime', 1440, 5, 180, 'aggregate_partition', true),
      ('business_vault.s_content_performance_daily', 'BDV daily', 'load_time', 'load_datetime', 60, 5, 180, 'aggregate_partition', true),
      ('business_vault.s_emoji_usage_daily', 'BDV daily', 'load_time', 'load_datetime', 60, 5, 180, 'aggregate_partition', true),
      ('business_vault.s_reaction_valence_daily', 'BDV daily', 'load_time', 'load_datetime', 1440, 5, 180, 'aggregate_partition', true),
      ('business_vault.br_feed_interest_proxy', 'BDV daily', 'load_time', 'load_datetime', 60, 5, 180, 'aggregate_partition', true),
      ('business_vault.s_channel_session_daily', 'BDV daily', 'load_time', 'load_datetime', 1440, 5, 180, 'aggregate_partition', true),
      ('business_vault.pit_event_daily', 'BDV daily', 'load_time', 'load_datetime', 1440, 5, 180, 'aggregate_partition', true),
      ('business_vault.br_event_funnel', 'BDV daily', 'load_time', 'load_datetime', 1440, 5, 180, 'aggregate_partition', true),
      ('business_vault.s_event_funnel_daily', 'BDV daily', 'load_time', 'load_datetime', 1440, 5, 180, 'aggregate_partition', true),
      ('business_vault.pit_together_daily', 'BDV daily', 'load_time', 'load_datetime', 1440, 5, 180, 'aggregate_partition', true),
      ('business_vault.br_together_response_flow', 'BDV daily', 'load_time', 'load_datetime', 1440, 5, 180, 'aggregate_partition', true),
      ('business_vault.s_together_coordination_daily', 'BDV daily', 'load_time', 'load_datetime', 1440, 5, 180, 'aggregate_partition', true),
      ('mart.mart_product_reporting_occupation_cohort_daily', 'PL mart', 'mart_complete_through_time', 'refreshed_at', 1440, 5, 180, 'reportable_partition', true),
      ('mart.mart_product_reporting_content_performance_daily', 'PL mart', 'mart_complete_through_time', 'refreshed_at', 60, 5, 180, 'reportable_partition', true),
      ('mart.mart_product_reporting_emoji_reaction_daily', 'PL mart', 'mart_complete_through_time', 'refreshed_at', 60, 5, 180, 'reportable_partition', true),
      ('mart.mart_product_reporting_reaction_valence_daily', 'PL mart', 'mart_complete_through_time', 'refreshed_at', 1440, 5, 180, 'reportable_partition', true),
      ('mart.mart_product_reporting_feed_interest_proxy_daily', 'PL mart', 'mart_complete_through_time', 'refreshed_at', 60, 5, 180, 'reportable_partition', true),
      ('mart.mart_product_reporting_together_coordination_daily', 'PL mart', 'mart_complete_through_time', 'refreshed_at', 1440, 5, 180, 'reportable_partition', true)
  ) as contract(
    asset_key,
    asset_layer,
    freshness_evaluation_basis,
    freshness_timestamp_field,
    freshness_target_minutes,
    max_clock_skew_minutes,
    late_arrival_grace_minutes,
    expected_minimum_mode,
    asset_required_for_publication
  )
),

raw_landing as (
  select
    event_id,
    event_name,
    occurred_at,
    received_at,
    landed_at,
    payload
  from {{ source("analytics", "raw_event_landing") }}
),

stage_source_observations as (
  select
    'stage.stg_product_reporting_content_events'::text as asset_key,
    (occurred_at at time zone 'Europe/Istanbul')::date as reporting_date,
    count(*)::bigint as upstream_activity_count,
    (
      count(*)
      - count(*) filter (
        where coalesce(payload ->> 'post_id', payload ->> 'item_id', payload ->> 'entity_id') is null
      )
    )::bigint as expected_target_min_row_count,
    count(distinct event_id)::bigint as source_distinct_key_count,
    count(distinct event_name)::bigint as source_event_class_count,
    max(occurred_at) as latest_source_event_at,
    max(received_at) as latest_source_received_at,
    max(landed_at) as latest_source_load_datetime
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
    'stage.stg_product_reporting_reactions'::text,
    (occurred_at at time zone 'Europe/Istanbul')::date,
    count(*)::bigint,
    (
      count(*)
      - count(*) filter (
        where coalesce(payload ->> 'post_id', payload ->> 'item_id', payload ->> 'entity_id') is null
      )
    )::bigint,
    count(distinct event_id)::bigint,
    count(distinct event_name)::bigint,
    max(occurred_at),
    max(received_at),
    max(landed_at)
  from raw_landing
  where event_name in ('reaction_added', 'reaction_removed', 'feed_item_like')
  group by 1, 2

  union all

  select
    'stage.stg_product_reporting_feed_events'::text,
    (occurred_at at time zone 'Europe/Istanbul')::date,
    count(*)::bigint,
    (
      count(*)
      - count(*) filter (
        where coalesce(payload ->> 'item_id', payload ->> 'entity_id') is null
      )
    )::bigint,
    count(distinct event_id)::bigint,
    count(distinct event_name)::bigint,
    max(occurred_at),
    max(received_at),
    max(landed_at)
  from raw_landing
  where event_name like 'feed_item_%'
  group by 1, 2

  union all

  select
    'stage.stg_product_reporting_channel_sessions'::text,
    (occurred_at at time zone 'Europe/Istanbul')::date,
    count(*)::bigint,
    (count(*) - count(*) filter (where nullif(payload ->> 'channel_id', '') is null))::bigint,
    count(distinct event_id)::bigint,
    count(distinct event_name)::bigint,
    max(occurred_at),
    max(received_at),
    max(landed_at)
  from raw_landing
  where event_name in ('channel_session_started', 'channel_session_ended')
  group by 1, 2

  union all

  select
    'stage.stg_product_reporting_event_funnel'::text,
    (occurred_at at time zone 'Europe/Istanbul')::date,
    count(*)::bigint,
    (
      count(*)
      - count(*) filter (
        where coalesce(payload ->> 'event_id', payload ->> 'community_event_id', payload ->> 'entity_id') is null
      )
    )::bigint,
    count(distinct event_id)::bigint,
    count(distinct event_name)::bigint,
    max(occurred_at),
    max(received_at),
    max(landed_at)
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
    'stage.stg_app_together_items'::text,
    (occurred_at at time zone 'Europe/Istanbul')::date,
    count(*)::bigint,
    count(*)::bigint,
    count(distinct event_id)::bigint,
    count(distinct event_name)::bigint,
    max(occurred_at),
    max(received_at),
    max(landed_at)
  from raw_landing
  where event_name like 'together_%'
  group by 1, 2
),

stage_target_observations as (
  select
    'stage.stg_product_reporting_content_events'::text as asset_key,
    reporting_date,
    count(*)::bigint as observed_row_count,
    count(distinct event_id)::bigint as observed_distinct_key_count,
    (count(*) - count(distinct event_id))::bigint as duplicate_final_grain_count,
    max(occurred_at) as latest_target_event_at,
    max(load_datetime) as latest_target_load_datetime
  from {{ ref("stg_product_reporting_content_events") }}
  group by 1, 2

  union all

  select
    'stage.stg_product_reporting_reactions'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct event_id)::bigint,
    (count(*) - count(distinct event_id))::bigint,
    max(occurred_at),
    max(load_datetime)
  from {{ ref("stg_product_reporting_reactions") }}
  group by 1, 2

  union all

  select
    'stage.stg_product_reporting_feed_events'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct event_id)::bigint,
    (count(*) - count(distinct event_id))::bigint,
    max(occurred_at),
    max(load_datetime)
  from {{ ref("stg_product_reporting_feed_events") }}
  group by 1, 2

  union all

  select
    'stage.stg_product_reporting_channel_sessions'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct event_id)::bigint,
    (count(*) - count(distinct event_id))::bigint,
    max(occurred_at),
    max(load_datetime)
  from {{ ref("stg_product_reporting_channel_sessions") }}
  group by 1, 2

  union all

  select
    'stage.stg_product_reporting_event_funnel'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct event_id)::bigint,
    (count(*) - count(distinct event_id))::bigint,
    max(occurred_at),
    max(load_datetime)
  from {{ ref("stg_product_reporting_event_funnel") }}
  group by 1, 2

  union all

  select
    'stage.stg_app_together_items'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct event_id)::bigint,
    (count(*) - count(distinct event_id))::bigint,
    max(occurred_at),
    max(load_datetime)
  from {{ ref("stg_app_together_items") }}
  group by 1, 2
),

rdv_source_observations as (
  select
    'raw_vault.sat_reporting_content_event'::text as asset_key,
    reporting_date,
    count(*)::bigint as upstream_activity_count,
    count(*)::bigint as expected_target_min_row_count,
    count(distinct event_id)::bigint as source_distinct_key_count,
    count(distinct event_name)::bigint as source_event_class_count,
    max(occurred_at) as latest_source_event_at,
    max(received_at) as latest_source_received_at,
    max(load_datetime) as latest_source_load_datetime
  from {{ ref("stg_product_reporting_content_events") }}
  group by 1, 2

  union all

  select
    'raw_vault.sat_reporting_reaction_event'::text,
    reporting_date,
    count(*)::bigint,
    count(*)::bigint,
    count(distinct event_id)::bigint,
    count(distinct event_name)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("stg_product_reporting_reactions") }}
  group by 1, 2

  union all

  select
    'raw_vault.sat_reporting_feed_serving_event'::text,
    reporting_date,
    count(*)::bigint,
    count(*)::bigint,
    count(distinct event_id)::bigint,
    count(distinct event_name)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("stg_product_reporting_feed_events") }}
  group by 1, 2

  union all

  select
    'raw_vault.s_channel_session_raw'::text,
    reporting_date,
    count(*)::bigint,
    count(*)::bigint,
    count(distinct event_id)::bigint,
    count(distinct channel_session_action)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("stg_product_reporting_channel_sessions") }}
  group by 1, 2

  union all

  select
    'raw_vault.s_event_metadata_raw'::text,
    reporting_date,
    count(*)::bigint,
    count(*)::bigint,
    count(distinct event_id)::bigint,
    count(distinct event_funnel_action)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("stg_product_reporting_event_funnel") }}
  group by 1, 2

  union all

  select
    'raw_vault.s_together_metadata_raw'::text,
    reporting_date,
    count(*)::bigint,
    count(*)::bigint,
    count(distinct event_id)::bigint,
    count(distinct together_action)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("stg_app_together_items") }}
  group by 1, 2
),

rdv_target_observations as (
  select
    'raw_vault.sat_reporting_content_event'::text as asset_key,
    reporting_date,
    count(*)::bigint as observed_row_count,
    count(distinct content_hk)::bigint as observed_distinct_key_count,
    (
      count(*)
      - count(distinct concat_ws('||', content_hk::text, source_topic::text, source_partition::text, source_offset::text))
    )::bigint as duplicate_final_grain_count,
    max(occurred_at) as latest_target_event_at,
    max(load_datetime) as latest_target_load_datetime
  from {{ ref("sat_reporting_content_event") }}
  group by 1, 2

  union all

  select
    'raw_vault.sat_reporting_reaction_event'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct reaction_hk)::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', reaction_hk::text, source_topic::text, source_partition::text, source_offset::text))
    )::bigint,
    max(occurred_at),
    max(load_datetime)
  from {{ ref("sat_reporting_reaction_event") }}
  group by 1, 2

  union all

  select
    'raw_vault.sat_reporting_feed_serving_event'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct feed_item_hk)::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', feed_item_hk::text, source_topic::text, source_partition::text, source_offset::text))
    )::bigint,
    max(occurred_at),
    max(load_datetime)
  from {{ ref("sat_reporting_feed_serving_event") }}
  group by 1, 2

  union all

  select
    'raw_vault.s_channel_session_raw'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct channel_session_hk)::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', channel_session_hk::text, source_topic::text, source_partition::text, source_offset::text))
    )::bigint,
    max(occurred_at),
    max(load_datetime)
  from {{ ref("s_channel_session_raw") }}
  group by 1, 2

  union all

  select
    'raw_vault.s_event_metadata_raw'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct event_funnel_action_hk)::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', event_funnel_action_hk::text, source_topic::text, source_partition::text, source_offset::text))
    )::bigint,
    max(occurred_at),
    max(load_datetime)
  from {{ ref("s_event_metadata_raw") }}
  group by 1, 2

  union all

  select
    'raw_vault.s_together_metadata_raw'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct together_item_hk)::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', together_item_hk::text, source_topic::text, source_partition::text, source_offset::text))
    )::bigint,
    max(occurred_at),
    max(load_datetime)
  from {{ ref("s_together_metadata_raw") }}
  group by 1, 2
),

bdv_source_observations as (
  select
    'business_vault.pit_reporting_content_daily'::text as asset_key,
    reporting_date,
    count(*)::bigint as upstream_activity_count,
    case when count(*) > 0 then 1::bigint else 0::bigint end as expected_target_min_row_count,
    count(distinct content_hk)::bigint as source_distinct_key_count,
    count(distinct event_name)::bigint as source_event_class_count,
    max(occurred_at) as latest_source_event_at,
    max(received_at) as latest_source_received_at,
    max(load_datetime) as latest_source_load_datetime
  from {{ ref("sat_reporting_content_event") }}
  group by 1, 2

  union all

  select
    'business_vault.s_content_performance_daily'::text,
    reporting_date,
    count(*)::bigint,
    case when count(*) > 0 then 1::bigint else 0::bigint end,
    count(distinct content_hk)::bigint,
    count(distinct event_name)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("sat_reporting_content_event") }}
  group by 1, 2

  union all

  select
    'business_vault.br_content_reaction_daily'::text,
    reporting_date,
    count(*)::bigint,
    case when count(*) > 0 then 1::bigint else 0::bigint end,
    count(distinct reaction_hk)::bigint,
    count(distinct reaction_action)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("sat_reporting_reaction_event") }}
  group by 1, 2

  union all

  select
    'business_vault.s_occupation_cohort_daily'::text,
    reporting_date,
    count(*)::bigint,
    case when count(*) > 0 then 1::bigint else 0::bigint end,
    count(distinct subject_user_hk)::bigint,
    count(distinct occupation_cohort_key)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from (
    select reporting_date, subject_user_hk, occupation_cohort_key, occurred_at, received_at, load_datetime
    from {{ ref("sat_reporting_content_event") }}
    union all
    select reporting_date, subject_user_hk, occupation_cohort_key, occurred_at, received_at, load_datetime
    from {{ ref("sat_reporting_reaction_event") }}
    union all
    select reporting_date, subject_user_hk, occupation_cohort_key, occurred_at, received_at, load_datetime
    from {{ ref("sat_reporting_feed_serving_event") }}
  ) actor_source
  group by 1, 2

  union all

  select
    'business_vault.s_emoji_usage_daily'::text,
    reporting_date,
    count(*)::bigint,
    case when count(*) > 0 then 1::bigint else 0::bigint end,
    count(distinct reaction_hk)::bigint,
    count(distinct emoji_key)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("sat_reporting_reaction_event") }}
  group by 1, 2

  union all

  select
    'business_vault.s_reaction_valence_daily'::text,
    reporting_date,
    count(*)::bigint,
    case when count(*) > 0 then 1::bigint else 0::bigint end,
    count(distinct reaction_hk)::bigint,
    count(distinct reaction_valence)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("sat_reporting_reaction_event") }}
  group by 1, 2

  union all

  select
    'business_vault.br_feed_interest_proxy'::text,
    reporting_date,
    count(*)::bigint,
    case when count(*) > 0 then 1::bigint else 0::bigint end,
    count(distinct feed_item_hk)::bigint,
    count(distinct event_name)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("sat_reporting_feed_serving_event") }}
  group by 1, 2

  union all

  select
    'business_vault.s_channel_session_daily'::text,
    reporting_date,
    count(*)::bigint,
    case when count(*) > 0 then 1::bigint else 0::bigint end,
    count(distinct channel_session_hk)::bigint,
    count(distinct channel_session_action)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("s_channel_session_raw") }}
  group by 1, 2

  union all

  select
    'business_vault.pit_event_daily'::text,
    reporting_date,
    count(*)::bigint,
    case when count(*) > 0 then 1::bigint else 0::bigint end,
    count(distinct community_event_hk)::bigint,
    count(distinct event_funnel_action)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("s_event_metadata_raw") }}
  group by 1, 2

  union all

  select
    'business_vault.br_event_funnel'::text,
    reporting_date,
    count(*)::bigint,
    case when count(*) > 0 then 1::bigint else 0::bigint end,
    count(distinct event_funnel_action_hk)::bigint,
    count(distinct event_funnel_action)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("s_event_metadata_raw") }}
  group by 1, 2

  union all

  select
    'business_vault.s_event_funnel_daily'::text,
    reporting_date,
    count(*)::bigint,
    case when count(*) > 0 then 1::bigint else 0::bigint end,
    count(distinct event_funnel_action_hk)::bigint,
    count(distinct event_funnel_action)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("s_event_metadata_raw") }}
  group by 1, 2

  union all

  select
    'business_vault.pit_together_daily'::text,
    reporting_date,
    count(*)::bigint,
    case when count(*) > 0 then 1::bigint else 0::bigint end,
    count(distinct together_item_hk)::bigint,
    count(distinct together_action)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("s_together_metadata_raw") }}
  group by 1, 2

  union all

  select
    'business_vault.br_together_response_flow'::text,
    reporting_date,
    count(*)::bigint,
    case when count(*) > 0 then 1::bigint else 0::bigint end,
    count(distinct together_item_hk)::bigint,
    count(distinct together_action)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("s_together_metadata_raw") }}
  group by 1, 2

  union all

  select
    'business_vault.s_together_coordination_daily'::text,
    reporting_date,
    count(*)::bigint,
    case when count(*) > 0 then 1::bigint else 0::bigint end,
    count(distinct together_item_hk)::bigint,
    count(distinct together_action)::bigint,
    max(occurred_at),
    max(received_at),
    max(load_datetime)
  from {{ ref("s_together_metadata_raw") }}
  group by 1, 2
),

bdv_target_observations as (
  select
    'business_vault.pit_reporting_content_daily'::text as asset_key,
    reporting_date,
    count(*)::bigint as observed_row_count,
    count(distinct content_hk)::bigint as observed_distinct_key_count,
    (count(*) - count(distinct concat_ws('||', content_hk::text, reporting_date::text)))::bigint
      as duplicate_final_grain_count,
    max(latest_event_at) as latest_target_event_at,
    max(as_of_datetime) as latest_target_load_datetime
  from {{ ref("pit_reporting_content_daily") }}
  group by 1, 2

  union all

  select
    'business_vault.br_content_reaction_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct content_hk)::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', reporting_date::text, content_hk::text, reaction_key::text, emoji_key::text, reaction_valence::text))
    )::bigint,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("br_content_reaction_daily") }}
  group by 1, 2

  union all

  select
    'business_vault.s_occupation_cohort_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct occupation_cohort_key)::bigint,
    (count(*) - count(distinct concat_ws('||', reporting_date::text, occupation_cohort_key::text)))::bigint,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("s_occupation_cohort_daily") }}
  group by 1, 2

  union all

  select
    'business_vault.s_content_performance_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct content_hk)::bigint,
    (count(*) - count(distinct concat_ws('||', reporting_date::text, content_hk::text)))::bigint,
    max(latest_event_at),
    max(load_datetime)
  from {{ ref("s_content_performance_daily") }}
  group by 1, 2

  union all

  select
    'business_vault.s_emoji_usage_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct emoji_key)::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', reporting_date::text, emoji_key::text, occupation_cohort_key::text))
    )::bigint,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("s_emoji_usage_daily") }}
  group by 1, 2

  union all

  select
    'business_vault.s_reaction_valence_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct reaction_valence)::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', reporting_date::text, reaction_valence::text, occupation_cohort_key::text))
    )::bigint,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("s_reaction_valence_daily") }}
  group by 1, 2

  union all

  select
    'business_vault.br_feed_interest_proxy'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct feed_item_hk)::bigint,
    (count(*) - count(distinct concat_ws('||', reporting_date::text, feed_item_hk::text, content_business_key::text)))::bigint,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("br_feed_interest_proxy") }}
  group by 1, 2

  union all

  select
    'business_vault.s_channel_session_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct channel_hk)::bigint,
    (count(*) - count(distinct concat_ws('||', reporting_date::text, channel_hk::text)))::bigint,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("s_channel_session_daily") }}
  group by 1, 2

  union all

  select
    'business_vault.pit_event_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct event_hk)::bigint,
    (count(*) - count(distinct concat_ws('||', reporting_date::text, event_hk::text)))::bigint,
    max(latest_event_at),
    max(load_datetime)
  from {{ ref("pit_event_daily") }}
  group by 1, 2

  union all

  select
    'business_vault.br_event_funnel'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct event_hk)::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', reporting_date::text, event_hk::text, event_funnel_action::text))
    )::bigint,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("br_event_funnel") }}
  group by 1, 2

  union all

  select
    'business_vault.s_event_funnel_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct event_funnel_action)::bigint,
    (count(*) - count(distinct concat_ws('||', reporting_date::text, event_funnel_action::text)))::bigint,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("s_event_funnel_daily") }}
  group by 1, 2

  union all

  select
    'business_vault.pit_together_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct together_item_hk)::bigint,
    (count(*) - count(distinct concat_ws('||', reporting_date::text, together_item_hk::text)))::bigint,
    max(latest_event_at),
    max(load_datetime)
  from {{ ref("pit_together_daily") }}
  group by 1, 2

  union all

  select
    'business_vault.br_together_response_flow'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct together_item_hk)::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', reporting_date::text, together_item_hk::text, response_type::text, together_status::text))
    )::bigint,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("br_together_response_flow") }}
  group by 1, 2

  union all

  select
    'business_vault.s_together_coordination_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct concat_ws('||', activity_type::text, visibility::text, together_status::text, channel_business_key::text))::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', reporting_date::text, activity_type::text, visibility::text, together_status::text, channel_business_key::text))
    )::bigint,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("s_together_coordination_daily") }}
  group by 1, 2
),

pl_source_observations as (
  select
    'mart.mart_product_reporting_occupation_cohort_daily'::text as asset_key,
    reporting_date,
    count(*)::bigint as upstream_activity_count,
    count(*) filter (where small_cell_suppression_status = 'reportable')::bigint as expected_target_min_row_count,
    count(distinct occupation_cohort_key)::bigint as source_distinct_key_count,
    count(distinct source_completeness_label)::bigint as source_event_class_count,
    null::timestamp with time zone as latest_source_event_at,
    null::timestamp with time zone as latest_source_received_at,
    max(load_datetime) as latest_source_load_datetime
  from {{ ref("s_occupation_cohort_daily") }}
  group by 1, 2

  union all

  select
    'mart.mart_product_reporting_content_performance_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(*) filter (where small_cell_suppression_status = 'reportable')::bigint,
    count(distinct content_hk)::bigint,
    count(distinct source_completeness_label)::bigint,
    max(latest_event_at),
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("s_content_performance_daily") }}
  group by 1, 2

  union all

  select
    'mart.mart_product_reporting_emoji_reaction_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(*) filter (where small_cell_suppression_status = 'reportable')::bigint,
    count(distinct emoji_key)::bigint,
    count(distinct source_completeness_label)::bigint,
    null::timestamp with time zone,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("s_emoji_usage_daily") }}
  group by 1, 2

  union all

  select
    'mart.mart_product_reporting_reaction_valence_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(*) filter (where small_cell_suppression_status = 'reportable')::bigint,
    count(distinct reaction_valence)::bigint,
    count(distinct source_completeness_label)::bigint,
    null::timestamp with time zone,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("s_reaction_valence_daily") }}
  group by 1, 2

  union all

  select
    'mart.mart_product_reporting_feed_interest_proxy_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(*) filter (where small_cell_suppression_status = 'reportable')::bigint,
    count(distinct feed_item_hk)::bigint,
    count(distinct source_completeness_label)::bigint,
    null::timestamp with time zone,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("br_feed_interest_proxy") }}
  group by 1, 2

  union all

  select
    'mart.mart_product_reporting_together_coordination_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(*) filter (where small_cell_suppression_status = 'reportable')::bigint,
    count(distinct concat_ws('||', activity_type::text, visibility::text, together_status::text, channel_business_key::text))::bigint,
    count(distinct source_completeness_label)::bigint,
    null::timestamp with time zone,
    null::timestamp with time zone,
    max(load_datetime)
  from {{ ref("s_together_coordination_daily") }}
  group by 1, 2
),

pl_target_observations as (
  select
    'mart.mart_product_reporting_occupation_cohort_daily'::text as asset_key,
    reporting_date,
    count(*)::bigint as observed_row_count,
    count(distinct occupation_cohort_key)::bigint as observed_distinct_key_count,
    (count(*) - count(distinct concat_ws('||', reporting_date::text, occupation_cohort_key::text)))::bigint
      as duplicate_final_grain_count,
    null::timestamp with time zone as latest_target_event_at,
    max(refreshed_at) as latest_target_load_datetime
  from {{ ref("mart_product_reporting_occupation_cohort_daily") }}
  group by 1, 2

  union all

  select
    'mart.mart_product_reporting_content_performance_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct content_reporting_key)::bigint,
    (count(*) - count(distinct concat_ws('||', reporting_date::text, content_reporting_key::text)))::bigint,
    max(latest_event_at),
    max(refreshed_at)
  from {{ ref("mart_product_reporting_content_performance_daily") }}
  group by 1, 2

  union all

  select
    'mart.mart_product_reporting_emoji_reaction_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct emoji_key)::bigint,
    (count(*) - count(distinct concat_ws('||', reporting_date::text, emoji_key::text, occupation_cohort_key::text)))::bigint,
    null::timestamp with time zone,
    max(refreshed_at)
  from {{ ref("mart_product_reporting_emoji_reaction_daily") }}
  group by 1, 2

  union all

  select
    'mart.mart_product_reporting_reaction_valence_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct reaction_valence)::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', reporting_date::text, reaction_valence::text, occupation_cohort_key::text))
    )::bigint,
    null::timestamp with time zone,
    max(refreshed_at)
  from {{ ref("mart_product_reporting_reaction_valence_daily") }}
  group by 1, 2

  union all

  select
    'mart.mart_product_reporting_feed_interest_proxy_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct feed_item_reporting_key)::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', reporting_date::text, feed_item_reporting_key::text, content_business_key::text))
    )::bigint,
    null::timestamp with time zone,
    max(refreshed_at)
  from {{ ref("mart_product_reporting_feed_interest_proxy_daily") }}
  group by 1, 2

  union all

  select
    'mart.mart_product_reporting_together_coordination_daily'::text,
    reporting_date,
    count(*)::bigint,
    count(distinct concat_ws('||', activity_type::text, visibility::text, together_status::text, channel_business_key::text))::bigint,
    (
      count(*)
      - count(distinct concat_ws('||', reporting_date::text, activity_type::text, visibility::text, together_status::text, channel_business_key::text))
    )::bigint,
    null::timestamp with time zone,
    max(refreshed_at)
  from {{ ref("mart_product_reporting_together_coordination_daily") }}
  group by 1, 2
),

source_observations as (
  select * from stage_source_observations
  union all
  select * from rdv_source_observations
  union all
  select * from bdv_source_observations
  union all
  select * from pl_source_observations
),

target_observations as (
  select * from stage_target_observations
  union all
  select * from rdv_target_observations
  union all
  select * from bdv_target_observations
  union all
  select * from pl_target_observations
),

partition_observations as (
  select
    coalesce(source_observations.asset_key, target_observations.asset_key) as asset_key,
    coalesce(source_observations.reporting_date, target_observations.reporting_date) as reporting_date,
    coalesce(source_observations.upstream_activity_count, 0)::bigint as upstream_activity_count,
    coalesce(source_observations.expected_target_min_row_count, 0)::bigint as expected_target_min_row_count,
    coalesce(source_observations.source_distinct_key_count, 0)::bigint as source_distinct_key_count,
    coalesce(source_observations.source_event_class_count, 0)::bigint as source_event_class_count,
    source_observations.latest_source_event_at,
    source_observations.latest_source_received_at,
    source_observations.latest_source_load_datetime,
    coalesce(target_observations.observed_row_count, 0)::bigint as observed_row_count,
    coalesce(target_observations.observed_distinct_key_count, 0)::bigint as observed_distinct_key_count,
    coalesce(target_observations.duplicate_final_grain_count, 0)::bigint as duplicate_final_grain_count,
    target_observations.latest_target_event_at,
    target_observations.latest_target_load_datetime
  from source_observations
  full outer join target_observations
    on source_observations.asset_key = target_observations.asset_key
   and source_observations.reporting_date = target_observations.reporting_date
),

asset_signal_times as (
  select asset_key, latest_source_event_at, latest_source_load_datetime as latest_load_datetime
  from source_observations
  union all
  select asset_key, latest_target_event_at, latest_target_load_datetime
  from target_observations
),

asset_freshness as (
  select
    asset_key,
    max(latest_source_event_at) as latest_source_event_at,
    max(latest_load_datetime) as latest_load_datetime
  from asset_signal_times
  group by asset_key
),

partition_with_contract as (
  select
    contract.asset_key,
    partition_observations.reporting_date,
    settings.reporting_timezone,
    contract.asset_layer,
    contract.freshness_evaluation_basis,
    contract.freshness_timestamp_field,
    contract.freshness_target_minutes,
    contract.max_clock_skew_minutes,
    contract.late_arrival_grace_minutes,
    contract.expected_minimum_mode,
    contract.asset_required_for_publication,
    partition_observations.upstream_activity_count,
    partition_observations.expected_target_min_row_count,
    partition_observations.source_distinct_key_count,
    partition_observations.source_event_class_count,
    partition_observations.observed_row_count,
    partition_observations.observed_distinct_key_count,
    partition_observations.duplicate_final_grain_count,
    asset_freshness.latest_source_event_at,
    asset_freshness.latest_load_datetime,
    settings.evaluated_at,
    ((partition_observations.reporting_date + 1)::timestamp at time zone settings.reporting_timezone)
      as partition_complete_after
  from partition_observations
  join asset_contracts contract
    on partition_observations.asset_key = contract.asset_key
  cross join settings
  left join asset_freshness
    on partition_observations.asset_key = asset_freshness.asset_key
),

freshness_and_completeness as (
  select
    *,
    case
      when latest_load_datetime is null then null::bigint
      else floor(extract(epoch from (evaluated_at - latest_load_datetime)) / 60)::bigint
    end as freshness_lag_minutes,
    case
      when evaluated_at <= partition_complete_after + (late_arrival_grace_minutes * interval '1 minute')
        then 'inside_late_arrival_window'
      else 'closed_partition'
    end as late_arrival_state,
    case
      when upstream_activity_count = 0
       and expected_target_min_row_count = 0
       and observed_row_count = 0
        then 'no_activity_partition'
      when upstream_activity_count > 0
       and expected_target_min_row_count = 0
        then 'zero_target_expected_by_contract'
      when expected_minimum_mode = 'reportable_partition'
        then 'reportable_rows_from_bdv'
      when expected_minimum_mode = 'aggregate_partition'
        then 'structural_daily_row_required_when_source_active'
      else 'source_rows_required'
    end as expected_minimum_state
  from partition_with_contract
),

status_evaluation as (
  select
    *,
    case
      when latest_load_datetime is null then 'UNAVAILABLE'
      when latest_load_datetime > evaluated_at + (max_clock_skew_minutes * interval '1 minute') then 'CLOCK_SKEW'
      when freshness_lag_minutes <= freshness_target_minutes then 'FRESH'
      else 'STALE'
    end as freshness_status,
    case
      when latest_load_datetime is null then 'no_freshness_timestamp'
      when latest_load_datetime > evaluated_at + (max_clock_skew_minutes * interval '1 minute')
        then 'latest_load_after_evaluation_clock_skew'
      when freshness_lag_minutes <= freshness_target_minutes then 'within_target'
      else 'freshness_target_exceeded'
    end as freshness_reason_code,
    case
      when upstream_activity_count = 0
       and expected_target_min_row_count = 0
       and observed_row_count = 0
        then 'EXPECTED_EMPTY'
      when upstream_activity_count > 0
       and expected_target_min_row_count = 0
       and observed_row_count = 0
        then 'EXPECTED_EMPTY'
      when expected_target_min_row_count > 0
       and observed_row_count = 0
       and evaluated_at <= partition_complete_after + (late_arrival_grace_minutes * interval '1 minute')
        then 'LATE_OPEN'
      when expected_target_min_row_count > 0
       and observed_row_count = 0
        then 'MISSING'
      when expected_target_min_row_count > 0
       and observed_row_count < expected_target_min_row_count
       and evaluated_at <= partition_complete_after + (late_arrival_grace_minutes * interval '1 minute')
        then 'LATE_OPEN'
      when expected_target_min_row_count > 0
       and observed_row_count < expected_target_min_row_count
        then 'PARTIAL'
      when observed_row_count > 0
       and observed_distinct_key_count = 0
        then 'PARTIAL'
      when duplicate_final_grain_count > 0
        then 'PARTIAL'
      else 'COMPLETE'
    end as completeness_status,
    case
      when upstream_activity_count = 0
       and expected_target_min_row_count = 0
       and observed_row_count = 0
        then 'expected_empty_proven_by_source_observation'
      when upstream_activity_count > 0
       and expected_target_min_row_count = 0
       and observed_row_count = 0
        then 'expected_exclusion_proven_by_source_contract'
      when expected_target_min_row_count > 0
       and observed_row_count = 0
       and evaluated_at <= partition_complete_after + (late_arrival_grace_minutes * interval '1 minute')
        then 'partition_inside_late_arrival_window'
      when expected_target_min_row_count > 0
       and observed_row_count = 0
        then 'required_partition_missing'
      when expected_target_min_row_count > 0
       and observed_row_count < expected_target_min_row_count
        then 'observed_rows_below_contract_minimum'
      when observed_row_count > 0
       and observed_distinct_key_count = 0
        then 'canonical_key_count_zero'
      when duplicate_final_grain_count > 0
        then 'duplicate_final_grain'
      when upstream_activity_count > 0
       and expected_target_min_row_count = 0
       and observed_row_count = 0
        then 'zero_reportable_rows_expected_by_contract'
      else 'partition_structurally_complete'
    end as completeness_reason_code
  from freshness_and_completeness
),

trust_evaluation as (
  select
    *,
    max(
      case
        when completeness_status in ('COMPLETE', 'EXPECTED_EMPTY') then reporting_date
      end
    ) over (partition by asset_key) as complete_through_date,
    case
      when completeness_status = 'EXPECTED_EMPTY' then 'EXPECTED_EMPTY'
      when completeness_status = 'LATE_OPEN' then 'LATE_OPEN'
      when freshness_status = 'FRESH' and completeness_status = 'COMPLETE' then 'TRUSTED'
      when freshness_status = 'STALE' and completeness_status = 'COMPLETE' then 'COMPLETE_BUT_STALE'
      when freshness_status = 'FRESH' and completeness_status in ('PARTIAL', 'MISSING') then 'FRESH_BUT_PARTIAL'
      when freshness_status in ('UNAVAILABLE', 'CLOCK_SKEW') then 'UNAVAILABLE'
      else 'FAILED'
    end as trust_status
  from status_evaluation
)

select
  asset_key,
  reporting_date::text as partition_key,
  reporting_date,
  reporting_timezone,
  asset_layer,
  freshness_evaluation_basis,
  freshness_timestamp_field,
  freshness_target_minutes,
  max_clock_skew_minutes,
  late_arrival_grace_minutes,
  latest_source_event_at,
  latest_load_datetime,
  case
    when complete_through_date is null then null::timestamp with time zone
    else ((complete_through_date + 1)::timestamp at time zone reporting_timezone)
  end as complete_through,
  freshness_lag_minutes,
  freshness_status,
  freshness_reason_code,
  completeness_status,
  completeness_reason_code,
  upstream_activity_count,
  expected_target_min_row_count,
  observed_row_count,
  observed_distinct_key_count,
  duplicate_final_grain_count,
  source_distinct_key_count,
  source_event_class_count,
  expected_minimum_state,
  late_arrival_state,
  trust_status,
  case
    when trust_status = 'TRUSTED' then 'fresh_and_complete'
    when trust_status = 'EXPECTED_EMPTY' then 'expected_empty_partition'
    when trust_status = 'LATE_OPEN' then 'late_arrival_window_open'
    when trust_status = 'COMPLETE_BUT_STALE' then 'complete_partition_stale_asset'
    when trust_status = 'FRESH_BUT_PARTIAL' then 'fresh_asset_incomplete_partition'
    when trust_status = 'UNAVAILABLE' then 'freshness_or_clock_unavailable'
    else 'required_trust_boundary_failed'
  end as trust_reason_code,
  asset_required_for_publication,
  evaluated_at
from trust_evaluation
