{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

select
  reporting_date,
  community_event_hk as event_hk,
  max(community_event_business_key) as event_business_key,
  event_funnel_action,
  count(*)::bigint as action_count,
  count(distinct subject_user_hk)::bigint as distinct_actor_count,
  count(*) filter (
    where event_funnel_action in (
      'interested',
      'join_attempted',
      'joined',
      'reminder_set',
      'calendar_added',
      'shared',
      'check_in',
      'attendance_confirmed'
    )
  )::bigint as positive_intent_count,
  count(*) filter (
    where event_funnel_action in ('uninterested', 'join_failed', 'join_abandoned', 'left', 'no_show_inferred')
  )::bigint as dropoff_or_negative_count,
  case
    when count(*) filter (where source_completeness_input = 'source_complete') > 0 then 'partial'
    else 'unavailable'
  end as source_completeness_label,
  case
    when count(distinct subject_user_hk) >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  max(load_datetime) as load_datetime,
  'analytics.raw_event_landing'::text as record_source
from {{ ref("s_event_metadata_raw") }}
where community_event_hk is not null
group by reporting_date, community_event_hk, event_funnel_action
