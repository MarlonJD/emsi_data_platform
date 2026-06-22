{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

select
  reporting_date,
  channel_hk,
  max(channel_business_key) as channel_business_key,
  count(*) filter (where channel_session_action = 'started')::bigint as session_started_count,
  count(*) filter (where channel_session_action = 'ended')::bigint as session_ended_count,
  count(distinct channel_session_hk)::bigint as distinct_session_count,
  coalesce(sum(duration_ms), 0)::bigint as duration_ms_total,
  coalesce(sum(posts_seen_count), 0)::bigint as posts_seen_count,
  coalesce(sum(comments_written_count), 0)::bigint as comments_written_count,
  coalesce(sum(reactions_added_count), 0)::bigint as reactions_added_count,
  count(distinct subject_user_hk)::bigint as distinct_actor_count,
  case
    when count(*) filter (where source_completeness_input = 'source_complete') > 0 then 'partial'
    else 'unavailable'
  end as source_completeness_label,
  'channel_session_count_daily,channel_session_duration_ms_daily'::text as metric_contract_ids,
  'direct'::text as wording_status,
  case
    when count(distinct subject_user_hk) >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  max(load_datetime) as load_datetime,
  'analytics.raw_event_landing'::text as record_source
from {{ ref("s_channel_session_raw") }}
where channel_hk is not null
group by reporting_date, channel_hk
