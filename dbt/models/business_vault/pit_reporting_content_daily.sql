{{ config(materialized="view", tags=["product_reporting_phase2", "business_vault"]) }}

with aggregated as (
  select
    content_hk,
    reporting_date,
    max(content_business_key) as content_business_key,
    coalesce(max(content_type), 'unknown') as content_type,
    max(channel_business_key) as channel_business_key,
    coalesce(
      max(occupation_cohort_key) filter (where occupation_cohort_key is not null),
      'unknown'
    ) as occupation_cohort_key,
    min(occurred_at) as first_event_at,
    max(occurred_at) as latest_event_at,
    max(load_datetime) as load_datetime,
    max(load_datetime) as as_of_datetime,
    count(*)::bigint as content_event_count,
    count(distinct subject_user_hk)::bigint as distinct_actor_count,
    count(*) filter (
      where (load_datetime at time zone 'Europe/Istanbul')::date > reporting_date
    )::bigint as late_arriving_event_count,
    count(*) filter (
      where lower(consent_scope::text) not like '%"analytics"%'
         or lower(consent_scope::text) like '%analytics_opt_out%'
         or lower(consent_scope::text) like '%account_deletion%'
         or lower(consent_scope::text) like '%privacy_deletion%'
         or lower(consent_scope::text) like '%deleted%'
         or lower(consent_scope::text) like '%opted_out%'
         or lower(consent_scope::text) like '%opt_out%'
    )::bigint as deleted_or_opted_out_subject_count,
    case
      when count(*) filter (where source_completeness_input is not null) > 0 then 'partial'
      else 'unavailable'
    end as source_completeness_label
  from {{ ref("sat_reporting_content_event") }}
  where content_hk is not null
  group by content_hk, reporting_date
)

select
  content_hk,
  reporting_date,
  content_business_key,
  content_type,
  channel_business_key,
  occupation_cohort_key,
  first_event_at,
  latest_event_at,
  as_of_datetime,
  load_datetime,
  content_event_count,
  distinct_actor_count,
  late_arriving_event_count,
  case
    when late_arriving_event_count > 0 then 'restated_after_reporting_date'
    else 'current_as_of_reporting_date'
  end as restatement_state,
  deleted_or_opted_out_subject_count,
  source_completeness_label,
  case
    when distinct_actor_count >= 10 then 'reportable'
    else 'suppress_before_pl'
  end as small_cell_suppression_status,
  'analytics.raw_event_landing'::text as record_source
from aggregated
