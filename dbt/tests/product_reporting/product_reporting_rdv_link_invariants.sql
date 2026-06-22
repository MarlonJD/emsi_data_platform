{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

with link_rows as (
  select
    'link_reporting_reaction_content'::text as model_name,
    link_hk::text as link_hk,
    concat_ws('||', reaction_hk::text, content_hk::text) as participant_signature,
    load_datetime,
    record_source::text as record_source
  from {{ ref("link_reporting_reaction_content") }}

  union all

  select
    'link_reporting_feed_item_content'::text as model_name,
    link_hk::text as link_hk,
    concat_ws('||', feed_item_hk::text, content_hk::text) as participant_signature,
    load_datetime,
    record_source::text as record_source
  from {{ ref("link_reporting_feed_item_content") }}

  union all

  select
    'l_user_channel_session'::text as model_name,
    link_hk::text as link_hk,
    concat_ws('||', subject_user_hk::text, channel_hk::text, channel_session_hk::text) as participant_signature,
    load_datetime,
    record_source::text as record_source
  from {{ ref("l_user_channel_session") }}

  union all

  select
    'l_event_participant'::text as model_name,
    link_hk::text as link_hk,
    concat_ws('||', subject_user_hk::text, event_hk::text, event_funnel_action_hk::text) as participant_signature,
    load_datetime,
    record_source::text as record_source
  from {{ ref("l_event_participant") }}

  union all

  select
    'l_together_actor_target'::text as model_name,
    link_hk::text as link_hk,
    concat_ws('||', subject_user_hk::text, together_item_hk::text, together_target_hk::text) as participant_signature,
    load_datetime,
    record_source::text as record_source
  from {{ ref("l_together_actor_target") }}
),

required_participants as (
  select
    'link_reporting_reaction_content'::text as model_name,
    link_hk::text as link_hk,
    'reaction_hk'::text as participant_role,
    reaction_hk::text as participant_hk
  from {{ ref("link_reporting_reaction_content") }}

  union all

  select
    'link_reporting_reaction_content'::text,
    link_hk::text,
    'content_hk'::text,
    content_hk::text
  from {{ ref("link_reporting_reaction_content") }}

  union all

  select
    'link_reporting_feed_item_content'::text,
    link_hk::text,
    'feed_item_hk'::text,
    feed_item_hk::text
  from {{ ref("link_reporting_feed_item_content") }}

  union all

  select
    'link_reporting_feed_item_content'::text,
    link_hk::text,
    'content_hk'::text,
    content_hk::text
  from {{ ref("link_reporting_feed_item_content") }}

  union all

  select
    'l_user_channel_session'::text,
    link_hk::text,
    'subject_user_hk'::text,
    subject_user_hk::text
  from {{ ref("l_user_channel_session") }}

  union all

  select
    'l_user_channel_session'::text,
    link_hk::text,
    'channel_hk'::text,
    channel_hk::text
  from {{ ref("l_user_channel_session") }}

  union all

  select
    'l_user_channel_session'::text,
    link_hk::text,
    'channel_session_hk'::text,
    channel_session_hk::text
  from {{ ref("l_user_channel_session") }}

  union all

  select
    'l_event_participant'::text,
    link_hk::text,
    'subject_user_hk'::text,
    subject_user_hk::text
  from {{ ref("l_event_participant") }}

  union all

  select
    'l_event_participant'::text,
    link_hk::text,
    'event_hk'::text,
    event_hk::text
  from {{ ref("l_event_participant") }}

  union all

  select
    'l_event_participant'::text,
    link_hk::text,
    'event_funnel_action_hk'::text,
    event_funnel_action_hk::text
  from {{ ref("l_event_participant") }}

  union all

  select
    'l_together_actor_target'::text,
    link_hk::text,
    'subject_user_hk'::text,
    subject_user_hk::text
  from {{ ref("l_together_actor_target") }}

  union all

  select
    'l_together_actor_target'::text,
    link_hk::text,
    'together_item_hk'::text,
    together_item_hk::text
  from {{ ref("l_together_actor_target") }}

  union all

  select
    'l_together_actor_target'::text,
    link_hk::text,
    'together_target_hk'::text,
    together_target_hk::text
  from {{ ref("l_together_actor_target") }}
),

required_participant_failures as (
  select
    model_name,
    coalesce(link_hk, '<null>') as link_hk,
    participant_role,
    coalesce(participant_hk, '<null>') as participant_hk,
    'link_or_participant_key_null'::text as violation,
    count(*) as failing_row_count
  from required_participants
  where link_hk is null
     or participant_hk is null
     or nullif(trim(participant_hk), '') is null
  group by model_name, coalesce(link_hk, '<null>'), participant_role, coalesce(participant_hk, '<null>')
),

link_metadata_failures as (
  select
    model_name,
    coalesce(link_hk, '<null>') as link_hk,
    'link_metadata'::text as participant_role,
    '<metadata>'::text as participant_hk,
    'link_load_metadata_missing'::text as violation,
    count(*) as failing_row_count
  from link_rows
  where load_datetime is null
     or nullif(trim(record_source), '') is null
  group by model_name, coalesce(link_hk, '<null>')
),

duplicate_link_keys as (
  select
    model_name,
    link_hk,
    'link_hk'::text as participant_role,
    '<duplicate>'::text as participant_hk,
    'duplicate_link_hash_key'::text as violation,
    count(*) as failing_row_count
  from link_rows
  group by model_name, link_hk
  having count(*) > 1
),

link_hash_participant_drift as (
  select
    model_name,
    link_hk,
    'participant_signature'::text as participant_role,
    '<multiple>'::text as participant_hk,
    'link_hash_participant_drift'::text as violation,
    count(distinct participant_signature) as failing_row_count
  from link_rows
  group by model_name, link_hk
  having count(distinct participant_signature) > 1
),

participant_orphans as (
  select
    link.model_name,
    link.link_hk,
    link.participant_role,
    link.participant_hk,
    link.violation,
    count(*) as failing_row_count
  from (
    select
      'link_reporting_reaction_content'::text as model_name,
      link.link_hk::text as link_hk,
      'reaction_hk'::text as participant_role,
      link.reaction_hk::text as participant_hk,
      'reaction_hub_missing'::text as violation
    from {{ ref("link_reporting_reaction_content") }} link
    left join {{ ref("hub_reporting_reaction") }} hub
      on link.reaction_hk = hub.reaction_hk
    where hub.reaction_hk is null

    union all

    select
      'link_reporting_reaction_content'::text,
      link.link_hk::text,
      'content_hk'::text,
      link.content_hk::text,
      'content_hub_missing'::text
    from {{ ref("link_reporting_reaction_content") }} link
    left join {{ ref("hub_reporting_content") }} hub
      on link.content_hk = hub.content_hk
    where hub.content_hk is null

    union all

    select
      'link_reporting_feed_item_content'::text,
      link.link_hk::text,
      'feed_item_hk'::text,
      link.feed_item_hk::text,
      'feed_item_hub_missing'::text
    from {{ ref("link_reporting_feed_item_content") }} link
    left join {{ ref("hub_reporting_feed_item") }} hub
      on link.feed_item_hk = hub.feed_item_hk
    where hub.feed_item_hk is null

    union all

    select
      'link_reporting_feed_item_content'::text,
      link.link_hk::text,
      'content_hk'::text,
      link.content_hk::text,
      'content_hub_missing'::text
    from {{ ref("link_reporting_feed_item_content") }} link
    left join {{ ref("hub_reporting_content") }} hub
      on link.content_hk = hub.content_hk
    where hub.content_hk is null

    union all

    select
      'l_user_channel_session'::text,
      link.link_hk::text,
      'channel_hk'::text,
      link.channel_hk::text,
      'channel_hub_missing'::text
    from {{ ref("l_user_channel_session") }} link
    left join {{ ref("h_channel") }} hub
      on link.channel_hk = hub.channel_hk
    where hub.channel_hk is null

    union all

    select
      'l_event_participant'::text,
      link.link_hk::text,
      'event_hk'::text,
      link.event_hk::text,
      'event_hub_missing'::text
    from {{ ref("l_event_participant") }} link
    left join {{ ref("h_event") }} hub
      on link.event_hk = hub.event_hk
    where hub.event_hk is null

    union all

    select
      'l_together_actor_target'::text,
      link.link_hk::text,
      'together_item_hk'::text,
      link.together_item_hk::text,
      'together_item_hub_missing'::text
    from {{ ref("l_together_actor_target") }} link
    left join {{ ref("h_together_item") }} hub
      on link.together_item_hk = hub.together_item_hk
    where hub.together_item_hk is null
  ) link
  group by link.model_name, link.link_hk, link.participant_role, link.participant_hk, link.violation
),

controlled_link_orphan_fixture as (
  {% if var("force_product_reporting_rdv_link_negative_failure", false) %}
  select
    'link_reporting_reaction_content'::text as model_name,
    fixture.link_hk::text as link_hk,
    'content_hk'::text as participant_role,
    fixture.content_hk::text as participant_hk,
    'controlled_link_orphan_fixture'::text as violation,
    count(*) as failing_row_count
  from (
    select
      'controlled_missing_link_hk'::text as link_hk,
      'controlled_missing_content_hk'::text as content_hk
  ) fixture
  left join {{ ref("hub_reporting_content") }} hub
    on fixture.content_hk = hub.content_hk
  where hub.content_hk is null
  group by fixture.link_hk, fixture.content_hk
  {% else %}
  select * from participant_orphans where false
  {% endif %}
)

select * from required_participant_failures
union all
select * from link_metadata_failures
union all
select * from duplicate_link_keys
union all
select * from link_hash_participant_drift
union all
select * from participant_orphans
union all
select * from controlled_link_orphan_fixture
