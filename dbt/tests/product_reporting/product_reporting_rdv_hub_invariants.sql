{{ config(tags=["product_reporting_phase5", "product_reporting_quality"]) }}

with hub_rows as (
  select
    'hub_reporting_content'::text as model_name,
    content_hk::text as hash_key,
    content_business_key::text as business_key,
    load_datetime,
    record_source::text as record_source
  from {{ ref("hub_reporting_content") }}

  union all

  select
    'hub_reporting_reaction'::text as model_name,
    reaction_hk::text as hash_key,
    reaction_business_key::text as business_key,
    load_datetime,
    record_source::text as record_source
  from {{ ref("hub_reporting_reaction") }}

  union all

  select
    'hub_reporting_feed_item'::text as model_name,
    feed_item_hk::text as hash_key,
    feed_item_business_key::text as business_key,
    load_datetime,
    record_source::text as record_source
  from {{ ref("hub_reporting_feed_item") }}

  union all

  select
    'h_channel'::text as model_name,
    channel_hk::text as hash_key,
    channel_business_key::text as business_key,
    load_datetime,
    record_source::text as record_source
  from {{ ref("h_channel") }}

  union all

  select
    'h_event'::text as model_name,
    event_hk::text as hash_key,
    event_business_key::text as business_key,
    load_datetime,
    record_source::text as record_source
  from {{ ref("h_event") }}

  union all

  select
    'h_together_item'::text as model_name,
    together_item_hk::text as hash_key,
    together_item_business_key::text as business_key,
    load_datetime,
    record_source::text as record_source
  from {{ ref("h_together_item") }}
),

null_key_failures as (
  select
    model_name,
    coalesce(hash_key, '<null>') as hash_key,
    coalesce(business_key, '<null>') as business_key,
    'hub_required_field_null'::text as violation,
    count(*) as failing_row_count
  from hub_rows
  where hash_key is null
     or business_key is null
     or nullif(trim(business_key), '') is null
     or load_datetime is null
     or nullif(trim(record_source), '') is null
  group by model_name, coalesce(hash_key, '<null>'), coalesce(business_key, '<null>')
),

duplicate_hash_keys as (
  select
    model_name,
    hash_key,
    '<multiple>'::text as business_key,
    'duplicate_hash_key'::text as violation,
    count(*) as failing_row_count
  from hub_rows
  group by model_name, hash_key
  having count(*) > 1
),

business_key_hash_drift as (
  select
    model_name,
    '<multiple>'::text as hash_key,
    business_key,
    'business_key_hash_drift'::text as violation,
    count(distinct hash_key) as failing_row_count
  from hub_rows
  group by model_name, business_key
  having count(distinct hash_key) > 1
),

hash_key_business_key_drift as (
  select
    model_name,
    hash_key,
    '<multiple>'::text as business_key,
    'hash_key_business_key_drift'::text as violation,
    count(distinct business_key) as failing_row_count
  from hub_rows
  group by model_name, hash_key
  having count(distinct business_key) > 1
)

select * from null_key_failures
union all
select * from duplicate_hash_keys
union all
select * from business_key_hash_drift
union all
select * from hash_key_business_key_drift
