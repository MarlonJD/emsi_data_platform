CREATE DATABASE IF NOT EXISTS emsi_hot_analytics;

CREATE TABLE IF NOT EXISTS emsi_hot_analytics.analytics_events_local_candidate
(
  event_id String,
  event_name LowCardinality(String),
  event_version UInt16,
  occurred_at DateTime64(3, 'UTC'),
  received_at DateTime64(3, 'UTC'),
  producer LowCardinality(String),
  privacy_class LowCardinality(String),
  subject_user_hash String,
  payload_sha256 String,
  raw_record_sha256 String,
  raw_record_bytes UInt32,
  source_topic String,
  source_partition Int32,
  source_offset Int64,
  landed_at DateTime64(3, 'UTC'),
  ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (event_name, occurred_at, event_id);

CREATE TABLE IF NOT EXISTS emsi_hot_analytics.analytics_event_hourly_counts_local_candidate
(
  event_hour DateTime('UTC'),
  event_name LowCardinality(String),
  privacy_class LowCardinality(String),
  event_count UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(event_hour)
ORDER BY (event_hour, event_name, privacy_class);
