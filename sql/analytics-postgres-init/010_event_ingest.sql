CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.raw_event_landing (
  event_id TEXT PRIMARY KEY,
  event_name TEXT NOT NULL,
  event_version INTEGER NOT NULL CHECK (event_version > 0),
  occurred_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL,
  producer TEXT NOT NULL,
  privacy_class TEXT NOT NULL CHECK (privacy_class = 'pseudonymous'),
  consent_scope JSONB NOT NULL CHECK (jsonb_typeof(consent_scope) = 'array'),
  subject JSONB NOT NULL CHECK (jsonb_typeof(subject) = 'object'),
  subject_user_hash TEXT NOT NULL CHECK (position('@' IN subject_user_hash) = 0),
  subject_session_id TEXT,
  payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  source_topic TEXT NOT NULL,
  source_partition INTEGER NOT NULL,
  source_offset BIGINT NOT NULL,
  raw_record_sha256 TEXT NOT NULL,
  raw_record_bytes INTEGER NOT NULL CHECK (raw_record_bytes > 0),
  payload_sha256 TEXT NOT NULL,
  landed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS raw_event_landing_source_offset_idx
  ON analytics.raw_event_landing (source_topic, source_partition, source_offset);

CREATE TABLE IF NOT EXISTS analytics.raw_event_dlq (
  dlq_id BIGSERIAL PRIMARY KEY,
  event_id TEXT,
  event_name TEXT,
  source_topic TEXT NOT NULL,
  source_partition INTEGER NOT NULL,
  source_offset BIGINT NOT NULL,
  error_code TEXT NOT NULL,
  error_message TEXT NOT NULL,
  raw_record_sha256 TEXT NOT NULL,
  raw_record_bytes INTEGER NOT NULL CHECK (raw_record_bytes > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS raw_event_dlq_event_id_idx
  ON analytics.raw_event_dlq (event_id)
  WHERE event_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS analytics.event_ingest_checkpoints (
  consumer_group TEXT NOT NULL,
  source_topic TEXT NOT NULL,
  source_partition INTEGER NOT NULL,
  last_offset BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, source_topic, source_partition)
);

CREATE TABLE IF NOT EXISTS analytics.event_ingest_metrics (
  consumer_group TEXT NOT NULL,
  source_topic TEXT NOT NULL,
  accepted_count BIGINT NOT NULL DEFAULT 0,
  rejected_count BIGINT NOT NULL DEFAULT 0,
  last_processed_topic TEXT,
  last_processed_partition INTEGER,
  last_processed_offset BIGINT,
  last_lag BIGINT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, source_topic)
);
