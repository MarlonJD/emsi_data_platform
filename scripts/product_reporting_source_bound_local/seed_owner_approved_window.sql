-- Deterministic, owner-approved, non-sensitive Product Reporting source window
-- for the local-staging-equivalent SOURCE_BOUND_LOCAL evidence run.
--
-- Shape: a single closed reporting day (2 days before run time so the partition
-- is closed) with landed_at = now() so the partition is FRESH + COMPLETE ->
-- TRUSTED. Each cell carries 12 distinct pseudonymous users, clearing the
-- >= 10 distinct-user small-cell suppression threshold so at least the
-- reaction_valence / emoji / occupation_cohort / content_performance marts
-- publish reportable rows. No real user data: synthetic hashes, no '@', a
-- single synthetic post and channel.
--
-- Idempotent for standalone use: prior evidence rows are removed first. The
-- evidence runner additionally backs up and truncates the whole landing so the
-- partition trust invariants reflect only this window, then restores it.

DELETE FROM analytics.raw_event_landing WHERE producer = 'evidence-owner-approved-fixture';

-- 12 distinct-user positive reactions (engineering cohort, emoji=like, post=P1).
INSERT INTO analytics.raw_event_landing
  (event_id, event_name, event_version, occurred_at, received_at, producer, privacy_class,
   consent_scope, subject, subject_user_hash, subject_session_id, payload,
   source_topic, source_partition, source_offset, raw_record_sha256, raw_record_bytes,
   payload_sha256, landed_at)
SELECT
  'evidence-reaction-' || lpad(g::text, 4, '0'),
  'reaction_added', 1,
  (date_trunc('day', now()) - interval '2 days' + interval '10 hours') + (g || ' seconds')::interval,
  (date_trunc('day', now()) - interval '2 days' + interval '10 hours') + (g || ' seconds')::interval + interval '3 seconds',
  'evidence-owner-approved-fixture', 'pseudonymous',
  '["product_reporting"]'::jsonb, '{"type":"user"}'::jsonb,
  'evhash-reaction-user-' || lpad(g::text, 2, '0'), NULL,
  jsonb_build_object('post_id', 'evidence-post-1', 'item_type', 'post', 'emoji_key', 'like',
                     'occupation_cohort_key', 'engineering', 'channel_id', 'evidence-channel-1', 'surface', 'feed'),
  'evidence.product_reporting.v1', 0, g,
  md5('evidence-reaction-' || g), 256, md5('payload-reaction-' || g), now()
FROM generate_series(1, 12) AS g;

-- 12 distinct-user post impressions (same cohort/post) for content_performance coverage.
INSERT INTO analytics.raw_event_landing
  (event_id, event_name, event_version, occurred_at, received_at, producer, privacy_class,
   consent_scope, subject, subject_user_hash, subject_session_id, payload,
   source_topic, source_partition, source_offset, raw_record_sha256, raw_record_bytes,
   payload_sha256, landed_at)
SELECT
  'evidence-impression-' || lpad(g::text, 4, '0'),
  'post_impression', 1,
  (date_trunc('day', now()) - interval '2 days' + interval '11 hours') + (g || ' seconds')::interval,
  (date_trunc('day', now()) - interval '2 days' + interval '11 hours') + (g || ' seconds')::interval + interval '3 seconds',
  'evidence-owner-approved-fixture', 'pseudonymous',
  '["product_reporting"]'::jsonb, '{"type":"user"}'::jsonb,
  'evhash-impression-user-' || lpad(g::text, 2, '0'), NULL,
  jsonb_build_object('post_id', 'evidence-post-1', 'item_type', 'post', 'occupation_cohort_key', 'engineering',
                     'channel_id', 'evidence-channel-1', 'surface', 'feed', 'viewable', true, 'view_duration_ms', 1500),
  'evidence.product_reporting.v1', 1, g,
  md5('evidence-impression-' || g), 256, md5('payload-impression-' || g), now()
FROM generate_series(1, 12) AS g;
