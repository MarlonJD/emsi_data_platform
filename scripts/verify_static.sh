#!/usr/bin/env bash
set -euo pipefail

grep -q "POSTGRES_IMAGE=postgres:18.4-alpine3.24" versions.env
grep -q "POSTGRES_VOLUME_TARGET=/var/lib/postgresql" versions.env
grep -q "PYTHON_IMAGE=python:3.12.13-slim-bookworm" versions.env
grep -q "dagster==1.13.9" pyproject.toml
grep -q "dbt-core==1.11.11" pyproject.toml
grep -q "apt-get install -y --no-install-recommends git" docker/dbt/Dockerfile
grep -q "apt-get install -y --no-install-recommends git" docker/dagster/Dockerfile
grep -q "PYTHONPATH=/workspace" docker/dagster/Dockerfile
grep -q "COPY ingest_worker /workspace/ingest_worker" docker/dagster/Dockerfile
grep -q "SODA_CORE_VERSION=4.14.0" versions.env
grep -q "SODA_POSTGRES_VERSION=4.14.0" versions.env
grep -q "REDPANDA_IMAGE=redpandadata/redpanda:v26.1.10" versions.env
grep -q "REDPANDA_PROTOCOL_CONTRACT=kafka-api" versions.env
grep -q "REDPANDA_TOPIC_ANALYTICS_EVENTS=emsi.analytics.events.v1" versions.env
grep -q "APACHE_KAFKA_STATUS=rollback-certification" versions.env
grep -q "CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:25.8.24.21" versions.env
grep -q "CLICKHOUSE_IMAGE_DIGEST=sha256:0fa332a9a05ce4138b16d883f9c9d124c8d9d81cf4e52046878d537558626e49" versions.env
grep -q "CLICKHOUSE_IMAGE_STATUS=candidate-local-smoke" versions.env
grep -q "CLICKHOUSE_SECURITY_SCAN_STATUS=pending-local-vulnerability-scan" versions.env
grep -q "CLICKHOUSE_PRODUCTION_STATUS=blocked-pending-topology-security-governance-restore-owner-approval" versions.env
test -f sql/clickhouse-init/010_hot_analytics.sql
test -f docs/contracts/clickhouse-hot-analytics-promotion-gate.md
grep -q "ENGINE = MergeTree" sql/clickhouse-init/010_hot_analytics.sql
grep -q "ENGINE = SummingMergeTree" sql/clickhouse-init/010_hot_analytics.sql
grep -q "profiles: \\[\"hot-analytics\"\\]" docker-compose.yml
test -x scripts/run_clickhouse_candidate_smoke.sh
test -f ingest_worker/clickhouse_candidate_smoke.py
test -f ingest_worker/clickhouse_candidate_smoke_test.py
grep -q "clickhouse-candidate-smoke" docker-compose.yml
grep -q "ingest_worker.clickhouse_candidate_smoke" docker-compose.yml
grep -q "CLICKHOUSE_PROMOTION_REPORT_JSON" docker-compose.yml
grep -q "CLICKHOUSE_PROMOTION_REPORT_MD" docker-compose.yml
grep -q "./artifacts:/workspace/artifacts" docker-compose.yml
grep -q "clickhouse-promotion-gate" scripts/run_clickhouse_candidate_smoke.sh
grep -q "report.json" scripts/run_clickhouse_candidate_smoke.sh
grep -q "report.md" scripts/run_clickhouse_candidate_smoke.sh
grep -q -- "--force-recreate" scripts/run_clickhouse_candidate_smoke.sh
grep -q "clickhouseCanonical=false" docs/contracts/clickhouse-hot-analytics-promotion-gate.md
grep -q "clickhouseProductionEnabled=false" docs/contracts/clickhouse-hot-analytics-promotion-gate.md
grep -q "EVIDENCE_ID_RE" ingest_worker/clickhouse_candidate_smoke.py
grep -q "unsafe-redacted" ingest_worker/clickhouse_candidate_smoke.py
grep -q "KAFKA_PYTHON_VERSION=3.0.2" versions.env
grep -q "INGEST_POSTGRES_DRIVER=psycopg2-binary==2.9.12" versions.env
grep -q "kafka-python==3.0.2" pyproject.toml
grep -q "psycopg2-binary==2.9.12" pyproject.toml
grep -q "psycopg2-binary==2.9.10" docker/superset/requirements-postgres-metadata.txt
grep -q "ScalefreeCOM/datavault4dbt" dbt/packages.yml
grep -q "business_vault:" dbt/dbt_project.yml
grep -q "mart:" dbt/dbt_project.yml
test -f dbt/models/staging/stg_analytics_events.sql
grep -q "event_hk" dbt/models/staging/stg_analytics_events.sql
test -f dbt/models/raw_vault/hub_analytics_event.sql
grep -q "event_business_key" dbt/models/raw_vault/hub_analytics_event.sql
test -f dbt/models/staging/stg_product_reporting_content_events.sql
test -f dbt/models/staging/stg_product_reporting_reactions.sql
test -f dbt/models/staging/stg_product_reporting_feed_events.sql
test -f dbt/models/staging/stg_product_reporting_channel_sessions.sql
test -f dbt/models/staging/stg_product_reporting_event_funnel.sql
test -f dbt/models/staging/stg_app_together_items.sql
test -f dbt/models/raw_vault/hub_reporting_content.sql
test -f dbt/models/raw_vault/hub_reporting_reaction.sql
test -f dbt/models/raw_vault/hub_reporting_feed_item.sql
test -f dbt/models/raw_vault/link_reporting_reaction_content.sql
test -f dbt/models/raw_vault/link_reporting_feed_item_content.sql
test -f dbt/models/raw_vault/sat_reporting_content_event.sql
test -f dbt/models/raw_vault/sat_reporting_reaction_event.sql
test -f dbt/models/raw_vault/sat_reporting_feed_serving_event.sql
test -f dbt/models/raw_vault/product_reporting_stage_reconciliation.sql
grep -q "accepted_source_count" dbt/models/raw_vault/product_reporting_stage_reconciliation.sql
grep -q "expected_excluded_count" dbt/models/raw_vault/product_reporting_stage_reconciliation.sql
grep -q "unexplained_delta" dbt/models/raw_vault/product_reporting_stage_reconciliation.sql
test -f dbt/tests/product_reporting/product_reporting_stage_reconciliation_negative_fixture_guard.sql
grep -q "force_product_reporting_stage_reconciliation_negative_failure" dbt/tests/product_reporting/product_reporting_stage_reconciliation_negative_fixture_guard.sql
test -f dbt/tests/product_reporting/product_reporting_rdv_hub_invariants.sql
grep -q "duplicate_hash_key" dbt/tests/product_reporting/product_reporting_rdv_hub_invariants.sql
grep -q "business_key_hash_drift" dbt/tests/product_reporting/product_reporting_rdv_hub_invariants.sql
grep -q "hash_key_business_key_drift" dbt/tests/product_reporting/product_reporting_rdv_hub_invariants.sql
grep -q "hub_reporting_content" dbt/tests/product_reporting/product_reporting_rdv_hub_invariants.sql
test -f dbt/tests/product_reporting/product_reporting_rdv_link_invariants.sql
grep -q "duplicate_link_hash_key" dbt/tests/product_reporting/product_reporting_rdv_link_invariants.sql
grep -q "link_hash_participant_drift" dbt/tests/product_reporting/product_reporting_rdv_link_invariants.sql
grep -q "reaction_hub_missing" dbt/tests/product_reporting/product_reporting_rdv_link_invariants.sql
grep -q "content_hub_missing" dbt/tests/product_reporting/product_reporting_rdv_link_invariants.sql
grep -q "feed_item_hub_missing" dbt/tests/product_reporting/product_reporting_rdv_link_invariants.sql
grep -q "channel_hub_missing" dbt/tests/product_reporting/product_reporting_rdv_link_invariants.sql
grep -q "event_hub_missing" dbt/tests/product_reporting/product_reporting_rdv_link_invariants.sql
grep -q "together_item_hub_missing" dbt/tests/product_reporting/product_reporting_rdv_link_invariants.sql
grep -q "force_product_reporting_rdv_link_negative_failure" dbt/tests/product_reporting/product_reporting_rdv_link_invariants.sql
grep -q "controlled_link_orphan_fixture" dbt/tests/product_reporting/product_reporting_rdv_link_invariants.sql
test -f dbt/tests/product_reporting/product_reporting_rdv_satellite_invariants.sql
grep -q "satellite_parent_missing" dbt/tests/product_reporting/product_reporting_rdv_satellite_invariants.sql
grep -q "satellite_required_field_null" dbt/tests/product_reporting/product_reporting_rdv_satellite_invariants.sql
grep -q "satellite_grain_conflict" dbt/tests/product_reporting/product_reporting_rdv_satellite_invariants.sql
grep -q "satellite_hashdiff_state_drift" dbt/tests/product_reporting/product_reporting_rdv_satellite_invariants.sql
grep -q "satellite_duplicate_exact_replay" dbt/tests/product_reporting/product_reporting_rdv_satellite_invariants.sql
grep -q "force_product_reporting_rdv_satellite_orphan_failure" dbt/tests/product_reporting/product_reporting_rdv_satellite_invariants.sql
grep -q "force_product_reporting_rdv_satellite_conflict_failure" dbt/tests/product_reporting/product_reporting_rdv_satellite_invariants.sql
grep -q "voice_speaker_activity_conditional_path_enabled" dbt/tests/product_reporting/product_reporting_rdv_satellite_invariants.sql
grep -q "s_voice_mic_activity_raw" dbt/tests/product_reporting/product_reporting_rdv_satellite_invariants.sql
test -f dbt/models/raw_vault/h_channel.sql
test -f dbt/models/raw_vault/l_user_channel_session.sql
test -f dbt/models/raw_vault/s_channel_session_raw.sql
test -f dbt/models/raw_vault/h_event.sql
test -f dbt/models/raw_vault/l_event_participant.sql
test -f dbt/models/raw_vault/s_event_metadata_raw.sql
test -f dbt/models/raw_vault/h_together_item.sql
test -f dbt/models/raw_vault/l_together_actor_target.sql
test -f dbt/models/raw_vault/s_together_metadata_raw.sql
test -f dbt/models/staging/stg_analytics_voice_session_summary.sql
test -f dbt/models/raw_vault/h_voice_session.sql
test -f dbt/models/raw_vault/h_voice_room.sql
test -f dbt/models/raw_vault/l_voice_session_room.sql
test -f dbt/models/raw_vault/l_voice_participant_session.sql
test -f dbt/models/raw_vault/s_voice_usage_session_raw.sql
test -f dbt/models/raw_vault/s_voice_qoe_raw.sql
test -f dbt/models/raw_vault/s_voice_mic_activity_raw.sql
test -f dbt/models/business_vault/pit_reporting_content_daily.sql
test -f dbt/models/business_vault/br_content_reaction_daily.sql
test -f dbt/models/business_vault/br_feed_interest_proxy.sql
test -f dbt/models/business_vault/s_occupation_cohort_daily.sql
test -f dbt/models/business_vault/s_content_performance_daily.sql
test -f dbt/models/business_vault/s_emoji_usage_daily.sql
test -f dbt/models/business_vault/s_reaction_valence_daily.sql
test -f dbt/models/business_vault/s_channel_session_daily.sql
test -f dbt/models/business_vault/pit_event_daily.sql
test -f dbt/models/business_vault/br_event_funnel.sql
test -f dbt/models/business_vault/s_event_funnel_daily.sql
test -f dbt/models/business_vault/pit_together_daily.sql
test -f dbt/models/business_vault/br_together_response_flow.sql
test -f dbt/models/business_vault/s_together_coordination_daily.sql
test -f dbt/models/business_vault/s_voice_room_usage_daily.sql
test -f dbt/models/business_vault/s_voice_qos_daily.sql
test -f dbt/models/business_vault/s_voice_mic_usage_daily.sql
test -f dbt/models/business_vault/s_voice_speech_activity_daily.sql
test -f dbt/models/business_vault/product_reporting_bdv_contract_coverage.sql
test -f dbt/models/mart/mart_product_reporting_occupation_cohort_daily.sql
test -f dbt/models/mart/mart_product_reporting_content_performance_daily.sql
test -f dbt/models/mart/mart_product_reporting_emoji_reaction_daily.sql
test -f dbt/models/mart/mart_product_reporting_reaction_valence_daily.sql
test -f dbt/models/mart/mart_product_reporting_feed_interest_proxy_daily.sql
test -f dbt/models/mart/mart_product_reporting_together_coordination_daily.sql
test -f dbt/models/mart/mart_product_reporting_contract_coverage.sql
grep -q 'materialized="view"' dbt/models/mart/mart_product_reporting_occupation_cohort_daily.sql
grep -q 'materialized="view"' dbt/models/mart/mart_product_reporting_content_performance_daily.sql
grep -q 'materialized="view"' dbt/models/mart/mart_product_reporting_emoji_reaction_daily.sql
grep -q 'materialized="view"' dbt/models/mart/mart_product_reporting_reaction_valence_daily.sql
grep -q 'materialized="view"' dbt/models/mart/mart_product_reporting_feed_interest_proxy_daily.sql
grep -q 'materialized="view"' dbt/models/mart/mart_product_reporting_together_coordination_daily.sql
grep -q 'materialized="view"' dbt/models/mart/mart_product_reporting_contract_coverage.sql
grep -q "product_reporting_phase1" dbt/models/staging/stg_product_reporting_content_events.sql
grep -q "channel_session_started" dbt/models/staging/stg_product_reporting_channel_sessions.sql
grep -q "event_card_impression" dbt/models/staging/stg_product_reporting_event_funnel.sql
grep -q "together_%" dbt/models/staging/stg_app_together_items.sql
grep -q "product_reporting_phase2" dbt/models/business_vault/s_content_performance_daily.sql
grep -q "product_reporting_phase3" dbt/models/mart/mart_product_reporting_content_performance_daily.sql
grep -q "product_reporting_privacy_contract" dbt/models/staging/stg_analytics_voice_session_summary.sql
grep -q "voice_speaker_activity_legal_mode" dbt/models/staging/stg_analytics_voice_session_summary.sql
grep -q "production_write_enabled = false" dbt/models/business_vault/s_voice_speech_activity_daily.sql
grep -q "source_completeness_input" dbt/models/staging/stg_product_reporting_feed_events.sql
grep -q "occupation_cohort_key" dbt/models/staging/stg_product_reporting_content_events.sql
grep -q "emoji_key" dbt/models/staging/stg_product_reporting_reactions.sql
grep -q "reaction_valence" dbt/models/staging/stg_product_reporting_reactions.sql
grep -q "interest_proxy_valence" dbt/models/staging/stg_product_reporting_feed_events.sql
grep -q "product_reporting_stage_reconciliation" dagster_project/definitions.py
grep -q "product_reporting_stage_reconciliation_negative_fixture_guard" dagster_project/definitions.py
grep -q "product_reporting_rdv_hub_invariants" dagster_project/definitions.py
grep -q "product_reporting_rdv_link_invariants" dagster_project/definitions.py
grep -q "product_reporting_rdv_satellite_invariants" dagster_project/definitions.py
grep -q "product_reporting_phase1_stage_rdv_job" dagster_project/definitions.py
grep -q "product_reporting_phase2_bdv_job" dagster_project/definitions.py
grep -q "product_reporting_phase3_pl_job" dagster_project/definitions.py
grep -q "product_reporting_business_vault" dagster_project/definitions.py
grep -q "product_reporting_mart" dagster_project/definitions.py
grep -q "small_cell_suppression_status" dbt/models/business_vault/s_occupation_cohort_daily.sql
grep -q "metric_contract_ids" dbt/models/business_vault/s_content_performance_daily.sql
grep -q "source_completeness_label" dbt/models/business_vault/br_feed_interest_proxy.sql
grep -q "where small_cell_suppression_status = 'reportable'" dbt/models/mart/mart_product_reporting_content_performance_daily.sql
grep -q "metric_contract_ids" dbt/models/mart/mart_product_reporting_feed_interest_proxy_daily.sql
grep -q "metric_contract_ids" dbt/models/mart/mart_product_reporting_together_coordination_daily.sql
grep -q "reporting_timezone" dbt/models/mart/mart_product_reporting_reaction_valence_daily.sql
grep -q "bdv_only_pending_pl" dbt/models/business_vault/product_reporting_bdv_contract_coverage.sql
grep -q "together_coordination_success_proxy_daily" dbt/models/business_vault/product_reporting_bdv_contract_coverage.sql
if grep -R "tracking_token" dbt/models/staging/stg_product_reporting_*.sql dbt/models/staging/stg_app_together_items.sql dbt/models/raw_vault/*reporting*.sql dbt/models/raw_vault/h_channel.sql dbt/models/raw_vault/l_user_channel_session.sql dbt/models/raw_vault/s_channel_session_raw.sql dbt/models/raw_vault/h_event.sql dbt/models/raw_vault/l_event_participant.sql dbt/models/raw_vault/s_event_metadata_raw.sql dbt/models/raw_vault/h_together_item.sql dbt/models/raw_vault/l_together_actor_target.sql dbt/models/raw_vault/s_together_metadata_raw.sql dbt/models/business_vault/*.sql dbt/models/mart/*.sql; then
  echo "product reporting models must not expose or derive from tracking tokens" >&2
  exit 1
fi
if grep -R "channel_title\\|raw_content\\|post_body\\|comment_body\\|reply_body\\|dm_content\\|transcript\\|screenshot\\|exact_gps\\|contact_value\\|reveal_value\\|request_body\\|response_body\\|prompt\\|note_body\\|raw_note" dbt/models/staging/stg_product_reporting_*.sql dbt/models/staging/stg_app_together_items.sql dbt/models/raw_vault/*reporting*.sql dbt/models/raw_vault/h_channel.sql dbt/models/raw_vault/l_user_channel_session.sql dbt/models/raw_vault/s_channel_session_raw.sql dbt/models/raw_vault/h_event.sql dbt/models/raw_vault/l_event_participant.sql dbt/models/raw_vault/s_event_metadata_raw.sql dbt/models/raw_vault/h_together_item.sql dbt/models/raw_vault/l_together_actor_target.sql dbt/models/raw_vault/s_together_metadata_raw.sql dbt/models/business_vault/*.sql dbt/models/mart/*.sql; then
  echo "product reporting models must not project blocked raw content/contact/location fields" >&2
  exit 1
fi
if grep -R "raw_audio\\|audio_frame\\|spoken_words\\|voiceprint\\|speaker_embedding\\|speaking_timeline\\|speaking_interval\\|vad_frame_list\\|co_participant_key\\|pairwise_duration\\|private_room_name\\|raw_room_title\\|exact_gps\\|request_body\\|response_body\\|screenshot\\|ocr\\|raw_filename" dbt/models/staging/stg_analytics_voice_session_summary.sql dbt/models/raw_vault/*voice*.sql dbt/models/business_vault/*voice*.sql; then
  echo "voice usage models must not project blocked audio/content/pairwise fields" >&2
  exit 1
fi
grep -q "name: analytics_postgres" soda/configuration.yml
grep -q "dataset: analytics_postgres/analytics/analytics/raw_event_landing" soda/contracts/raw_event_landing.yml
grep -q "note_body" soda/contracts/raw_event_landing.yml
grep -q "reveal_payload" soda/contracts/raw_event_landing.yml
grep -q "exact_gps" soda/contracts/raw_event_landing.yml
test -f soda/contracts/product_reporting_occupation_cohort_daily.yml
test -f soda/contracts/product_reporting_content_performance_daily.yml
test -f soda/contracts/product_reporting_emoji_reaction_daily.yml
test -f soda/contracts/product_reporting_reaction_valence_daily.yml
test -f soda/contracts/product_reporting_feed_interest_proxy_daily.yml
test -f soda/contracts/product_reporting_together_coordination_daily.yml
test -f soda/contracts/product_reporting_contract_coverage.yml
test -f soda/contracts/voice_usage_session_summary.yml
test -f soda/contracts/privacy_lifecycle_contract.yml
test -f soda/contracts/personal_recap_monthly.yml
test -f ingest_worker/privacy_lifecycle_smoke.py
test -f ingest_worker/privacy_lifecycle_runtime.py
test -f ingest_worker/privacy_lifecycle_runtime_test.py
test -f ingest_worker/fixtures/privacy_lifecycle_source_bound_packet.json
test -x scripts/run_privacy_lifecycle_smoke.sh
test -x scripts/run_privacy_lifecycle_runtime.sh
grep -q "analytics_mart/mart_product_reporting_content_performance_daily" soda/contracts/product_reporting_content_performance_daily.yml
grep -q "analytics_mart/mart_product_reporting_feed_interest_proxy_daily" soda/contracts/product_reporting_feed_interest_proxy_daily.yml
grep -q "analytics_mart/mart_product_reporting_together_coordination_daily" soda/contracts/product_reporting_together_coordination_daily.yml
grep -q "Europe/Istanbul" soda/contracts/product_reporting_reaction_valence_daily.yml
grep -q "voice_speaker_activity_gate_closed" soda/contracts/voice_usage_session_summary.yml
grep -q "anonymization_failure_purge_required" soda/contracts/privacy_lifecycle_contract.yml
grep -q "personal_recap_opt_in_required" soda/contracts/personal_recap_monthly.yml
grep -q "retention_candidates" ingest_worker/privacy_lifecycle_smoke.py
grep -q "anonymous_candidate_checks" ingest_worker/privacy_lifecycle_smoke.py
grep -q "purge_after_anonymization_failure" ingest_worker/privacy_lifecycle_smoke.py
grep -q "lifecycle_audit" ingest_worker/privacy_lifecycle_smoke.py
grep -q "opt_out_deletion_cleanup" ingest_worker/privacy_lifecycle_smoke.py
grep -q "cleanup_analytics_opt_out" ingest_worker/privacy_lifecycle_smoke.py
grep -q "cleanup_account_deletion" ingest_worker/privacy_lifecycle_smoke.py
grep -q "ClickHouse candidate-only non-canonical" ingest_worker/privacy_lifecycle_smoke.py
grep -q "voiceSpeakerActivityAnalytics" ingest_worker/privacy_lifecycle_smoke.py
grep -q "personalYearlyRecap" ingest_worker/privacy_lifecycle_smoke.py
grep -q "local_source_bound_privacy_lifecycle_runtime" ingest_worker/privacy_lifecycle_runtime.py
grep -q "source_bound_packet_valid" dagster_project/definitions.py
grep -q "privacy.source_bound_runtime_report" dagster_project/definitions.py
grep -q "privacy.anonymize_or_delete_decisions" dagster_project/definitions.py
grep -q "privacy.anonymize_or_delete_outcomes" dagster_project/definitions.py
grep -q "local_source_bound_runtime_ready" dagster_project/definitions.py
grep -q "productionCollectionEnabled" ingest_worker/fixtures/privacy_lifecycle_source_bound_packet.json
grep -q '"apiExposureEnabled": false' ingest_worker/fixtures/privacy_lifecycle_source_bound_packet.json
grep -q '"dashboardExposureEnabled": false' ingest_worker/fixtures/privacy_lifecycle_source_bound_packet.json
grep -q '"clickhouseProductionEnabled": false' ingest_worker/fixtures/privacy_lifecycle_source_bound_packet.json
grep -q "PostgreSQL" ingest_worker/fixtures/privacy_lifecycle_source_bound_packet.json
grep -q "Redpanda/Kafka API" ingest_worker/fixtures/privacy_lifecycle_source_bound_packet.json
if grep -q "emsi_qa\\|emsi_qqq" ingest_worker/fixtures/privacy_lifecycle_source_bound_packet.json; then
  echo "privacy lifecycle source-bound packet must not use QA or legacy report sources" >&2
  exit 1
fi
grep -q "product_reporting_soda_mart_contracts" dagster_project/definitions.py
grep -q "PRODUCT_REPORTING_SODA_CONTRACT_NAMES" dagster_project/definitions.py
grep -q "product_reporting_together_coordination_daily.yml" dagster_project/definitions.py
grep -q "PRIVACY_SODA_CONTRACT_NAMES" dagster_project/definitions.py
grep -q "privacy_lifecycle_daily_job" dagster_project/definitions.py
grep -q "privacy_contract_guard_job" dagster_project/definitions.py
grep -q "voice_usage_soda_contracts" dagster_project/definitions.py
grep -q "personal_recap_deletion_checks" dagster_project/definitions.py
grep -q "soda contract verify" scripts/run_phase_d_smoke.sh
grep -q "phase_d_local_smoke_job" dagster_project/definitions.py
grep -q "note_body" dagster_project/definitions.py
grep -q "reveal_payload" dagster_project/definitions.py
grep -q "exact_gps" dagster_project/definitions.py
grep -q "raw_audio" dagster_project/definitions.py
grep -q "speaker_embedding" dagster_project/definitions.py
grep -q "pairwise_duration" dagster_project/definitions.py
grep -q "soda-core==4.14.0" pyproject.toml
grep -q "soda-postgres==4.14.0" pyproject.toml

PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/emsi-data-platform-pycache}" \
  python3 -m py_compile dagster_project/*.py ingest_worker/*.py

PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/emsi-data-platform-pycache}" \
  ./scripts/run_privacy_lifecycle_smoke.sh >/dev/null

PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/emsi-data-platform-pycache}" \
  ./scripts/run_privacy_lifecycle_runtime.sh >/dev/null

if grep -R "soda-core-postgres" docker dbt dagster_project soda pyproject.toml docker-compose.yml docker-compose.superset-postgres-metadata.yml; then
  echo "legacy Soda v3 package found in runnable scaffold" >&2
  exit 1
fi

if grep -n "/var/lib/postgresql/data" docker-compose.yml docker-compose.superset-postgres-metadata.yml; then
  echo "legacy PostgreSQL data mount found in compose files" >&2
  exit 1
fi

if grep -R "payload_preview\\|raw_payload" ingest_worker sql docker-compose.yml; then
  echo "raw DLQ payload storage found in ingest path" >&2
  exit 1
fi

grep -q "\"note\"" ingest_worker/worker.py
grep -q "\"revealpayload\"" ingest_worker/worker.py
grep -q "\"exactgps\"" ingest_worker/worker.py

if grep -R "clickhouse/clickhouse-server:latest\\|clickhouse/clickhouse-server:head" versions.env docker-compose.yml; then
  echo "unpinned ClickHouse image found" >&2
  exit 1
fi

if [ "${SKIP_DOCKER_CONFIG:-0}" = "1" ]; then
  echo "skipped docker compose config parse because SKIP_DOCKER_CONFIG=1" >&2
elif command -v docker >/dev/null 2>&1; then
  docker compose --env-file versions.env --profile local --profile streaming --profile observability --profile evidence --profile object-storage --profile hot-analytics --profile hot-analytics-smoke config >/dev/null
  docker compose --env-file versions.env -f docker-compose.yml -f docker-compose.superset-postgres-metadata.yml --profile superset config >/dev/null
else
  echo "docker not found; skipped docker compose config parse" >&2
fi

echo "static verification passed"
