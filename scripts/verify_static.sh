#!/usr/bin/env bash
set -euo pipefail

grep -q "POSTGRES_IMAGE=postgres:18.4-alpine3.24" versions.env
grep -q "POSTGRES_VOLUME_TARGET=/var/lib/postgresql" versions.env
grep -q "PYTHON_IMAGE=python:3.12.13-slim-bookworm" versions.env
grep -q "dagster==1.13.9" pyproject.toml
grep -q "dbt-core==1.11.11" pyproject.toml
grep -q "apt-get install -y --no-install-recommends git" docker/dbt/Dockerfile
grep -q "apt-get install -y --no-install-recommends git" docker/dagster/Dockerfile
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
test -f dbt/models/staging/stg_analytics_events.sql
grep -q "event_hk" dbt/models/staging/stg_analytics_events.sql
test -f dbt/models/raw_vault/hub_analytics_event.sql
grep -q "event_business_key" dbt/models/raw_vault/hub_analytics_event.sql
test -f dbt/models/staging/stg_product_reporting_content_events.sql
test -f dbt/models/staging/stg_product_reporting_reactions.sql
test -f dbt/models/staging/stg_product_reporting_feed_events.sql
test -f dbt/models/raw_vault/hub_reporting_content.sql
test -f dbt/models/raw_vault/hub_reporting_reaction.sql
test -f dbt/models/raw_vault/hub_reporting_feed_item.sql
test -f dbt/models/raw_vault/link_reporting_reaction_content.sql
test -f dbt/models/raw_vault/link_reporting_feed_item_content.sql
test -f dbt/models/raw_vault/sat_reporting_content_event.sql
test -f dbt/models/raw_vault/sat_reporting_reaction_event.sql
test -f dbt/models/raw_vault/sat_reporting_feed_serving_event.sql
test -f dbt/models/raw_vault/product_reporting_stage_reconciliation.sql
test -f dbt/models/business_vault/pit_reporting_content_daily.sql
test -f dbt/models/business_vault/br_content_reaction_daily.sql
test -f dbt/models/business_vault/br_feed_interest_proxy.sql
test -f dbt/models/business_vault/s_occupation_cohort_daily.sql
test -f dbt/models/business_vault/s_content_performance_daily.sql
test -f dbt/models/business_vault/s_emoji_usage_daily.sql
test -f dbt/models/business_vault/s_reaction_valence_daily.sql
test -f dbt/models/business_vault/product_reporting_bdv_contract_coverage.sql
grep -q "product_reporting_phase1" dbt/models/staging/stg_product_reporting_content_events.sql
grep -q "product_reporting_phase2" dbt/models/business_vault/s_content_performance_daily.sql
grep -q "source_completeness_input" dbt/models/staging/stg_product_reporting_feed_events.sql
grep -q "occupation_cohort_key" dbt/models/staging/stg_product_reporting_content_events.sql
grep -q "emoji_key" dbt/models/staging/stg_product_reporting_reactions.sql
grep -q "reaction_valence" dbt/models/staging/stg_product_reporting_reactions.sql
grep -q "interest_proxy_valence" dbt/models/staging/stg_product_reporting_feed_events.sql
grep -q "product_reporting_stage_reconciliation" dagster_project/definitions.py
grep -q "product_reporting_phase1_stage_rdv_job" dagster_project/definitions.py
grep -q "product_reporting_phase2_bdv_job" dagster_project/definitions.py
grep -q "product_reporting_business_vault" dagster_project/definitions.py
grep -q "small_cell_suppression_status" dbt/models/business_vault/s_occupation_cohort_daily.sql
grep -q "metric_contract_ids" dbt/models/business_vault/s_content_performance_daily.sql
grep -q "source_completeness_label" dbt/models/business_vault/br_feed_interest_proxy.sql
if grep -R "tracking_token" dbt/models/staging/stg_product_reporting_*.sql dbt/models/raw_vault/*reporting*.sql dbt/models/business_vault/*.sql; then
  echo "product reporting models must not expose or derive from tracking tokens" >&2
  exit 1
fi
if grep -R "raw_content\\|post_body\\|comment_body\\|reply_body\\|dm_content\\|transcript\\|screenshot\\|exact_gps\\|contact_value\\|reveal_value\\|request_body\\|response_body" dbt/models/staging/stg_product_reporting_*.sql dbt/models/raw_vault/*reporting*.sql dbt/models/business_vault/*.sql; then
  echo "product reporting models must not project blocked raw content/contact/location fields" >&2
  exit 1
fi
grep -q "name: analytics_postgres" soda/configuration.yml
grep -q "dataset: analytics_postgres/analytics/analytics/raw_event_landing" soda/contracts/raw_event_landing.yml
grep -q "note_body" soda/contracts/raw_event_landing.yml
grep -q "reveal_payload" soda/contracts/raw_event_landing.yml
grep -q "exact_gps" soda/contracts/raw_event_landing.yml
grep -q "soda contract verify" scripts/run_phase_d_smoke.sh
grep -q "phase_d_local_smoke_job" dagster_project/definitions.py
grep -q "note_body" dagster_project/definitions.py
grep -q "reveal_payload" dagster_project/definitions.py
grep -q "exact_gps" dagster_project/definitions.py
grep -q "soda-core==4.14.0" pyproject.toml
grep -q "soda-postgres==4.14.0" pyproject.toml

PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/emsi-data-platform-pycache}" \
  python3 -m py_compile dagster_project/*.py ingest_worker/*.py

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
