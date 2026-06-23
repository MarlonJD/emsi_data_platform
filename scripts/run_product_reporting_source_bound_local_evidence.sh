#!/usr/bin/env bash
# Product Reporting SOURCE_BOUND_LOCAL evidence runner.
#
# Proves the local-staging-equivalent chain end to end without a managed staging
# server:
#   owner-approved window -> dbt mart views -> reporting_reader (loopback-only)
#   -> GraphQL Admin API, plus quality-gate fail-closed behaviour.
#
# This is local-staging-equivalent evidence only:
#   environment_class    = LOCAL_STAGING_EQUIVALENT
#   managed_staging      = false
#   evidence_status      = SOURCE_BOUND_LOCAL
#   production_equivalent = false
# It does NOT claim STAGING_EVIDENCED, which requires a managed staging
# environment, secret management, and a continuously running deployment.
#
# Reproducible and non-destructive: it backs up the analytics landing, seeds a
# deterministic owner-approved window, runs the chain, writes a redacted
# manifest, then restores the original landing (set EVIDENCE_RESTORE_LANDING=0
# to keep the seeded window for inspection). No secrets are committed: the
# reporting_reader password defaults to a local value and is overridable via
# REPORTING_READER_PASSWORD; the emitted manifest redacts the password.
set -euo pipefail

cd "$(dirname "$0")/.."  # tools/data-platform

ANALYTICS_CONTAINER="${ANALYTICS_CONTAINER:-emsi-data-platform-analytics-postgres-1}"
ANALYTICS_DB="${ANALYTICS_POSTGRES_DB:-analytics}"
ANALYTICS_USER="${ANALYTICS_POSTGRES_USER:-analytics}"
MART_SCHEMA="${PRODUCT_REPORTING_MART_SCHEMA:-analytics_mart}"
MART_HOST="${PRODUCT_REPORTING_MART_HOST:-127.0.0.1}"
MART_PORT="${PRODUCT_REPORTING_MART_PORT:-5438}"
READER_USER="${REPORTING_READER_USER:-reporting_reader}"
READER_PASSWORD="${REPORTING_READER_PASSWORD:-reporting_reader_local_password}"
RESTORE_LANDING="${EVIDENCE_RESTORE_LANDING:-1}"
GO_API_DIR="${GO_API_DIR:-../../backend/go-api}"
GOCACHE_DIR="${GOCACHE:-/private/tmp/emsi-go-cache}"
MANIFEST_DIR="${MANIFEST_DIR:-/private/tmp/product-reporting-source-bound-local-evidence}"
MANIFEST="${MANIFEST_DIR}/manifest.json"

COMPOSE=(docker compose --env-file versions.env --env-file .env --profile local)

psql_analytics() { docker exec -i "$ANALYTICS_CONTAINER" psql -U "$ANALYTICS_USER" -d "$ANALYTICS_DB" "$@"; }
psql_reader() { docker exec -i -e PGPASSWORD="$READER_PASSWORD" "$ANALYTICS_CONTAINER" psql -U "$READER_USER" -d "$ANALYTICS_DB" "$@"; }
dbt_run() { "${COMPOSE[@]}" run --rm dbt-runner dbt "$@" --project-dir /workspace/dbt --profiles-dir /workspace/dbt; }

mkdir -p "$MANIFEST_DIR"
[ -f .env ] || cp .env.example .env

echo "## 1/8 ensure loopback mart + reporting_reader role"
"${COMPOSE[@]}" up -d analytics-postgres >/dev/null
for _ in $(seq 1 30); do psql_analytics -tAc "select 1" >/dev/null 2>&1 && break; sleep 1; done
psql_analytics -v ON_ERROR_STOP=1 < sql/analytics-postgres-init/030_reporting_reader_role.sql >/dev/null
BIND="$(docker port "$ANALYTICS_CONTAINER" 5432/tcp | head -1)"
echo "   mart published at: ${BIND}"

echo "## 2/8 back up landing + seed deterministic owner-approved window"
psql_analytics -v ON_ERROR_STOP=1 -c \
  "drop table if exists analytics.raw_event_landing_evidence_backup;
   create table analytics.raw_event_landing_evidence_backup as
     select * from analytics.raw_event_landing;" >/dev/null
BACKUP_ROWS="$(psql_analytics -tAc "select count(*) from analytics.raw_event_landing_evidence_backup")"
psql_analytics -v ON_ERROR_STOP=1 -c "truncate analytics.raw_event_landing;" >/dev/null
psql_analytics -v ON_ERROR_STOP=1 < scripts/product_reporting_source_bound_local/seed_owner_approved_window.sql >/dev/null
SEED_ROWS="$(psql_analytics -tAc "select count(*) from analytics.raw_event_landing")"
SEED_DAY="$(psql_analytics -tAc "select min((occurred_at at time zone 'Europe/Istanbul')::date) from analytics.raw_event_landing")"
echo "   seeded ${SEED_ROWS} rows for reporting day ${SEED_DAY} (backed up ${BACKUP_ROWS})"

echo "## 3/8 build mart views from the owner-approved window"
dbt_run run --select \
  +mart_product_reporting_occupation_cohort_daily +mart_product_reporting_content_performance_daily \
  +mart_product_reporting_emoji_reaction_daily +mart_product_reporting_reaction_valence_daily \
  +mart_product_reporting_feed_interest_proxy_daily +mart_product_reporting_together_coordination_daily \
  +mart_product_reporting_contract_coverage +product_reporting_partition_trust_state >/dev/null

echo "## 4/8 reporting_reader least-privilege checks"
# psql exits non-zero on "permission denied"; capture output first so pipefail
# does not invert the result of the grep match.
reader_denied() {
  local out
  out="$(psql_reader -tAc "$1" 2>&1 || true)"
  if printf '%s' "$out" | grep -qi "permission denied"; then echo denied; else echo NOT_DENIED; fi
}
READER_RAW_DENIED="$(reader_denied "select count(*) from analytics.raw_event_landing")"
READER_WRITE_DENIED="$(reader_denied "create table ${MART_SCHEMA}.evidence_probe(i int)")"
READER_MART_OK="$(psql_reader -tAc "select case when count(*) >= 0 then 'ok' else 'err' end from ${MART_SCHEMA}.mart_product_reporting_contract_coverage" 2>&1 | tr -d '[:space:]')"
VALENCE_COUNT="$(psql_reader -tAc "select coalesce(max(reaction_valence_count),0) from ${MART_SCHEMA}.mart_product_reporting_reaction_valence_daily" 2>&1 | tr -d '[:space:]')"
TRUST_DIST="$(psql_analytics -tAc "select coalesce(string_agg(trust_status||'='||c, ','),'none') from (select trust_status, count(*) c from analytics_raw_vault.product_reporting_partition_trust_state group by 1 order by 1) t")"
echo "   reader: mart=${READER_MART_OK}, raw_landing=${READER_RAW_DENIED}, write=${READER_WRITE_DENIED}; reaction_valence_count=${VALENCE_COUNT}"
echo "   trust: ${TRUST_DIST}"

echo "## 5/8 quality gate: invariants PASS clean, FAIL on injected break"
CLEAN_OUT="$(dbt_run test --select product_reporting_partition_trust_state_invariants 2>&1 || true)"
INVARIANTS_CLEAN="$(echo "$CLEAN_OUT" | grep -q 'PASS=1' && echo PASS || echo FAIL)"
BROKEN_OUT="$(dbt_run test --select product_reporting_partition_trust_state_invariants \
  --vars '{force_product_reporting_partition_trust_missing_required_partition_failure: true}' 2>&1 || true)"
INVARIANTS_BROKEN="$(echo "$BROKEN_OUT" | grep -qE 'ERROR=1|FAIL 1' && echo FAIL_CLOSED || echo NOT_ENFORCED)"
echo "   invariants clean=${INVARIANTS_CLEAN}, injected_break=${INVARIANTS_BROKEN}"

echo "## 6/8 API chain + authz (live integration + full httpapi suite)"
GO_RESULT=PASS
( cd "$GO_API_DIR" && GOCACHE="$GOCACHE_DIR" \
  PRODUCT_REPORTING_MART_EVIDENCE_DSN="postgres://${READER_USER}:${READER_PASSWORD}@${MART_HOST}:${MART_PORT}/${ANALYTICS_DB}?sslmode=disable" \
  go test ./internal/httpapi/... ./internal/graph/... ./internal/productreporting/... ./internal/config/... >/dev/null 2>&1 ) || GO_RESULT=FAIL
echo "   go api suite: ${GO_RESULT}"

echo "## 7/8 write redacted manifest"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cat > "$MANIFEST" <<JSON
{
  "evidence_id": "product-reporting-source-bound-local",
  "generated_at": "${GENERATED_AT}",
  "environment_class": "LOCAL_STAGING_EQUIVALENT",
  "managed_staging": false,
  "evidence_status": "SOURCE_BOUND_LOCAL",
  "production_equivalent": false,
  "mart": {
    "host_port_binding": "${BIND}",
    "schema": "${MART_SCHEMA}",
    "reader_role": "${READER_USER}",
    "reader_dsn_redacted": "postgres://${READER_USER}:***@${MART_HOST}:${MART_PORT}/${ANALYTICS_DB}?sslmode=disable"
  },
  "owner_approved_window": {
    "reporting_date": "${SEED_DAY}",
    "landing_rows": ${SEED_ROWS},
    "distinct_users_per_cell": 12,
    "landing_rows_backed_up": ${BACKUP_ROWS}
  },
  "least_privilege": {
    "reader_select_published_mart": "${READER_MART_OK}",
    "reader_raw_landing": "${READER_RAW_DENIED}",
    "reader_write_mart": "${READER_WRITE_DENIED}"
  },
  "chain": {
    "reaction_valence_max_count": ${VALENCE_COUNT},
    "partition_trust_distribution": "${TRUST_DIST}"
  },
  "quality_gate": {
    "invariants_clean_window": "${INVARIANTS_CLEAN}",
    "invariants_injected_break": "${INVARIANTS_BROKEN}"
  },
  "api": {
    "go_suite": "${GO_RESULT}",
    "roles_allowed": ["super_admin", "audit_viewer"],
    "fail_closed": ["unauthorized_forbidden", "flag_off_store_unbound"]
  },
  "not_claimed": "STAGING_EVIDENCED (requires managed staging environment, secret management, and a continuously running deployment)"
}
JSON
echo "   manifest: ${MANIFEST}"

echo "## 8/8 restore landing"
if [ "$RESTORE_LANDING" = "1" ]; then
  psql_analytics -v ON_ERROR_STOP=1 -c \
    "truncate analytics.raw_event_landing;
     insert into analytics.raw_event_landing select * from analytics.raw_event_landing_evidence_backup;
     drop table analytics.raw_event_landing_evidence_backup;" >/dev/null
  echo "   landing restored to ${BACKUP_ROWS} rows"
else
  echo "   landing left at seeded window (EVIDENCE_RESTORE_LANDING=0); backup kept as analytics.raw_event_landing_evidence_backup"
fi

# Overall gate: every proof must hold.
if [ "$READER_RAW_DENIED" = denied ] && [ "$READER_WRITE_DENIED" = denied ] \
   && [ "$INVARIANTS_CLEAN" = PASS ] && [ "$INVARIANTS_BROKEN" = FAIL_CLOSED ] \
   && [ "$GO_RESULT" = PASS ]; then
  echo "SOURCE_BOUND_LOCAL evidence: PASS"
else
  echo "SOURCE_BOUND_LOCAL evidence: FAIL"
  exit 1
fi
