#!/usr/bin/env bash
set -euo pipefail

JOB_NAME="${1:-}"
if [ -z "${JOB_NAME}" ]; then
  echo "usage: $0 <dagster-job-name>" >&2
  exit 2
fi

ENV_FILE="${ENV_FILE:-.env}"
DAGSTER_GRAPHQL_URL="${DAGSTER_GRAPHQL_URL:-http://localhost:3000/graphql}"
DAGSTER_WORKSPACE_PATH="${DAGSTER_WORKSPACE_PATH:-/workspace/dagster_workspace.yaml}"
DAGSTER_LAUNCH_TIMEOUT_SECONDS="${DAGSTER_LAUNCH_TIMEOUT_SECONDS:-900}"

if [ ! -f "${ENV_FILE}" ]; then
  cp .env.example "${ENV_FILE}"
fi

COMPOSE_ENV_ARGS=(--env-file versions.env --env-file "${ENV_FILE}")
RUN_ID="$(RUN_ID="${RUN_ID:-}" python3 - <<'PY'
import os
import sys
import uuid

raw_run_id = os.environ.get("RUN_ID", "")
if not raw_run_id:
    print(uuid.uuid4())
    sys.exit(0)

try:
    print(uuid.UUID(raw_run_id))
except ValueError:
    print(f"RUN_ID must be a valid UUID. Got {raw_run_id}", file=sys.stderr)
    sys.exit(2)
PY
)"

docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local up -d --build \
  dagster-webserver dagster-daemon

docker compose "${COMPOSE_ENV_ARGS[@]}" --profile local exec -T dagster-webserver \
  dagster job launch \
    -w "${DAGSTER_WORKSPACE_PATH}" \
    -j "${JOB_NAME}" \
    --run-id "${RUN_ID}" \
    --tags '{"emsi.launcher":"workspace","emsi.local_dev":"true"}'

echo "launched Dagster run ${RUN_ID} for ${JOB_NAME}"
echo "Dagster run URL: http://localhost:3000/runs/${RUN_ID}"

RUN_ID="${RUN_ID}" \
DAGSTER_GRAPHQL_URL="${DAGSTER_GRAPHQL_URL}" \
DAGSTER_LAUNCH_TIMEOUT_SECONDS="${DAGSTER_LAUNCH_TIMEOUT_SECONDS}" \
python3 - <<'PY'
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

run_id = os.environ["RUN_ID"]
graphql_url = os.environ["DAGSTER_GRAPHQL_URL"]
timeout_seconds = int(os.environ["DAGSTER_LAUNCH_TIMEOUT_SECONDS"])
deadline = time.monotonic() + timeout_seconds
last_status = "UNKNOWN"

query = {
    "query": """
        query RunStatus($runId: ID!) {
          runOrError(runId: $runId) {
            __typename
            ... on Run {
              status
              endTime
            }
            ... on PythonError {
              message
            }
          }
        }
    """,
    "variables": {"runId": run_id},
}

terminal_success = {"SUCCESS"}
terminal_failure = {"FAILURE", "CANCELED", "CANCELING"}

while time.monotonic() < deadline:
    request = urllib.request.Request(
        graphql_url,
        data=json.dumps(query).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError) as exc:
        print(f"waiting for Dagster run {run_id}: GraphQL probe failed: {exc}")
        time.sleep(2)
        continue

    run_or_error = payload.get("data", {}).get("runOrError", {})
    typename = run_or_error.get("__typename")
    if typename != "Run":
        print(f"Dagster run lookup failed for {run_id}: {run_or_error}", file=sys.stderr)
        sys.exit(1)

    status = run_or_error.get("status", "UNKNOWN")
    if status != last_status:
        print(f"Dagster run {run_id} status: {status}")
        last_status = status

    if status in terminal_success:
        sys.exit(0)
    if status in terminal_failure:
        sys.exit(1)

    time.sleep(2)

print(
    f"Dagster run {run_id} did not finish within {timeout_seconds} seconds; "
    f"last status: {last_status}",
    file=sys.stderr,
)
sys.exit(1)
PY
