#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

SOURCE_PACKET="${PRIVACY_LIFECYCLE_SOURCE_PACKET_PATH:-ingest_worker/fixtures/privacy_lifecycle_source_bound_packet.json}"

PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/emsi-data-platform-pycache}" \
  python3 -m ingest_worker.privacy_lifecycle_runtime --source-packet "${SOURCE_PACKET}" "$@"
