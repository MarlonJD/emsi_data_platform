#!/usr/bin/env bash
set -euo pipefail

PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/emsi-data-platform-pycache}" \
  python3 -m ingest_worker.privacy_lifecycle_smoke "$@"
