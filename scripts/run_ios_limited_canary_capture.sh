#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

cd "${WORKSPACE_DIR}"
PYTHONPATH="${PYTHONPATH:+${PYTHONPATH}:}." python3 -m ingest_worker.ios_limited_canary_capture "$@"
