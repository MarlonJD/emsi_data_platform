#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${EMSI_DP_CANARY_ENV_FILE:-.env.canary}"
ARGS=()

load_canary_env_file() {
  local line key value trimmed
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%$'\r'}"
    trimmed="${line#"${line%%[![:space:]]*}"}"
    trimmed="${trimmed%"${trimmed##*[![:space:]]}"}"
    if [[ -z "${trimmed}" || "${trimmed}" == \#* ]]; then
      continue
    fi
    if [[ "${trimmed}" != EMSI_DP_CANARY_*=* ]]; then
      echo "invalid canary env entry; only EMSI_DP_CANARY_* assignments are allowed" >&2
      exit 2
    fi
    key="${trimmed%%=*}"
    value="${trimmed#*=}"
    if [[ ! "${key}" =~ ^EMSI_DP_CANARY_[A-Z0-9_]+$ ]]; then
      echo "invalid canary env key: ${key}" >&2
      exit 2
    fi
    export "${key}=${value}"
  done < "${ENV_FILE}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --env-file" >&2
        exit 2
      fi
      ENV_FILE="$2"
      shift 2
      ;;
    --env-file=*)
      ENV_FILE="${1#--env-file=}"
      shift
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

cd "${WORKSPACE_DIR}"
if [[ -f "${ENV_FILE}" ]]; then
  load_canary_env_file
  echo "loaded canary env file: ${ENV_FILE}" >&2
elif [[ -n "${EMSI_DP_CANARY_ENV_FILE:-}" ]]; then
  echo "requested canary env file not found: ${ENV_FILE}" >&2
  exit 2
fi

PYTHONPATH="${PYTHONPATH:+${PYTHONPATH}:}." python3 -m ingest_worker.ios_limited_canary_capture "${ARGS[@]}"
