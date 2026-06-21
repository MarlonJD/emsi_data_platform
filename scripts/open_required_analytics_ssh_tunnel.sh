#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${EMSI_REQUIRED_ANALYTICS_ENV_FILE:-.env.required-analytics}"

load_required_analytics_env_file() {
  local line key value trimmed
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%$'\r'}"
    trimmed="${line#"${line%%[![:space:]]*}"}"
    trimmed="${trimmed%"${trimmed##*[![:space:]]}"}"
    if [[ -z "${trimmed}" || "${trimmed}" == \#* ]]; then
      continue
    fi
    if [[ "${trimmed}" != EMSI_REQUIRED_ANALYTICS_*=* ]]; then
      echo "invalid required analytics env entry; only EMSI_REQUIRED_ANALYTICS_* assignments are allowed" >&2
      exit 2
    fi
    key="${trimmed%%=*}"
    value="${trimmed#*=}"
    if [[ ! "${key}" =~ ^EMSI_REQUIRED_ANALYTICS_[A-Z0-9_]+$ ]]; then
      echo "invalid required analytics env key: ${key}" >&2
      exit 2
    fi
    export "${key}=${value}"
  done < "${ENV_FILE}"
}

cd "${WORKSPACE_DIR}"
if [[ -f "${ENV_FILE}" ]]; then
  load_required_analytics_env_file
fi

: "${EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_SSH_HOST:?missing SSH host alias}"
: "${EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_REMOTE_HOST:?missing remote warehouse host}"
: "${EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_LOCAL_PORT:?missing local tunnel port}"

REMOTE_PORT="${EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_REMOTE_PORT:-5432}"

echo "opening required analytics SSH tunnel on 127.0.0.1:${EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_LOCAL_PORT}" >&2
exec ssh -o ExitOnForwardFailure=yes -N \
  -L "127.0.0.1:${EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_LOCAL_PORT}:${EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_REMOTE_HOST}:${REMOTE_PORT}" \
  "${EMSI_REQUIRED_ANALYTICS_WAREHOUSE_TUNNEL_SSH_HOST}"
