#!/usr/bin/env bash
set -euo pipefail

KEYFILE_PATH="/data/configdb/mongo-keyfile"

mkdir -p "$(dirname "${KEYFILE_PATH}")"

if [[ ! -f "${KEYFILE_PATH}" ]]; then
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -base64 756 >"${KEYFILE_PATH}"
  else
    tr -dc 'A-Za-z0-9' </dev/urandom | head -c 756 >"${KEYFILE_PATH}"
  fi
fi

chmod 600 "${KEYFILE_PATH}" || true

exec /usr/local/bin/docker-entrypoint.sh "$@"
