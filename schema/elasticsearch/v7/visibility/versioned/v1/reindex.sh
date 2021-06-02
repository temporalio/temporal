#!/bin/bash

set -eu -o pipefail

ES_SCHEME="${ES_SCHEME:-http}"
ES_SERVER="${ES_SERVER:-127.0.0.1}"
ES_PORT="${ES_PORT:-9200}"
ES_USER="${ES_USER:-}"
ES_PWD="${ES_PWD:-}"
REINDEX_CUSTOM_FIELDS="${REINDEX_CUSTOM_FIELDS:-true}"

DIR_NAME=$(dirname "$0")
# Convert multiline script to single line and remove repeated spaces.
REINDEX_SCRIPT=$(tr --delete "\n" < "${DIR_NAME}/reindex.painless" | tr --squeeze-repeats " ")
# Substitute envs in reindex.json (envsubst is not available in alpine by default).
REINDEX_JSON=$(sed -e "s/\${REINDEX_SCRIPT}/${REINDEX_SCRIPT}/" -e "s/\${REINDEX_CUSTOM_FIELDS}/${REINDEX_CUSTOM_FIELDS}/" "${DIR_NAME}/reindex.json")
echo "${REINDEX_JSON}" | jq

ES_URL="${ES_SCHEME}://${ES_SERVER}:${ES_PORT}/_reindex"
curl --user "${ES_USER}":"${ES_PWD}" -X POST "${ES_URL}" -H "Content-Type: application/json" --data-binary "${REINDEX_JSON}" --write-out "\n" | jq

