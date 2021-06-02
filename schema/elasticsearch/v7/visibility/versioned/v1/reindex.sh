#!/bin/bash

set -eu -o pipefail

ES_SCHEME="${ES_SCHEME:-http}"
ES_SERVER="${ES_SERVER:-127.0.0.1}"
ES_PORT="${ES_PORT:-9200}"
ES_USER="${ES_USER:-}"
ES_PWD="${ES_PWD:-}"

DIR_NAME=$(dirname "$0")
REINDEX_SCRIPT=$(tr --delete "\n" < "${DIR_NAME}/reindex.painless" | tr --squeeze-repeats " ")
export REINDEX_SCRIPT
REINDEX_JSON=$(envsubst < "${DIR_NAME}/reindex.json")

echo "${REINDEX_JSON}" | jq

ES_URL="${ES_SCHEME}://${ES_SERVER}:${ES_PORT}/_reindex"
curl --user "${ES_USER}":"${ES_PWD}" -X POST "${ES_URL}" -H "Content-Type: application/json" --data-binary "${REINDEX_JSON}" --write-out "\n" | jq

