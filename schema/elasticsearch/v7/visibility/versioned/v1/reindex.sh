#!/bin/bash

set -eu -o pipefail

DIR_NAME=$(dirname "$0")
REINDEX_SCRIPT=$(tr --delete "\n" < "${DIR_NAME}/reindex.painless" | tr --squeeze-repeats " ")
export REINDEX_SCRIPT
REINDEX_JSON=$(envsubst < "${DIR_NAME}/reindex.json")

echo "${REINDEX_JSON}" | jq

curl -X POST "http://127.0.0.1:9200/_reindex" -H "Content-Type: application/json" --data-binary "${REINDEX_JSON}" --write-out "\n" | jq

