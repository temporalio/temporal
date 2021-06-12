#!/bin/bash

set -eu -o pipefail

# Input parameters.
ES_SCHEME="${ES_SCHEME:-http}"
ES_SERVER="${ES_SERVER:-127.0.0.1}"
ES_PORT="${ES_PORT:-9200}"
ES_USER="${ES_USER:-}"
ES_PWD="${ES_PWD:-}"
ES_VIS_INDEX_V0="${ES_VIS_INDEX_V0:-temporal-visibility-dev}"
ES_VIS_INDEX_V1="${ES_VIS_INDEX_V1:-temporal_visibility_v1_dev}"
REINDEX_CUSTOM_FIELDS="${REINDEX_CUSTOM_FIELDS:-true}"
AUTO_CONFIRM="${AUTO_CONFIRM:-}"

DIR_NAME="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
# Convert multiline script to single line, remove repeated spaces, and replace / with \/.
REINDEX_SCRIPT=$(tr -d "\n" < "${DIR_NAME}/reindex.painless" | tr -s " " | sed "s/\//\\\\\//g")
# Substitute envs in reindex.json (envsubst is not available in alpine by default).
REINDEX_JSON=$(sed \
    -e "s/\${REINDEX_SCRIPT}/${REINDEX_SCRIPT}/g" \
    -e "s/\${ES_VIS_INDEX_V0}/${ES_VIS_INDEX_V0}/g" \
    -e "s/\${ES_VIS_INDEX_V1}/${ES_VIS_INDEX_V1}/g" \
    -e "s/\${REINDEX_CUSTOM_FIELDS}/${REINDEX_CUSTOM_FIELDS}/g" \
    "${DIR_NAME}/reindex.json")

echo "${REINDEX_JSON}" | jq
if [ -z "${AUTO_CONFIRM}" ]; then
    read -p "Apply reindex script above? (N/y)" -n 1 -r
    echo
else
    REPLY="y"
fi

if [ "${REPLY}" = "y" ]; then
    ES_URL="${ES_SCHEME}://${ES_SERVER}:${ES_PORT}/_reindex"
    curl --user "${ES_USER}":"${ES_PWD}" -X POST "${ES_URL}" -H "Content-Type: application/json" --data-binary "${REINDEX_JSON}" --write-out "\n" | jq
fi


