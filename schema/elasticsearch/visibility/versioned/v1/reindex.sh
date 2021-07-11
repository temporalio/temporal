#!/bin/bash

set -eu -o pipefail

# Input parameters.
ES_SCHEME="${ES_SCHEME:-http}"
ES_SERVER="${ES_SERVER:-127.0.0.1}"
ES_PORT="${ES_PORT:-9200}"
ES_USER="${ES_USER:-}"
ES_PWD="${ES_PWD:-}"
ES_VERSION="${ES_VERSION:-v7}"
ES_VIS_INDEX_V0="${ES_VIS_INDEX_V0:-temporal-visibility-dev}"
ES_VIS_INDEX_V1="${ES_VIS_INDEX_V1:-temporal_visibility_v1_dev}"
CUSTOM_SEARCH_ATTRIBUTES="${CUSTOM_SEARCH_ATTRIBUTES:-[\"CustomKeywordField\",\"CustomStringField\",\"CustomIntField\",\"CustomDatetimeField\",\"CustomDoubleField\",\"CustomBoolField\"]}"
AUTO_CONFIRM="${AUTO_CONFIRM:-}"

ES_ENDPOINT="${ES_SCHEME}://${ES_SERVER}:${ES_PORT}"
DIR_NAME="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"

echo "=== Step 1. Add custom search attributes to the new index. ==="
DOC_TYPE=""
if [ "${ES_VERSION}" != "v7" ]; then
    DOC_TYPE="/_doc"
fi
CUSTOM_SEARCH_ATTRIBUTES_MAPPING=$(curl --silent --user "${ES_USER}":"${ES_PWD}" "${ES_ENDPOINT}/${ES_VIS_INDEX_V0}${DOC_TYPE}/_mapping" | jq -c '.. | .Attr? | select(.!=null) | .properties | with_entries(select(.key == ($customSA[]))) | {properties:.}' --argjson customSA "${CUSTOM_SEARCH_ATTRIBUTES}")
if [ "${ES_VERSION}" == "v7" ]; then
    # Replace "date" type with "date_nanos" for Elasticsearch v7.
    CUSTOM_SEARCH_ATTRIBUTES_MAPPING=$(jq '(.properties[].type | select(. == "date")) = "date_nanos"' <<< "${CUSTOM_SEARCH_ATTRIBUTES_MAPPING}")
    # Replace "double" type with "scaled_float" for Elasticsearch v7.
    CUSTOM_SEARCH_ATTRIBUTES_MAPPING=$(jq '(.properties[] | select(.type == "double")) = {"type":"scaled_float","scaling_factor":10000}' <<< "${CUSTOM_SEARCH_ATTRIBUTES_MAPPING}")
fi
jq -n "${CUSTOM_SEARCH_ATTRIBUTES_MAPPING}"
if [ -z "${AUTO_CONFIRM}" ]; then
    read -p "Add custom search attributes above to the index ${ES_VIS_INDEX_V1}? (N/y)" -n 1 -r
    echo
else
    REPLY="y"
fi
if [ "${REPLY}" = "y" ]; then
    curl --silent --user "${ES_USER}":"${ES_PWD}" -X PUT "${ES_ENDPOINT}/${ES_VIS_INDEX_V1}${DOC_TYPE}/_mapping" -H "Content-Type: application/json" --data-binary "${CUSTOM_SEARCH_ATTRIBUTES_MAPPING}" --write-out "\n" | jq
    # Wait for mapping changes to go through.
    until curl --silent --user "${ES_USER}":"${ES_PWD}" "${ES_ENDPOINT}/_cluster/health/${ES_VIS_INDEX_V1}" | jq  --exit-status '.status=="green" | .'; do
        echo "Waiting for Elasticsearch index ${ES_VIS_INDEX_V1} become green."
        sleep 1
    done
fi

echo "=== Step 2. Reindex old index to the new index. ==="
# Convert multiline script to single line, remove repeated spaces, and replace / with \/.
REINDEX_SCRIPT=$(tr -d "\n" < "${DIR_NAME}/reindex.painless" | tr -s " " | sed "s/\//\\\\\//g" | sed "s/&/\\\&/g")
# Substitute envs in reindex.json (envsubst is not available in alpine by default).
REINDEX_JSON=$(sed \
    -e "s/\${REINDEX_SCRIPT}/${REINDEX_SCRIPT}/g" \
    -e "s/\${ES_VIS_INDEX_V0}/${ES_VIS_INDEX_V0}/g" \
    -e "s/\${ES_VIS_INDEX_V1}/${ES_VIS_INDEX_V1}/g" \
    -e "s/\${CUSTOM_SEARCH_ATTRIBUTES}/${CUSTOM_SEARCH_ATTRIBUTES}/g" \
    "${DIR_NAME}/reindex.json")

jq -n "${REINDEX_JSON}"
if [ -z "${AUTO_CONFIRM}" ]; then
    read -p "Apply reindex script above? (N/y)" -n 1 -r
    echo
else
    REPLY="y"
fi

if [ "${REPLY}" = "y" ]; then
    curl --silent --user "${ES_USER}":"${ES_PWD}" -X POST "${ES_ENDPOINT}/_reindex" -H "Content-Type: application/json" --data-binary "${REINDEX_JSON}" --write-out "\n" | jq
fi


