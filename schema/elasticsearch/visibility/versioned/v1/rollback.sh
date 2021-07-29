#!/bin/bash

set -eu -o pipefail

# Prerequisites:
#   - jq
#   - curl
#   - sed
#   - tr

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
SLICES_COUNT="${SLICES_COUNT:-auto}"

ES_ENDPOINT="${ES_SCHEME}://${ES_SERVER}:${ES_PORT}"
DIR_NAME="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"

echo "=== Step 0. Sanity check if both Elasticsearch indices are accessible. ==="
if ! curl --silent --fail --user "${ES_USER}":"${ES_PWD}" "${ES_ENDPOINT}/${ES_VIS_INDEX_V0}/_stats/docs" --write-out "\n"; then
    echo "Elasticsearch index ${ES_VIS_INDEX_V0} is not accessible at ${ES_ENDPOINT}."
    exit 1
fi

if ! curl --silent --fail --user "${ES_USER}":"${ES_PWD}" "${ES_ENDPOINT}/${ES_VIS_INDEX_V1}/_stats/docs" --write-out "\n"; then
    echo "Elasticsearch index ${ES_VIS_INDEX_V1} is not accessible at ${ES_ENDPOINT}."
    exit 1
fi

echo "=== Step 2. Reindex newly created documents from the new index to the old index. ==="
# Convert multiline script to single line, remove repeated spaces, and replace / with \/.
REINDEX_SCRIPT=$(tr -d "\n" < "${DIR_NAME}/rollback.painless" | tr -s " " | sed "s/\//\\\\\//g" | sed "s/&/\\\&/g")
# Substitute envs in rollback.json (envsubst is not available in alpine by default).
REINDEX_JSON=$(sed \
    -e "s/\${REINDEX_SCRIPT}/${REINDEX_SCRIPT}/g" \
    -e "s/\${ES_VIS_INDEX_V0}/${ES_VIS_INDEX_V0}/g" \
    -e "s/\${ES_VIS_INDEX_V1}/${ES_VIS_INDEX_V1}/g" \
    -e "s/\${CUSTOM_SEARCH_ATTRIBUTES}/${CUSTOM_SEARCH_ATTRIBUTES}/g" \
    "${DIR_NAME}/rollback.json")

jq <<< "${REINDEX_JSON}"
if [ -z "${AUTO_CONFIRM}" ]; then
    read -p "Apply reindex script above? (N/y)" -n 1 -r
    echo
else
    REPLY="y"
fi

if [ "${REPLY}" = "y" ]; then
    REINDEX_RESPONSE=$(curl --silent --user "${ES_USER}":"${ES_PWD}" -X POST "${ES_ENDPOINT}/_reindex?wait_for_completion=false&slices=${SLICES_COUNT}" -H "Content-Type: application/json" --data-binary "${REINDEX_JSON}")
    TASK_ID=$(jq --raw-output --raw-input 'fromjson? | .task' <<< "${REINDEX_RESPONSE}")
    if [ "${TASK_ID}" = null ] || [ "${TASK_ID}" = "" ]; then
        echo "${REINDEX_RESPONSE}"
        exit 1
    fi

    echo "Started reindex task ${TASK_ID}. Check status with:"
    echo "    curl ${ES_ENDPOINT}/_tasks/${TASK_ID}"
    echo "Query subtasks with:"
    echo "    curl ${ES_ENDPOINT}/_tasks?parent_task_id=${TASK_ID}"
    echo "Cancel with:"
    echo "    curl -X POST ${ES_ENDPOINT}/_tasks/${TASK_ID}/_cancel"
    echo
    echo "Waiting for reindex to complete (it is safe to Ctrl+C now)."

    until curl --silent --user "${ES_USER}":"${ES_PWD}" "${ES_ENDPOINT}/_tasks/${TASK_ID}" | jq --exit-status '.completed==true | .'; do
        sleep 10
    done

    TASK_JSON=$(curl --silent --user "${ES_USER}":"${ES_PWD}" "${ES_ENDPOINT}/_tasks/${TASK_ID}")
    if jq --exit-status '.response.failures | length > 0' <<< "${TASK_JSON}"; then
        FAILURES_COUNT=$(jq '.response.failures | length' <<< "${TASK_JSON}")
        echo "There where ${FAILURES_COUNT} failures during reindexing. Check them with:"
        echo "    curl ${ES_ENDPOINT}/_tasks/${TASK_ID} | jq '.response.failures'"
    fi

    echo "Reindex complete:"
    jq --raw-output '.task.status | with_entries(select([.key] | inside(["total", "updated", "created", "version_conflicts"])))' <<< "${TASK_JSON}"
    echo "Source index ${ES_VIS_INDEX_V0} document count:"
    curl --silent --user "${ES_USER}":"${ES_PWD}" "${ES_ENDPOINT}/${ES_VIS_INDEX_V0}/_stats/docs" | jq '._all.primaries.docs.count'
    echo "Destination index ${ES_VIS_INDEX_V1} document count:"
    curl --silent --user "${ES_USER}":"${ES_PWD}" "${ES_ENDPOINT}/${ES_VIS_INDEX_V1}/_stats/docs" | jq '._all.primaries.docs.count'
fi

