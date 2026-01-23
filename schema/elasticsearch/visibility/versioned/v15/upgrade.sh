#!/usr/bin/env bash

set -eu -o pipefail

# Prerequisites:
#   - jq
#   - curl

# Input parameters.
: "${ES_SCHEME:=http}"
: "${ES_SERVER:=127.0.0.1}"
: "${ES_PORT:=9200}"
: "${ES_USER:=}"
: "${ES_PWD:=}"
: "${ES_VERSION:=v7}"
: "${ES_VIS_INDEX_V1:=temporal_visibility_v1_dev}"
: "${AUTO_CONFIRM:=}"
: "${SLICES_COUNT:=auto}"

es_endpoint="${ES_SCHEME}://${ES_SERVER}:${ES_PORT}"

echo "=== Step 0. Sanity check if Elasticsearch index is accessible ==="

if ! curl --silent --fail --user "${ES_USER}":"${ES_PWD}" "${es_endpoint}/${ES_VIS_INDEX_V1}/_stats/docs" --write-out "\n"; then
    echo "Elasticsearch index ${ES_VIS_INDEX_V1} is not accessible at ${es_endpoint}."
    exit 1
fi

echo "=== Step 1. Add TemporalExternalPayloadSizeBytes and TemporalExternalPayloadCount builtin search attributes ==="

new_mapping='
{
  "properties": {
    "Bool01": {
      "type": "boolean"
    },
    "Bool02": {
      "type": "boolean"
    },
    "Bool03": {
      "type": "boolean"
    },
    "Int01": {
      "type": "long"
    },
    "Int02": {
      "type": "long"
    },
    "Int03": {
      "type": "long"
    },
    "Double01": {
      "type": "scaled_float",
      "scaling_factor": 10000
    },
    "Double02": {
      "type": "scaled_float",
      "scaling_factor": 10000
    },
    "Double03": {
      "type": "scaled_float",
      "scaling_factor": 10000
    },
    "Datetime01": {
      "type": "date_nanos"
    },
    "Datetime02": {
      "type": "date_nanos"
    },
    "Datetime03": {
      "type": "date_nanos"
    },
    "Keyword01": {
      "type": "keyword"
    },
    "Keyword02": {
      "type": "keyword"
    },
    "Keyword03": {
      "type": "keyword"
    },
    "Keyword04": {
      "type": "keyword"
    },
    "Keyword05": {
      "type": "keyword"
    },
    "Keyword06": {
      "type": "keyword"
    },
    "Keyword07": {
      "type": "keyword"
    },
    "Keyword08": {
      "type": "keyword"
    },
    "Keyword09": {
      "type": "keyword"
    },
    "Keyword10": {
      "type": "keyword"
    },
    "KeywordList01": {
      "type": "keyword"
    },
    "KeywordList02": {
      "type": "keyword"
    },
    "KeywordList03": {
      "type": "keyword"
    },
    "Text01": {
      "type": "text"
    },
    "Text02": {
      "type": "text"
    },
    "Text03": {
      "type": "text"
    }
  }
}
'

if [ -z "${AUTO_CONFIRM}" ]; then
    read -p "Add custom search attributes to the index ${ES_VIS_INDEX_V1}? (N/y)" -n 1 -r
    echo
else
    REPLY="y"
fi
if [ "${REPLY}" = "y" ]; then
    curl --silent --fail --user "${ES_USER}":"${ES_PWD}" -X PUT "${es_endpoint}/${ES_VIS_INDEX_V1}/_mapping" -H "Content-Type: application/json" --data-binary "$new_mapping" | jq
    # Wait for mapping changes to go through.
    until curl --silent --user "${ES_USER}":"${ES_PWD}" "${es_endpoint}/_cluster/health/${ES_VIS_INDEX_V1}" | jq --exit-status '.status=="green" | .'; do
        echo "Waiting for Elasticsearch index ${ES_VIS_INDEX_V1} become green."
        sleep 1
    done
fi