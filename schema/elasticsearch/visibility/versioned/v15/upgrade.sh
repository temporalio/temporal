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

<<<<<<< Updated upstream
echo "=== Step 1. Add TemporalExternalPayloadSizeBytes and TemporalExternalPayloadCount builtin search attributes ==="
=======
echo "=== Step 1. Add custom search attribute fields ==="
>>>>>>> Stashed changes

new_mapping='
{
  "properties": {
<<<<<<< Updated upstream
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
=======
    "Bool01": { "type": "boolean" },
    "Bool02": { "type": "boolean" },
    "Bool03": { "type": "boolean" },
    "Bool04": { "type": "boolean" },
    "Bool05": { "type": "boolean" },
    "Bool06": { "type": "boolean" },
    "Bool07": { "type": "boolean" },
    "Bool08": { "type": "boolean" },
    "Bool09": { "type": "boolean" },
    "Bool10": { "type": "boolean" },
    "Bool11": { "type": "boolean" },
    "Bool12": { "type": "boolean" },
    "Bool13": { "type": "boolean" },
    "Bool14": { "type": "boolean" },
    "Bool15": { "type": "boolean" },
    "Bool16": { "type": "boolean" },
    "Bool17": { "type": "boolean" },
    "Bool18": { "type": "boolean" },
    "Bool19": { "type": "boolean" },
    "Bool20": { "type": "boolean" },
    "Int01": { "type": "long" },
    "Int02": { "type": "long" },
    "Int03": { "type": "long" },
    "Int04": { "type": "long" },
    "Int05": { "type": "long" },
    "Int06": { "type": "long" },
    "Int07": { "type": "long" },
    "Int08": { "type": "long" },
    "Int09": { "type": "long" },
    "Int10": { "type": "long" },
    "Int11": { "type": "long" },
    "Int12": { "type": "long" },
    "Int13": { "type": "long" },
    "Int14": { "type": "long" },
    "Int15": { "type": "long" },
    "Int16": { "type": "long" },
    "Int17": { "type": "long" },
    "Int18": { "type": "long" },
    "Int19": { "type": "long" },
    "Int20": { "type": "long" },
    "Double01": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double02": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double03": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double04": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double05": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double06": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double07": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double08": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double09": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double10": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double11": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double12": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double13": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double14": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double15": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double16": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double17": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double18": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double19": { "type": "scaled_float", "scaling_factor": 10000 },
    "Double20": { "type": "scaled_float", "scaling_factor": 10000 },
    "Datetime01": { "type": "date_nanos" },
    "Datetime02": { "type": "date_nanos" },
    "Datetime03": { "type": "date_nanos" },
    "Datetime04": { "type": "date_nanos" },
    "Datetime05": { "type": "date_nanos" },
    "Datetime06": { "type": "date_nanos" },
    "Datetime07": { "type": "date_nanos" },
    "Datetime08": { "type": "date_nanos" },
    "Datetime09": { "type": "date_nanos" },
    "Datetime10": { "type": "date_nanos" },
    "Datetime11": { "type": "date_nanos" },
    "Datetime12": { "type": "date_nanos" },
    "Datetime13": { "type": "date_nanos" },
    "Datetime14": { "type": "date_nanos" },
    "Datetime15": { "type": "date_nanos" },
    "Datetime16": { "type": "date_nanos" },
    "Datetime17": { "type": "date_nanos" },
    "Datetime18": { "type": "date_nanos" },
    "Datetime19": { "type": "date_nanos" },
    "Datetime20": { "type": "date_nanos" },
    "Keyword01": { "type": "keyword" },
    "Keyword02": { "type": "keyword" },
    "Keyword03": { "type": "keyword" },
    "Keyword04": { "type": "keyword" },
    "Keyword05": { "type": "keyword" },
    "Keyword06": { "type": "keyword" },
    "Keyword07": { "type": "keyword" },
    "Keyword08": { "type": "keyword" },
    "Keyword09": { "type": "keyword" },
    "Keyword10": { "type": "keyword" },
    "Keyword11": { "type": "keyword" },
    "Keyword12": { "type": "keyword" },
    "Keyword13": { "type": "keyword" },
    "Keyword14": { "type": "keyword" },
    "Keyword15": { "type": "keyword" },
    "Keyword16": { "type": "keyword" },
    "Keyword17": { "type": "keyword" },
    "Keyword18": { "type": "keyword" },
    "Keyword19": { "type": "keyword" },
    "Keyword20": { "type": "keyword" },
    "Keyword21": { "type": "keyword" },
    "Keyword22": { "type": "keyword" },
    "Keyword23": { "type": "keyword" },
    "Keyword24": { "type": "keyword" },
    "Keyword25": { "type": "keyword" },
    "Keyword26": { "type": "keyword" },
    "Keyword27": { "type": "keyword" },
    "Keyword28": { "type": "keyword" },
    "Keyword29": { "type": "keyword" },
    "Keyword30": { "type": "keyword" },
    "Keyword31": { "type": "keyword" },
    "Keyword32": { "type": "keyword" },
    "Keyword33": { "type": "keyword" },
    "Keyword34": { "type": "keyword" },
    "Keyword35": { "type": "keyword" },
    "Keyword36": { "type": "keyword" },
    "Keyword37": { "type": "keyword" },
    "Keyword38": { "type": "keyword" },
    "Keyword39": { "type": "keyword" },
    "Keyword40": { "type": "keyword" },
    "KeywordList01": { "type": "keyword" },
    "KeywordList02": { "type": "keyword" },
    "KeywordList03": { "type": "keyword" },
    "KeywordList04": { "type": "keyword" },
    "KeywordList05": { "type": "keyword" },
    "Text01": { "type": "text" },
    "Text02": { "type": "text" },
    "Text03": { "type": "text" },
    "Text04": { "type": "text" },
    "Text05": { "type": "text" }
>>>>>>> Stashed changes
  }
}
'

if [ -z "${AUTO_CONFIRM}" ]; then
<<<<<<< Updated upstream
    read -p "Add custom search attributes to the index ${ES_VIS_INDEX_V1}? (N/y)" -n 1 -r
=======
    read -p "Add custom search attribute fields to the index ${ES_VIS_INDEX_V1}? (N/y)" -n 1 -r
>>>>>>> Stashed changes
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
<<<<<<< Updated upstream
fi
=======
fi

>>>>>>> Stashed changes
