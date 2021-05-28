#!/bin/bash

set -eu -o pipefail

export SCRIPT=$(tr --delete "[:space:]" < ./schema/elasticsearch/reindex.js)
JSON=$(envsubst < ./schema/elasticsearch/reindex.json)

curl -X POST "http://127.0.0.1:9200/_reindex" -H "Content-Type: application/json" --data-binary "${JSON}" --write-out "\n"