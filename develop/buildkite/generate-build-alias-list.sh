#!/usr/bin/env bash
# Fixes build-alias list in pipeline.yml.
# Install yq before running this.

set -e

cd develop/buildkite

{
  sed -n '1,/<<< build-alias list/ p' pipeline.yml
  yq -y '[ .services | to_entries[] | select(.value.build.context == "../..") | .key | select(. != "build") ]' docker-compose.yml | \
    sed 's/^/            /'
  sed -n '/>>> build-alias list/,$ p' pipeline.yml
} > pipeline.yml.tmp

mv pipeline.yml.tmp pipeline.yml
