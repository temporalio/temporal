#!/bin/sh

set -eu

./develop/scripts/create_build_info_data.sh

curl -sfL https://install.goreleaser.com/github.com/goreleaser/goreleaser.sh | sh

./bin/goreleaser "$@"
