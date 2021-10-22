#!/bin/sh

set -eu

./develop/scripts/create_last_build_info.sh

curl -sfL https://install.goreleaser.com/github.com/goreleaser/goreleaser.sh | sh

./bin/goreleaser "$@"
