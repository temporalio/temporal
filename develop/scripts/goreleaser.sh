#!/bin/sh

export GIT_REVISION=$(git rev-parse --short HEAD)
export GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
export GIT_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo unknown)
export BUILD_DATE=$(date '+%F-%T') # outputs something in this format 2017-08-21-18:58:45
export BUILD_TS_UNIX=$(date '+%s') # second since epoch
export BUILD_PLATFORM=$(go version | cut -d ' ' -f 4) # from "go version go1.15.3 linux/amd64"
export GO_VERSION=$(go version | cut -d ' ' -f 3) # from "go version go1.15.3 linux/amd64"

curl -sfL https://install.goreleaser.com/github.com/goreleaser/goreleaser.sh | sh

./bin/goreleaser "$@"
