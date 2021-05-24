#!/bin/sh

set -eu

GIT_REVISION=$(git rev-parse --short HEAD)
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
GIT_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo unknown)
BUILD_DATE=$(date '+%F-%T') # outputs something in this format 2017-08-21-18:58:45
BUILD_TS_UNIX=$(date '+%s') # second since epoch
BUILD_PLATFORM=$(go version | cut -d ' ' -f 4) # from "go version go1.15.3 linux/amd64"
GO_VERSION=$(go version | cut -d ' ' -f 3) # from "go version go1.15.3 linux/amd64"

export GIT_REVISION
export GIT_BRANCH
export GIT_TAG
export BUILD_DATE
export BUILD_TS_UNIX
export BUILD_PLATFORM
export GO_VERSION

curl -sfL https://install.goreleaser.com/github.com/goreleaser/goreleaser.sh | sh

./bin/goreleaser "$@"
