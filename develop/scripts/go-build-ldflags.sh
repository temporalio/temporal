#!/bin/sh

set -eu

BASE_PACKAGE=$1

GIT_REVISION=$(git rev-parse --short HEAD)
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
GIT_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo unknown)
BUILD_DATE=$(date '+%F-%T') # outputs something in this format 2017-08-21-18:58:45
BUILD_TS_UNIX=$(date '+%s') # second since epoch
BUILD_PLATFORM=$(go version | cut -d ' ' -f 4) # from "go version go1.15.3 linux/amd64"
GO_VERSION=$(go version | cut -d ' ' -f 3) # from "go version go1.15.3 linux/amd64"

LD_FLAGS="-X ${BASE_PACKAGE}.GitRevision=${GIT_REVISION} \
-X ${BASE_PACKAGE}.GitBranch=${GIT_BRANCH} \
-X ${BASE_PACKAGE}.GitTag=${GIT_TAG} \
-X ${BASE_PACKAGE}.BuildDate=${BUILD_DATE} \
-X ${BASE_PACKAGE}.BuildTimeUnix=${BUILD_TS_UNIX} \
-X ${BASE_PACKAGE}.BuildPlatform=${BUILD_PLATFORM} \
-X ${BASE_PACKAGE}.GoVersion=${GO_VERSION}"

echo "${LD_FLAGS}"
