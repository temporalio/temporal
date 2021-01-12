#!/bin/bash

set -ex

git fetch origin master
MASTER_SHA=$(git rev-parse origin/master)

if [ "$BUILDKITE_COMMIT" != "$MASTER_SHA" ]; then
    echo "Skipping docker-push for this commit since tip of master is already ahead"
    exit 0
fi

echo "Building docker image for $BUILDKITE_MESSAGE ($BUILDKITE_COMMIT)"

docker build . -t temporalio/server:latest --build-arg TARGET=server
docker push temporalio/server:latest

docker build . -t temporalio/auto-setup:latest --build-arg TARGET=auto-setup
docker push temporalio/auto-setup:latest

docker build . -t temporalio/tctl:latest --build-arg TARGET=tctl
docker push temporalio/tctl:latest

docker build . -t temporalio/admin-tools:latest --build-arg TARGET=admin-tools
docker push temporalio/admin-tools:latest
