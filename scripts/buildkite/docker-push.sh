#!/bin/bash

set -ex

# if current branch is not master, this PR is not merged to master yet
if [ "$BUILDKITE_BRANCH" != "master" ]; then
    echo "Skipping docker-push since this change is not in master yet"
    exit 0
fi

git fetch origin master
MASTER_SHA=$(git rev-parse origin/master)

# if this commit is not the same as tip of master, lets skip this and
# let the commit at tip of master do the push
if [ "$BUILDKITE_COMMIT" != "$MASTER_SHA" ]; then
    echo "Skipping docker-push for this commit since tip of master is already ahead"
    exit 0
fi

echo "Building docker image for $BUILDKITE_MESSAGE"

docker build . -f Dockerfile -t temporalio/temporal:master --build-arg TARGET=server
docker push temporalio/temporal:master

docker build . -f Dockerfile -t temporalio/temporal:master-auto-setup --build-arg TARGET=auto-setup 
docker push temporalio/temporal:master-auto-setup

docker build . -f Dockerfile -t temporalio/tctl:master --build-arg TARGET=tctl
docker push temporalio/tctl:master

docker build . -f Dockerfile -t temporalio/admin-tools:master --build-arg TARGET=admin-tools
docker push temporalio/admin-tools:master
