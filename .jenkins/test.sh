#!/bin/bash

bash scripts/cadence-stop-cassandra
bash scripts/cadence-start-cassandra
bash scripts/cadence-setup-schema

. .jenkins/build-common.sh

make clean
make bins

# e = exit on error
# x = echo on
set -ex
make -j5 -k test-xml TEST_FLAGS="-v -race -timeout 15m" PHAB_COMMENT=.phabricator-comment
set +e # Disable bail on failure. We don't care about lint errors and stopping cassandra

# make lint PHAB_COMMENT=.phabricator-comment

bash scripts/cadence-stop-cassandra
exit $?
