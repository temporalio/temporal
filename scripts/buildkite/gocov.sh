#!/bin/sh

set -ex

# fetch codecov reporting tool
go get github.com/dmetzgar/goveralls

# download cover files from all the tests
mkdir -p build/coverage
buildkite-agent artifact download "build/coverage/unit_cover.out" . --step ":golang: unit test" --build "$BUILDKITE_BUILD_ID"
buildkite-agent artifact download "build/coverage/integ_cassandra_cover.out" . --step ":golang: integration test with cassandra" --build "$BUILDKITE_BUILD_ID"
buildkite-agent artifact download "build/coverage/integ_xdc_cassandra_cover.out" . --step ":golang: integration xdc test with cassandra" --build "$BUILDKITE_BUILD_ID"
buildkite-agent artifact download "build/coverage/integ_sql_cover.out" . --step ":golang: integration test with mysql" --build "$BUILDKITE_BUILD_ID"
buildkite-agent artifact download "build/coverage/integ_xdc_sql_cover.out" . --step ":golang: integration xdc test with mysql" --build "$BUILDKITE_BUILD_ID"

echo "download complete"

# report coverage
make cover_ci

# cleanup
rm -rf build
