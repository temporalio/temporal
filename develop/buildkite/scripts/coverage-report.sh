#!/bin/sh

set -eu

# Download cover profiles from all the test containers.
buildkite-agent artifact download ".coverage/unit_coverprofile.out" . --step ":golang: unit test" --build "${BUILDKITE_BUILD_ID}"

# ES6.
buildkite-agent artifact download ".coverage/integ_cassandra_coverprofile.out" . --step ":golang: integration test with cassandra (ES6)" --build "${BUILDKITE_BUILD_ID}"
mv ./.coverage/integ_cassandra_coverprofile.out ./.coverage/integ_cassandra_es6_coverprofile.out

# Cassandra.
buildkite-agent artifact download ".coverage/integ_cassandra_coverprofile.out" . --step ":golang: integration test with cassandra" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/integ_xdc_cassandra_coverprofile.out" . --step ":golang: integration xdc test with cassandra" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/integ_ndc_cassandra_coverprofile.out" . --step ":golang: integration ndc test with cassandra" --build "${BUILDKITE_BUILD_ID}"

# MySQL.
buildkite-agent artifact download ".coverage/integ_mysql_coverprofile.out" . --step ":golang: integration test with mysql" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/integ_xdc_mysql_coverprofile.out" . --step ":golang: integration xdc test with mysql" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/integ_ndc_mysql_coverprofile.out" . --step ":golang: integration ndc test with mysql" --build "${BUILDKITE_BUILD_ID}"

# PostgreSQL.
buildkite-agent artifact download ".coverage/integ_postgres_coverprofile.out" . --step ":golang: integration test with postgresql" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/integ_xdc_postgres_coverprofile.out" . --step ":golang: integration xdc test with postgresql" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/integ_ndc_postgres_coverprofile.out" . --step ":golang: integration ndc test with postgresql" --build "${BUILDKITE_BUILD_ID}"

# SQLite.
buildkite-agent artifact download ".coverage/integ_sqlite_coverprofile.out" . --step ":golang: integration test with sqlite" --build "${BUILDKITE_BUILD_ID}"

make ci-coverage-report
