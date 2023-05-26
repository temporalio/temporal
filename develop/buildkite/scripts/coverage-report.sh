#!/bin/sh

set -eu

# Download cover profiles from all the test containers.
buildkite-agent artifact download ".coverage/unit_coverprofile.out" . --step ":golang: unit test" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/integration_coverprofile.out" . --step ":golang: integration test" --build "${BUILDKITE_BUILD_ID}"

# ES8.
buildkite-agent artifact download ".coverage/functional_cassandra_coverprofile.out" . --step ":golang: functional test with cassandra (ES8)" --build "${BUILDKITE_BUILD_ID}"
mv ./.coverage/functional_cassandra_coverprofile.out ./.coverage/functional_cassandra_es8_coverprofile.out

# OpenSearch 2.
# buildkite-agent artifact download ".coverage/functional_cassandra_coverprofile.out" . --step ":golang: functional test with cassandra (OpenSearch 2)" --build "${BUILDKITE_BUILD_ID}"
# mv ./.coverage/functional_cassandra_coverprofile.out ./.coverage/functional_cassandra_os2_coverprofile.out

# Cassandra.
buildkite-agent artifact download ".coverage/functional_cassandra_coverprofile.out" . --step ":golang: functional test with cassandra" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/functional_xdc_cassandra_coverprofile.out" . --step ":golang: functional xdc test with cassandra" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/functional_ndc_cassandra_coverprofile.out" . --step ":golang: functional ndc test with cassandra" --build "${BUILDKITE_BUILD_ID}"

# MySQL.
buildkite-agent artifact download ".coverage/functional_mysql_coverprofile.out" . --step ":golang: functional test with mysql" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/functional_xdc_mysql_coverprofile.out" . --step ":golang: functional xdc test with mysql" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/functional_ndc_mysql_coverprofile.out" . --step ":golang: functional ndc test with mysql" --build "${BUILDKITE_BUILD_ID}"

# MySQL 8.
buildkite-agent artifact download ".coverage/functional_mysql8_coverprofile.out" . --step ":golang: functional test with mysql 8" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/functional_xdc_mysql8_coverprofile.out" . --step ":golang: functional xdc test with mysql 8" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/functional_ndc_mysql8_coverprofile.out" . --step ":golang: functional ndc test with mysql 8" --build "${BUILDKITE_BUILD_ID}"

# PostgreSQL.
buildkite-agent artifact download ".coverage/functional_postgres_coverprofile.out" . --step ":golang: functional test with postgresql" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/functional_xdc_postgres_coverprofile.out" . --step ":golang: functional xdc test with postgresql" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/functional_ndc_postgres_coverprofile.out" . --step ":golang: functional ndc test with postgresql" --build "${BUILDKITE_BUILD_ID}"

# PostgreSQL 12.
buildkite-agent artifact download ".coverage/functional_postgres12_coverprofile.out" . --step ":golang: functional test with postgresql 12" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/functional_xdc_postgres12_coverprofile.out" . --step ":golang: functional xdc test with postgresql 12" --build "${BUILDKITE_BUILD_ID}"
buildkite-agent artifact download ".coverage/functional_ndc_postgres12_coverprofile.out" . --step ":golang: functional ndc test with postgresql 12" --build "${BUILDKITE_BUILD_ID}"

# SQLite.
buildkite-agent artifact download ".coverage/functional_sqlite_coverprofile.out" . --step ":golang: functional test with sqlite" --build "${BUILDKITE_BUILD_ID}"

make ci-coverage-report
