#!/bin/bash

set -xu

bash scripts/cadence-stop-cassandra
bash scripts/cadence-start-cassandra
bash scripts/cadence-setup-schema

exit $?
