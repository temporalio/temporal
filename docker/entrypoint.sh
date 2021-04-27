#!/bin/bash

set -e

export BIND_ON_IP="${BIND_ON_IP:-$(hostname -arg)}"

# tctl
if [[ "BIND_ON_IP" =~ ":" ]]
then  # ipv6
    export TEMPORAL_CLI_ADDRESS="[${BIND_ON_IP}]:7233"
else  # ipv4
    export TEMPORAL_CLI_ADDRESS="${BIND_ON_IP}:7233"
fi

export DB="${DB:-cassandra}"

# Cassandra
export KEYSPACE="${KEYSPACE:-temporal}"
export VISIBILITY_KEYSPACE="${VISIBILITY_KEYSPACE:-temporal_visibility}"
export CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"

# MySQL
export DBNAME="${DBNAME:-temporal}"
export VISIBILITY_DBNAME="${VISIBILITY_DBNAME:-temporal_visibility}"
export DB_PORT=${DB_PORT:-3306}

# Elasticsearch
export ENABLE_ES="${ENABLE_ES:-false}"
export ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS="${ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS:-0}"
export ES_PORT="${ES_PORT:-9200}"
export ES_VERSION="${ES_VERSION:-v6}"
export ES_SCHEME="${ES_SCHEME:-http}"
export ES_VIS_INDEX="${ES_VIS_INDEX:-temporal-visibility-dev}"

dockerize -template ./config/config_template.yaml:./config/docker.yaml

# autosetup
for arg in "$@" ; do [[ $arg == "autosetup" ]] && ./auto-setup.sh && break ; done

# debug
for arg in "$@" ; do [[ $arg == "debug" ]] && ./debug.sh && break ; done

# bash
for arg in "$@" ; do [[ $arg == "bash" ]] && bash && exit 0 ; done

./start-temporal.sh
