#!/bin/bash

set -eux -o pipefail

# === Auto setup defaults ===

DB="${DB:-cassandra}"
SKIP_SCHEMA_SETUP="${SKIP_SCHEMA_SETUP:-false}"

# Cassandra
KEYSPACE="${KEYSPACE:-temporal}"
VISIBILITY_KEYSPACE="${VISIBILITY_KEYSPACE:-temporal_visibility}"

CASSANDRA_SEEDS="${CASSANDRA_SEEDS:-}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
CASSANDRA_USER="${CASSANDRA_USER:-}"
CASSANDRA_PASSWORD="${CASSANDRA_PASSWORD:-}"
CASSANDRA_TLS_ENABLED="${CASSANDRA_TLS_ENABLED:-}"
CASSANDRA_CERT="${CASSANDRA_CERT:-}"
CASSANDRA_CERT_KEY="${CASSANDRA_CERT_KEY:-}"
CASSANDRA_CA="${CASSANDRA_CA:-}"
CASSANDRA_REPLICATION_FACTOR="${CASSANDRA_REPLICATION_FACTOR:-1}"

# MySQL/PostgreSQL
DBNAME="${DBNAME:-temporal}"
VISIBILITY_DBNAME="${VISIBILITY_DBNAME:-temporal_visibility}"
DB_PORT="${DB_PORT:-3306}"

MYSQL_SEEDS="${MYSQL_SEEDS:-}"
MYSQL_USER="${MYSQL_USER:-}"
MYSQL_PWD="${MYSQL_PWD:-}"
MYSQL_TX_ISOLATION_COMPAT="${MYSQL_TX_ISOLATION_COMPAT:-false}"

POSTGRES_SEEDS="${POSTGRES_SEEDS:-}"
POSTGRES_USER="${POSTGRES_USER:-}"
POSTGRES_PWD="${POSTGRES_PWD:-}"

# Elasticsearch
ENABLE_ES="${ENABLE_ES:-false}"
ES_SCHEME="${ES_SCHEME:-http}"
ES_SEEDS="${ES_SEEDS:-}"
ES_PORT="${ES_PORT:-9200}"
ES_USER="${ES_USER:-}"
ES_PWD="${ES_PWD:-}"
ES_VERSION="${ES_VERSION:-v7}"
ES_VIS_INDEX="${ES_VIS_INDEX:-temporal_visibility_v1_dev}"
ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS="${ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS:-0}"

# Server setup
TEMPORAL_CLI_ADDRESS="${TEMPORAL_CLI_ADDRESS:-}"

SKIP_DEFAULT_NAMESPACE_CREATION="${SKIP_DEFAULT_NAMESPACE_CREATION:-false}"
DEFAULT_NAMESPACE="${DEFAULT_NAMESPACE:-default}"
DEFAULT_NAMESPACE_RETENTION=${DEFAULT_NAMESPACE_RETENTION:-1}

SKIP_ADD_CUSTOM_SEARCH_ATTRIBUTES="${SKIP_ADD_CUSTOM_SEARCH_ATTRIBUTES:-false}"

# === Main database functions ===

validate_db_env() {
    if [ "${DB}" == "mysql" ]; then
        if [ -z "${MYSQL_SEEDS}" ]; then
            echo "MYSQL_SEEDS env must be set if DB is ${DB}."
            exit 1
        fi
    elif [ "${DB}" == "postgresql" ]; then
        if [ -z "${POSTGRES_SEEDS}" ]; then
            echo "POSTGRES_SEEDS env must be set if DB is ${DB}."
            exit 1
        fi
    elif [ "${DB}" == "cassandra" ]; then
        if [ -z "${CASSANDRA_SEEDS}" ]; then
            echo "CASSANDRA_SEEDS env must be set if DB is ${DB}."
            exit 1
        fi
    else
        echo "Unsupported DB type: ${DB}."
        exit 1
    fi
}

wait_for_cassandra() {
    # TODO (alex): Remove exports
    export CASSANDRA_USER=${CASSANDRA_USER}
    export CASSANDRA_PORT=${CASSANDRA_PORT}
    export CASSANDRA_ENABLE_TLS=${CASSANDRA_TLS_ENABLED}
    export CASSANDRA_TLS_CERT=${CASSANDRA_CERT}
    export CASSANDRA_TLS_KEY=${CASSANDRA_CERT_KEY}
    export CASSANDRA_TLS_CA=${CASSANDRA_CA}

    { export CASSANDRA_PASSWORD=${CASSANDRA_PASSWORD}; } 2> /dev/null

    until temporal-cassandra-tool --ep "${CASSANDRA_SEEDS}" validate-health; do
        echo 'Waiting for Cassandra to start up.'
        sleep 1
    done
    echo 'Cassandra started.'
}

wait_for_mysql() {
    until nc -z "${MYSQL_SEEDS%%,*}" "${DB_PORT}"; do
        echo 'Waiting for MySQL to start up.'
        sleep 1
    done

    echo 'MySQL started.'
}

wait_for_postgres() {
    until nc -z "${POSTGRES_SEEDS%%,*}" "${DB_PORT}"; do
        echo 'Waiting for PostgreSQL to startup.'
        sleep 1
    done

    echo 'PostgreSQL started.'
}

wait_for_db() {
    if [ "${DB}" == "mysql" ]; then
        wait_for_mysql
    elif [ "${DB}" == "postgresql" ]; then
        wait_for_postgres
    elif [ "${DB}" == "cassandra" ]; then
        wait_for_cassandra
    else
        echo "Unsupported DB type: ${DB}."
        exit 1
    fi
}

setup_cassandra_schema() {
    # TODO (alex): Remove exports
    export CASSANDRA_USER=${CASSANDRA_USER}
    export CASSANDRA_PORT=${CASSANDRA_PORT}
    export CASSANDRA_ENABLE_TLS=${CASSANDRA_TLS_ENABLED}
    export CASSANDRA_TLS_CERT=${CASSANDRA_CERT}
    export CASSANDRA_TLS_KEY=${CASSANDRA_CERT_KEY}
    export CASSANDRA_TLS_CA=${CASSANDRA_CA}

    { export CASSANDRA_PASSWORD=${CASSANDRA_PASSWORD}; } 2> /dev/null

    SCHEMA_DIR=${TEMPORAL_HOME}/schema/cassandra/temporal/versioned
    temporal-cassandra-tool --ep "${CASSANDRA_SEEDS}" create -k "${KEYSPACE}" --rf "${CASSANDRA_REPLICATION_FACTOR}"
    temporal-cassandra-tool --ep "${CASSANDRA_SEEDS}" -k "${KEYSPACE}" setup-schema -v 0.0
    temporal-cassandra-tool --ep "${CASSANDRA_SEEDS}" -k "${KEYSPACE}" update-schema -d "${SCHEMA_DIR}"

    VISIBILITY_SCHEMA_DIR=${TEMPORAL_HOME}/schema/cassandra/visibility/versioned
    temporal-cassandra-tool --ep "${CASSANDRA_SEEDS}" create -k "${VISIBILITY_KEYSPACE}" --rf "${CASSANDRA_REPLICATION_FACTOR}"
    temporal-cassandra-tool --ep "${CASSANDRA_SEEDS}" -k "${VISIBILITY_KEYSPACE}" setup-schema -v 0.0
    temporal-cassandra-tool --ep "${CASSANDRA_SEEDS}" -k "${VISIBILITY_KEYSPACE}" update-schema -d "${VISIBILITY_SCHEMA_DIR}"
}

setup_mysql_schema() {
    # TODO (alex): Remove exports
    { export SQL_PASSWORD=${MYSQL_PWD}; } 2> /dev/null

    if [ "${MYSQL_TX_ISOLATION_COMPAT}" == "true" ]; then
        MYSQL_CONNECT_ATTR=(--connect-attributes "tx_isolation=READ-COMMITTED")
    else
        MYSQL_CONNECT_ATTR=()
    fi

    SCHEMA_DIR=${TEMPORAL_HOME}/schema/mysql/v57/temporal/versioned
    temporal-sql-tool --ep "${MYSQL_SEEDS}" -u "${MYSQL_USER}" "${MYSQL_CONNECT_ATTR[@]}" create --db "${DBNAME}"
    temporal-sql-tool --ep "${MYSQL_SEEDS}" -u "${MYSQL_USER}" "${MYSQL_CONNECT_ATTR[@]}" --db "${DBNAME}" setup-schema -v 0.0
    temporal-sql-tool --ep "${MYSQL_SEEDS}" -u "${MYSQL_USER}" "${MYSQL_CONNECT_ATTR[@]}" --db "${DBNAME}" update-schema -d "${SCHEMA_DIR}"
    VISIBILITY_SCHEMA_DIR=${TEMPORAL_HOME}/schema/mysql/v57/visibility/versioned
    temporal-sql-tool --ep "${MYSQL_SEEDS}" -u "${MYSQL_USER}" "${MYSQL_CONNECT_ATTR[@]}" create --db "${VISIBILITY_DBNAME}"
    temporal-sql-tool --ep "${MYSQL_SEEDS}" -u "${MYSQL_USER}" "${MYSQL_CONNECT_ATTR[@]}" --db "${VISIBILITY_DBNAME}" setup-schema -v 0.0
    temporal-sql-tool --ep "${MYSQL_SEEDS}" -u "${MYSQL_USER}" "${MYSQL_CONNECT_ATTR[@]}" --db "${VISIBILITY_DBNAME}" update-schema -d "${VISIBILITY_SCHEMA_DIR}"
}

setup_postgres_schema() {
    # TODO (alex): Remove exports
    { export SQL_PASSWORD=${POSTGRES_PWD}; } 2> /dev/null

    SCHEMA_DIR=${TEMPORAL_HOME}/schema/postgresql/v96/temporal/versioned
    # Create database only if its name is different from the user name. Otherwise PostgreSQL container itself will create database.
    if [ "${DBNAME}" != "${POSTGRES_USER}" ]; then
        temporal-sql-tool --plugin postgres --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" -p "${DB_PORT}" create --db "${DBNAME}"
    fi
    temporal-sql-tool --plugin postgres --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" -p "${DB_PORT}" --db "${DBNAME}" setup-schema -v 0.0
    temporal-sql-tool --plugin postgres --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" -p "${DB_PORT}" --db "${DBNAME}" update-schema -d "${SCHEMA_DIR}"
    VISIBILITY_SCHEMA_DIR=${TEMPORAL_HOME}/schema/postgresql/v96/visibility/versioned
    if [ "${VISIBILITY_DBNAME}" != "${POSTGRES_USER}" ]; then
        temporal-sql-tool --plugin postgres --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" -p "${DB_PORT}" create --db "${VISIBILITY_DBNAME}"
    fi
    temporal-sql-tool --plugin postgres --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" -p "${DB_PORT}" --db "${VISIBILITY_DBNAME}" setup-schema -v 0.0
    temporal-sql-tool --plugin postgres --ep "${POSTGRES_SEEDS}" -u "${POSTGRES_USER}" -p "${DB_PORT}" --db "${VISIBILITY_DBNAME}" update-schema -d "${VISIBILITY_SCHEMA_DIR}"
}

setup_schema() {
    if [ "${DB}" == "mysql" ]; then
        echo 'Setup MySQL schema.'
        setup_mysql_schema
    elif [ "${DB}" == "postgresql" ]; then
        echo 'Setup PostgreSQL schema.'
        setup_postgres_schema
    else
        echo 'Setup Cassandra schema.'
        setup_cassandra_schema
    fi
}

# === Elasticsearch functions ===

validate_es_env() {
    if [ "${ENABLE_ES}" == true ]; then
        if [ -z "${ES_SEEDS}" ]; then
            echo "ES_SEEDS env must be set if ENABLE_ES is ${ENABLE_ES}"
            exit 1
        fi
    fi
}

wait_for_es() {
    SECONDS=0

    ES_SERVER="${ES_SCHEME}://${ES_SEEDS%%,*}:${ES_PORT}"

    until curl --silent --fail --user "${ES_USER}":"${ES_PWD}" "${ES_SERVER}" > /dev/null 2>&1; do
        DURATION=${SECONDS}

        if [ "${ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS}" -gt 0 ] && [ ${DURATION} -ge "${ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS}" ]; then
            echo 'WARNING: timed out waiting for Elasticsearch to start up. Skipping index creation.'
            return;
        fi

        echo 'Waiting for Elasticsearch to start up.'
        sleep 1
    done

    echo 'Elasticsearch started.'
}

setup_es_index() {
    ES_SERVER="${ES_SCHEME}://${ES_SEEDS%%,*}:${ES_PORT}"
# @@@SNIPSTART setup-es-template-commands
    # ES_SERVER is the URL of Elasticsearch server i.e. "http://localhost:9200".
    SETTINGS_URL="${ES_SERVER}/_cluster/settings"
    SETTINGS_FILE=${TEMPORAL_HOME}/schema/elasticsearch/visibility/cluster_settings_${ES_VERSION}.json
    TEMPLATE_URL="${ES_SERVER}/_template/temporal_visibility_v1_template"
    SCHEMA_FILE=${TEMPORAL_HOME}/schema/elasticsearch/visibility/index_template_${ES_VERSION}.json
    INDEX_URL="${ES_SERVER}/${ES_VIS_INDEX}"
    curl --fail --user "${ES_USER}":"${ES_PWD}" -X PUT "${SETTINGS_URL}" -H "Content-Type: application/json" --data-binary "@${SETTINGS_FILE}" --write-out "\n"
    curl --fail --user "${ES_USER}":"${ES_PWD}" -X PUT "${TEMPLATE_URL}" -H 'Content-Type: application/json' --data-binary "@${SCHEMA_FILE}" --write-out "\n"
    curl --user "${ES_USER}":"${ES_PWD}" -X PUT "${INDEX_URL}" --write-out "\n"
# @@@SNIPEND
}

# === Server setup ===

register_default_namespace() {
    echo "Registering default namespace: ${DEFAULT_NAMESPACE}."
    if ! tctl --ns "${DEFAULT_NAMESPACE}" namespace describe; then
        echo "Default namespace ${DEFAULT_NAMESPACE} not found. Creating..."
        tctl --ns "${DEFAULT_NAMESPACE}" namespace register --rd "${DEFAULT_NAMESPACE_RETENTION}" --desc "Default namespace for Temporal Server."
        echo "Default namespace ${DEFAULT_NAMESPACE} registration complete."
    else
        echo "Default namespace ${DEFAULT_NAMESPACE} already registered."
    fi
}

add_custom_search_attributes() {
      echo "Adding Custom*Field search attributes."
      # TODO: Remove CustomStringField
# @@@SNIPSTART add-custom-search-attributes-for-testing-command
      tctl --auto_confirm admin cluster add-search-attributes \
          --name CustomKeywordField --type Keyword \
          --name CustomStringField --type Text \
          --name CustomTextField --type Text \
          --name CustomIntField --type Int \
          --name CustomDatetimeField --type Datetime \
          --name CustomDoubleField --type Double \
          --name CustomBoolField --type Bool
# @@@SNIPEND
}

setup_server(){
    echo "Temporal CLI address: ${TEMPORAL_CLI_ADDRESS}."

    until tctl cluster health | grep SERVING; do
        echo "Waiting for Temporal server to start..."
        sleep 1
    done
    echo "Temporal server started."

    if [ "${SKIP_DEFAULT_NAMESPACE_CREATION}" != true ]; then
        register_default_namespace
    fi

    if [ "${SKIP_ADD_CUSTOM_SEARCH_ATTRIBUTES}" != true ]; then
        add_custom_search_attributes
    fi
}

# === Main ===

if [ "${SKIP_SCHEMA_SETUP}" != true ]; then
    validate_db_env
    wait_for_db
    setup_schema
fi

if [ "${ENABLE_ES}" == true ]; then
    validate_es_env
    wait_for_es
    setup_es_index
fi

# Run this func in parallel process. It will wait for server to start and then run required steps.
setup_server &
