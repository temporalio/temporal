#!/bin/bash

set -ex

DB="${DB:-cassandra}"

# Cassandra
KEYSPACE="${KEYSPACE:-temporal}"
VISIBILITY_KEYSPACE="${VISIBILITY_KEYSPACE:-temporal_visibility}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"

# MySQL
DBNAME="${DBNAME:-temporal}"
VISIBILITY_DBNAME="${VISIBILITY_DBNAME:-temporal_visibility}"
DB_PORT=${DB_PORT:-3306}

# Elasticsearch
ENABLE_ES="${ENABLE_ES:-false}"
ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS="${ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS:-0}"
ES_PORT="${ES_PORT:-9200}"
ES_VERSION="${ES_VERSION:-v6}"
ES_SCHEME="${ES_SCHEME:-http}"
ES_VIS_INDEX="${ES_VIS_INDEX:-temporal-visibility-dev}"

# Default namespace
DEFAULT_NAMESPACE="${DEFAULT_NAMESPACE:-default}"
DEFAULT_NAMESPACE_RETENTION=${DEFAULT_NAMESPACE_RETENTION:-1}

setup_cassandra_schema() {
    # TODO (alex): Remove exports
    export CASSANDRA_USER=$CASSANDRA_USER
    export CASSANDRA_PORT=$CASSANDRA_PORT
    export CASSANDRA_ENABLE_TLS=$CASSANDRA_TLS_ENABLED
    export CASSANDRA_TLS_CERT=$CASSANDRA_CERT
    export CASSANDRA_TLS_KEY=$CASSANDRA_CERT_KEY
    export CASSANDRA_TLS_CA=$CASSANDRA_CA
    export CASSANDRA_REPLICATION_FACTOR=${CASSANDRA_REPLICATION_FACTOR:-1}

    { export CASSANDRA_PASSWORD=$CASSANDRA_PASSWORD; } 2> /dev/null

    SCHEMA_DIR=$TEMPORAL_HOME/schema/cassandra/temporal/versioned
    temporal-cassandra-tool --ep $CASSANDRA_SEEDS create -k $KEYSPACE --rf $CASSANDRA_REPLICATION_FACTOR
    temporal-cassandra-tool --ep $CASSANDRA_SEEDS -k $KEYSPACE setup-schema -v 0.0
    temporal-cassandra-tool --ep $CASSANDRA_SEEDS -k $KEYSPACE update-schema -d $SCHEMA_DIR

    VISIBILITY_SCHEMA_DIR=$TEMPORAL_HOME/schema/cassandra/visibility/versioned
    temporal-cassandra-tool --ep $CASSANDRA_SEEDS create -k $VISIBILITY_KEYSPACE --rf $CASSANDRA_REPLICATION_FACTOR
    temporal-cassandra-tool --ep $CASSANDRA_SEEDS -k $VISIBILITY_KEYSPACE setup-schema -v 0.0
    temporal-cassandra-tool --ep $CASSANDRA_SEEDS -k $VISIBILITY_KEYSPACE update-schema -d $VISIBILITY_SCHEMA_DIR
}

setup_mysql_schema() {
    # TODO (alex): Remove exports
    { export SQL_PASSWORD=$MYSQL_PWD; } 2> /dev/null

    if [ "$MYSQL_TX_ISOLATION_COMPAT" == "true" ]; then
        MYSQL_CONNECT_ATTR='--connect-attributes tx_isolation=READ-COMMITTED'
    fi

    SCHEMA_DIR=$TEMPORAL_HOME/schema/mysql/v57/temporal/versioned
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER $MYSQL_CONNECT_ATTR create --db $DBNAME
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER $MYSQL_CONNECT_ATTR --db $DBNAME setup-schema -v 0.0
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER $MYSQL_CONNECT_ATTR --db $DBNAME update-schema -d $SCHEMA_DIR
    VISIBILITY_SCHEMA_DIR=$TEMPORAL_HOME/schema/mysql/v57/visibility/versioned
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER $MYSQL_CONNECT_ATTR create --db $VISIBILITY_DBNAME
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER $MYSQL_CONNECT_ATTR --db $VISIBILITY_DBNAME setup-schema -v 0.0
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER $MYSQL_CONNECT_ATTR --db $VISIBILITY_DBNAME update-schema -d $VISIBILITY_SCHEMA_DIR
}

setup_postgres_schema() {
    # TODO (alex): Remove exports
    { export SQL_PASSWORD=$POSTGRES_PWD; } 2> /dev/null

    SCHEMA_DIR=$TEMPORAL_HOME/schema/postgresql/v96/temporal/versioned
    temporal-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER -p $DB_PORT create --db $DBNAME
    temporal-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER -p $DB_PORT --db $DBNAME setup-schema -v 0.0
    temporal-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER -p $DB_PORT --db $DBNAME update-schema -d $SCHEMA_DIR
    VISIBILITY_SCHEMA_DIR=$TEMPORAL_HOME/schema/postgresql/v96/visibility/versioned
    temporal-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER -p $DB_PORT create --db $VISIBILITY_DBNAME
    temporal-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER -p $DB_PORT --db $VISIBILITY_DBNAME setup-schema -v 0.0
    temporal-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER -p $DB_PORT --db $VISIBILITY_DBNAME update-schema -d $VISIBILITY_SCHEMA_DIR
}

setup_schema() {
    if [ "$DB" == "mysql" ]; then
        echo 'Setup MySQL schema.'
        setup_mysql_schema
    elif [ "$DB" == "postgresql" ]; then
        echo 'Setup PostgreSQL schema.'
        setup_postgres_schema
    else
        echo 'Setup Cassandra schema.'
        setup_cassandra_schema
    fi
}

wait_for_cassandra() {
    # TODO (alex): Remove exports
    export CASSANDRA_USER=$CASSANDRA_USER
    export CASSANDRA_PORT=$CASSANDRA_PORT
    export CASSANDRA_ENABLE_TLS=$CASSANDRA_TLS_ENABLED
    export CASSANDRA_TLS_CERT=$CASSANDRA_CERT
    export CASSANDRA_TLS_KEY=$CASSANDRA_CERT_KEY
    export CASSANDRA_TLS_CA=$CASSANDRA_CA

    { export CASSANDRA_PASSWORD=$CASSANDRA_PASSWORD; } 2> /dev/null

    until temporal-cassandra-tool --ep $CASSANDRA_SEEDS validate-health < /dev/null; do
        echo 'Waiting for Cassandra to start up.'
        sleep 1
    done
    echo 'Cassandra started.'
}

wait_for_mysql() {
    ES_SERVER=`echo $MYSQL_SEEDS | awk -F ',' '{print $1}'`
    nc -z $ES_SERVER $DB_PORT < /dev/null
    until [ $? -eq 0 ]; do
        echo 'Waiting for MySQL to start up.'
        sleep 1
        nc -z $ES_SERVER $DB_PORT < /dev/null
    done
    echo 'MySQL started.'
}

wait_for_postgres() {
    ES_SERVER=`echo $POSTGRES_SEEDS | awk -F ',' '{print $1}'`
    nc -z $ES_SERVER $DB_PORT < /dev/null
    until [ $? -eq 0 ]; do
        echo 'Waiting for PostgreSQL to start up.'
        sleep 1
        nc -z $ES_SERVER $DB_PORT < /dev/null
    done
    echo 'PostgreSQL started.'
}

setup_es_template() {
    SCHEMA_FILE=$TEMPORAL_HOME/schema/elasticsearch/${ES_VERSION}/visibility/index_template.json
    ES_SERVER=`echo $ES_SEEDS | awk -F ',' '{print $1}'`
    URL="${ES_SCHEME}://$ES_SERVER:$ES_PORT/_template/temporal-visibility-template"
    curl -X PUT $URL -H 'Content-Type: application/json' --data-binary "@$SCHEMA_FILE"
    URL="${ES_SCHEME}://$ES_SERVER:$ES_PORT/$ES_VIS_INDEX"
    curl -X PUT $URL
}

wait_for_es() {
    SECONDS=0
    
    ES_SERVER=`echo $ES_SEEDS | awk -F ',' '{print $1}'`
    URL="${ES_SCHEME}://$ES_SERVER:$ES_PORT"
    curl -s $URL 2>&1 > /dev/null
    
    until [ $? -eq 0 ]; do
        DURATION=$SECONDS

        if [ $ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS -gt 0 ] && [ $DURATION -ge $ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS ]; then
            echo 'WARNING: timed out waiting for Elasticsearch to start up. Skipping index creation.'
            return;
        fi

        echo 'Waiting for Elasticsearch to start up.'
        sleep 1
        curl -s $URL 2>&1 > /dev/null
    done
    
    echo 'Elasticsearch started.'
}

wait_for_db() {
    if [ "$DB" == "mysql" ]; then
        wait_for_mysql
    elif [ "$DB" == "postgresql" ]; then
        wait_for_postgres
    else
        wait_for_cassandra
    fi
}

register_default_namespace() {
    echo "Temporal CLI Address: $TEMPORAL_CLI_ADDRESS."
    sleep 5
    echo "Registering default namespace: $DEFAULT_NAMESPACE."
    until tctl --ns $DEFAULT_NAMESPACE namespace describe < /dev/null; do
        echo "Default namespace $DEFAULT_NAMESPACE not found. Creating..."
        sleep 1
        tctl --ns $DEFAULT_NAMESPACE namespace register --rd $DEFAULT_NAMESPACE_RETENTION --desc "Default namespace for Temporal Server."
    done
    echo "Default namespace registration complete."
}

auto_setup() {
    if [ "$SKIP_SCHEMA_SETUP" != true ]; then
        wait_for_db
        setup_schema
    fi

    if [ "$ENABLE_ES" == true ]; then
        wait_for_es
        setup_es_template
    fi

    if [ "$SKIP_DEFAULT_NAMESPACE_CREATION" != true ]; then
        register_default_namespace &
    fi
}

auto_setup
