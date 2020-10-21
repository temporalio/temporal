#!/bin/bash

set -x

DB="${DB:-cassandra}"
ENABLE_ES="${ENABLE_ES:-false}"
ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS="${ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS:-0}"
ES_PORT="${ES_PORT:-9200}"
ES_SCHEME="${ES_SCHEME:-http}"
RF=${RF:-1}
DEFAULT_NAMESPACE="${DEFAULT_NAMESPACE:-default}"
DEFAULT_NAMESPACE_RETENTION=${DEFAULT_NAMESPACE_RETENTION:-1}

# tctl env
export TEMPORAL_CLI_ADDRESS="${BIND_ON_IP}:7233"

# cassandra env
export KEYSPACE="${KEYSPACE:-temporal}"
export VISIBILITY_KEYSPACE="${VISIBILITY_KEYSPACE:-temporal_visibility}"

# mysql env
export DBNAME="${DBNAME:-temporal}"
export VISIBILITY_DBNAME="${VISIBILITY_DBNAME:-temporal_visibility}"
export DB_PORT=${DB_PORT:-3306}

setup_cassandra_schema() {
    SCHEMA_DIR=$TEMPORAL_HOME/schema/cassandra/temporal/versioned
    temporal-cassandra-tool --ep $CASSANDRA_SEEDS create -k $KEYSPACE --rf $RF
    temporal-cassandra-tool --ep $CASSANDRA_SEEDS -k $KEYSPACE setup-schema -v 0.0
    temporal-cassandra-tool --ep $CASSANDRA_SEEDS -k $KEYSPACE update-schema -d $SCHEMA_DIR
    VISIBILITY_SCHEMA_DIR=$TEMPORAL_HOME/schema/cassandra/visibility/versioned
    temporal-cassandra-tool --ep $CASSANDRA_SEEDS create -k $VISIBILITY_KEYSPACE --rf $RF
    temporal-cassandra-tool --ep $CASSANDRA_SEEDS -k $VISIBILITY_KEYSPACE setup-schema -v 0.0
    temporal-cassandra-tool --ep $CASSANDRA_SEEDS -k $VISIBILITY_KEYSPACE update-schema -d $VISIBILITY_SCHEMA_DIR
}

setup_mysql_schema() {
    { export SQL_PASSWORD=$MYSQL_PWD; } 2> /dev/null

    SCHEMA_DIR=$TEMPORAL_HOME/schema/mysql/v57/temporal/versioned

    if [ "$MYSQL_TX_ISOLATION_COMPAT" == "true" ]; then
        MYSQL_CONNECT_ATTR='--connect-attributes tx_isolation=READ-COMMITTED'
    fi

    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER $MYSQL_CONNECT_ATTR create --db $DBNAME
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER $MYSQL_CONNECT_ATTR --db $DBNAME setup-schema -v 0.0
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER $MYSQL_CONNECT_ATTR --db $DBNAME update-schema -d $SCHEMA_DIR
    VISIBILITY_SCHEMA_DIR=$TEMPORAL_HOME/schema/mysql/v57/visibility/versioned
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER $MYSQL_CONNECT_ATTR create --db $VISIBILITY_DBNAME
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER $MYSQL_CONNECT_ATTR --db $VISIBILITY_DBNAME setup-schema -v 0.0
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER $MYSQL_CONNECT_ATTR --db $VISIBILITY_DBNAME update-schema -d $VISIBILITY_SCHEMA_DIR
}

setup_postgres_schema() {
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


setup_es_template() {
    SCHEMA_FILE=$TEMPORAL_HOME/schema/elasticsearch/visibility/index_template.json
    server=`echo $ES_SEEDS | awk -F ',' '{print $1}'`
    URL="${ES_SCHEME}://$server:$ES_PORT/_template/temporal-visibility-template"
    curl -X PUT $URL -H 'Content-Type: application/json' --data-binary "@$SCHEMA_FILE"
    URL="${ES_SCHEME}://$server:$ES_PORT/temporal-visibility-dev"
    curl -X PUT $URL
}

setup_schema() {
    if [ "$DB" == "mysql" ]; then
        echo 'setup mysql schema'
        setup_mysql_schema
    elif [ "$DB" == "postgresql" ]; then
        echo 'setup postgresql schema'
        setup_postgres_schema
    else
        echo 'setup cassandra schema'
        setup_cassandra_schema
    fi
}

wait_for_cassandra() {
    server=`echo $CASSANDRA_SEEDS | awk -F ',' '{print $1}'`
    until cqlsh --cqlversion=3.4.4 $server < /dev/null; do
        echo 'waiting for cassandra to start up'
        sleep 1
    done
    echo 'cassandra started'
}

wait_for_mysql() {
    server=`echo $MYSQL_SEEDS | awk -F ',' '{print $1}'`
    nc -z $server $DB_PORT < /dev/null
    until [ $? -eq 0 ]; do
        echo 'waiting for mysql to start up'
        sleep 1
        nc -z $server $DB_PORT < /dev/null
    done
    echo 'mysql started'
}

wait_for_postgres() {
    server=`echo $POSTGRES_SEEDS | awk -F ',' '{print $1}'`
    nc -z $server $DB_PORT < /dev/null
    until [ $? -eq 0 ]; do
        echo 'waiting for postgresql to start up'
        sleep 1
        nc -z $server $DB_PORT < /dev/null
    done
    echo 'postgres started'
}


setup_es() {
    SECONDS=0
    
    server=`echo $ES_SEEDS | awk -F ',' '{print $1}'`
    URL="${ES_SCHEME}://$server:$ES_PORT"
    curl -s $URL 2>&1 > /dev/null
    
    until [ $? -eq 0 ]; do
        duration=$SECONDS

        if [ $ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS -gt 0 ] && [ $duration -ge $ES_SCHEMA_SETUP_TIMEOUT_IN_SECONDS ]; then
            echo 'WARNING: timed out waiting for elasticsearch to start up. skipping index creation'
            return;
        fi

        echo 'waiting for elasticsearch to start up'
        sleep 1
        curl -s $URL 2>&1 > /dev/null
    done
    
    echo 'elasticsearch started'
    setup_es_template
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
    echo "Temporal CLI Address: $TEMPORAL_CLI_ADDRESS"
    sleep 5
    echo "Registering default namespace: $DEFAULT_NAMESPACE"
    until tctl --ns $DEFAULT_NAMESPACE namespace describe < /dev/null; do
        echo "Default namespace $DEFAULT_NAMESPACE not found.  Creating..."
        sleep 1
        tctl --ns $DEFAULT_NAMESPACE namespace register --rd $DEFAULT_NAMESPACE_RETENTION --desc "Default namespace for Temporal Server"
    done
    echo "Default namespace registration complete."
}

auto_setup() {
    wait_for_db
    if [ "$SKIP_SCHEMA_SETUP" != true ]; then
        setup_schema
    fi

    if [ "$SKIP_DEFAULT_NAMESPACE_CREATION" != true ]; then
        register_default_namespace &
    fi
}

if [ "$1" = "autosetup" ]; then
	auto_setup
fi

if [ "$ENABLE_ES" == "true" ]; then
    setup_es
fi

exec bash /start-temporal.sh
