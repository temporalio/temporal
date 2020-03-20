#!/bin/bash

set -x

DB="${DB:-cassandra}"
ENABLE_ES="${ENABLE_ES:-false}"
ES_PORT="${ES_PORT:-9200}"
RF=${RF:-1}
DEFAULT_DOMAIN_NAME="${DEFAULT_DOMAIN_NAME:-default}"
DEFAULT_DOMAIN_RETENTION=${DEFAULT_DOMAIN_RETENTION:-1}

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
    SCHEMA_DIR=$TEMPORAL_HOME/schema/mysql/v57/temporal/versioned
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER --pw $MYSQL_PWD create --db $DBNAME
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER --pw $MYSQL_PWD --db $DBNAME setup-schema -v 0.0
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER --pw $MYSQL_PWD --db $DBNAME update-schema -d $SCHEMA_DIR
    VISIBILITY_SCHEMA_DIR=$TEMPORAL_HOME/schema/mysql/v57/visibility/versioned
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER --pw $MYSQL_PWD create --db $VISIBILITY_DBNAME
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER --pw $MYSQL_PWD --db $VISIBILITY_DBNAME setup-schema -v 0.0
    temporal-sql-tool --ep $MYSQL_SEEDS -u $MYSQL_USER --pw $MYSQL_PWD --db $VISIBILITY_DBNAME update-schema -d $VISIBILITY_SCHEMA_DIR
}

setup_postgres_schema() {
    SCHEMA_DIR=$TEMPORAL_HOME/schema/postgres/temporal/versioned
    temporal-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER --pw $POSTGRES_PWD -p $DB_PORT create --db $DBNAME
    temporal-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER --pw $POSTGRES_PWD -p $DB_PORT --db $DBNAME setup-schema -v 0.0
    temporal-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER --pw $POSTGRES_PWD -p $DB_PORT --db $DBNAME update-schema -d $SCHEMA_DIR
    VISIBILITY_SCHEMA_DIR=$TEMPORAL_HOME/schema/postgres/visibility/versioned
    temporal-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER --pw $POSTGRES_PWD -p $DB_PORT create --db $VISIBILITY_DBNAME
    temporal-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER --pw $POSTGRES_PWD -p $DB_PORT --db $VISIBILITY_DBNAME setup-schema -v 0.0
    temporal-sql-tool --plugin postgres --ep $POSTGRES_SEEDS -u $POSTGRES_USER --pw $POSTGRES_PWD -p $DB_PORT --db $VISIBILITY_DBNAME update-schema -d $VISIBILITY_SCHEMA_DIR
}


setup_es_template() {
    SCHEMA_FILE=$TEMPORAL_HOME/schema/elasticsearch/visibility/index_template.json
    server=`echo $ES_SEEDS | awk -F ',' '{print $1}'`
    URL="http://$server:$ES_PORT/_template/temporal-visibility-template"
    curl -X PUT $URL -H 'Content-Type: application/json' --data-binary "@$SCHEMA_FILE"
    URL="http://$server:$ES_PORT/temporal-visibility-dev"
    curl -X PUT $URL
}

setup_schema() {
    if [ "$DB" == "mysql" ]; then
        echo 'setup mysql schema'
        setup_mysql_schema
    elif [ "$DB" == "postgres" ]; then
        echo 'setup postgres schema'
        setup_postgres_schema
    else
        echo 'setup cassandra schema'
        setup_cassandra_schema
    fi

    if [ "$ENABLE_ES" == "true" ]; then
        setup_es_template
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
        echo 'waiting for postgres to start up'
        sleep 1
        nc -z $server $DB_PORT < /dev/null
    done
    echo 'postgres started'
}


wait_for_es() {
    server=`echo $ES_SEEDS | awk -F ',' '{print $1}'`
    URL="http://$server:$ES_PORT"
    curl -s $URL 2>&1 > /dev/null
    until [ $? -eq 0 ]; do
        echo 'waiting for elasticsearch to start up'
        sleep 1
        curl -s $URL 2>&1 > /dev/null
    done
    echo 'elasticsearch started'
}

wait_for_db() {
    if [ "$DB" == "mysql" ]; then
        wait_for_mysql
    elif [ "$DB" == "postgres" ]; then
        wait_for_postgres
    else
        wait_for_cassandra
    fi

    if [ "$ENABLE_ES" == "true" ]; then
        wait_for_es
    fi
}

register_default_domain() {
    echo "Temporal CLI Address: $TEMPORAL_CLI_ADDRESS"
    sleep 5
    echo "Registering default domain: $DEFAULT_DOMAIN_NAME"
    until tctl --do $DEFAULT_DOMAIN_NAME domain describe < /dev/null; do
        echo "Default domain $DEFAULT_DOMAIN_NAME not found.  Creating..."
        sleep 1
        tctl --do $DEFAULT_DOMAIN_NAME domain register --rd $DEFAULT_DOMAIN_RETENTION --desc "Default domain for Temporal Server"
    done
    echo "Default domain registration complete."
}

wait_for_db
if [ "$SKIP_SCHEMA_SETUP" != true ]; then
    setup_schema
fi

if [ "$SKIP_DEFAULT_DOMAIN_CREATION" != true ]; then
    register_default_domain &
fi

bash /start-temporal.sh
