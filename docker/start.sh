#!/bin/bash -x

# Copyright (c) 2017 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

CADENCE_HOME=$1
DB="${DB:-cassandra}"
CFG_TEMPLATE=docker_template_$DB.yaml
SERVICES="${SERVICES:-history,matching,frontend,worker}"
RF=${RF:-1}
export LOG_LEVEL="${LOG_LEVEL:-info}"
export NUM_HISTORY_SHARDS=${NUM_HISTORY_SHARDS:-4}

# cassandra env
export KEYSPACE="${KEYSPACE:-cadence}"
export VISIBILITY_KEYSPACE="${VISIBILITY_KEYSPACE:-cadence_visibility}"
export CASSANDRA_CONSISTENCY="${CASSANDRA_CONSISTENCY:-One}"

#mysql env
export DBNAME="${DBNAME:-cadence}"
export VISIBILITY_DBNAME="${VISIBILITY_DBNAME:-cadence_visibility}"
export DB_PORT=${DB_PORT:-3306}


setup_cassandra_schema() {
    SCHEMA_DIR=$CADENCE_HOME/schema/cassandra/cadence/versioned
    $CADENCE_HOME/cadence-cassandra-tool --ep $CASSANDRA_SEEDS create -k $KEYSPACE --rf $RF
    $CADENCE_HOME/cadence-cassandra-tool --ep $CASSANDRA_SEEDS -k $KEYSPACE setup-schema -v 0.0
    $CADENCE_HOME/cadence-cassandra-tool --ep $CASSANDRA_SEEDS -k $KEYSPACE update-schema -d $SCHEMA_DIR
    VISIBILITY_SCHEMA_DIR=$CADENCE_HOME/schema/cassandra/visibility/versioned
    $CADENCE_HOME/cadence-cassandra-tool --ep $CASSANDRA_SEEDS create -k $VISIBILITY_KEYSPACE --rf $RF
    $CADENCE_HOME/cadence-cassandra-tool --ep $CASSANDRA_SEEDS -k $VISIBILITY_KEYSPACE setup-schema -v 0.0
    $CADENCE_HOME/cadence-cassandra-tool --ep $CASSANDRA_SEEDS -k $VISIBILITY_KEYSPACE update-schema -d $VISIBILITY_SCHEMA_DIR
}

setup_mysql_schema() {
    mysql -h $MYSQL_SEEDS -u$MYSQL_USER -p$MYSQL_PWD < $CADENCE_HOME/schema/mysql/v57/cadence/database.sql
    mysql -h $MYSQL_SEEDS -u$MYSQL_USER -p$MYSQL_PWD $DBNAME < $CADENCE_HOME/schema/mysql/v57/cadence/schema.sql
    mysql -h $MYSQL_SEEDS -u$MYSQL_USER -p$MYSQL_PWD < $CADENCE_HOME/schema/mysql/v57/visibility/database.sql
    mysql -h $MYSQL_SEEDS -u$MYSQL_USER -p$MYSQL_PWD $VISIBILITY_DBNAME < $CADENCE_HOME/schema/mysql/v57/visibility/schema.sql
}

setup_schema() {
    if [ "$DB" == "mysql" ]; then
        setup_mysql_schema
    else
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

wait_for_db() {
    if [ "$DB" == "mysql" ]; then
        wait_for_mysql
    else
        wait_for_cassandra
    fi
}


json_array() {
  echo -n '['
  while [ $# -gt 0 ]; do
    x=${1//\\/\\\\}
    echo -n \"${x//\"/\\\"}\"
    [ $# -gt 1 ] && echo -n ', '
    shift
  done
  echo ']'
}

init_env() {
    export HOST_IP=`hostname --ip-address`

    if [ "$BIND_ON_LOCALHOST" == true ] || [ "$BIND_ON_IP" == "127.0.0.1" ]; then
        export BIND_ON_IP="127.0.0.1"
        export HOST_IP="127.0.0.1"
    elif [ -z "$BIND_ON_IP" ]; then
        # not binding to localhost and bind_on_ip is empty - use default host ip addr
        export BIND_ON_IP=$HOST_IP
    elif [ "$BIND_ON_IP" != "0.0.0.0" ]; then
        # binding to a user specified addr, make sure HOST_IP also uses the same addr
        export HOST_IP=$BIND_ON_IP
    fi

    // this env variable is deprecated
    export BIND_ON_LOCALHOST=false

    if [ -z "$RINGPOP_SEEDS" ]; then
        export RINGPOP_SEEDS_JSON_ARRAY="[\"$HOST_IP:7933\",\"$HOST_IP:7934\",\"$HOST_IP:7935\",\"$HOST_IP:7939\"]"
    else
        array=(${RINGPOP_SEEDS//,/ })
        export RINGPOP_SEEDS_JSON_ARRAY=$(json_array "${array[@]}")
    fi
}

init_env
wait_for_db
if [ "$SKIP_SCHEMA_SETUP" != true ]; then
    setup_schema
fi

# fix up config
envsubst < config/$CFG_TEMPLATE > config/docker.yaml
./cadence-server --root $CADENCE_HOME --env docker start --services=$SERVICES
