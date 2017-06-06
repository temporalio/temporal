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

setup_schema() {
    SCHEMA_FILE=$CADENCE_HOME/schema/cadence/schema.cql
    $CADENCE_HOME/cadence-cassandra-tool --ep $CASSANDRA_SEEDS create -k $KEYSPACE --rf $RF
    $CADENCE_HOME/cadence-cassandra-tool --ep $CASSANDRA_SEEDS -k $KEYSPACE setup-schema -d -f $SCHEMA_FILE
    VISIBILITY_SCHEMA_FILE=$CADENCE_HOME/schema/visibility/schema.cql
    $CADENCE_HOME/cadence-cassandra-tool --ep $CASSANDRA_SEEDS create -k $VISIBILITY_KEYSPACE --rf $RF
    $CADENCE_HOME/cadence-cassandra-tool --ep $CASSANDRA_SEEDS -k $VISIBILITY_KEYSPACE setup-schema -d -f $VISIBILITY_SCHEMA_FILE
}

wait_for_cassandra() {
    server=`echo $CASSANDRA_SEEDS | awk -F ',' '{print $1}'`
    until cqlsh --cqlversion=3.4.2 $server < /dev/null; do
        echo 'waiting for cassandra to start up'
        sleep 1
    done
    echo 'cassandra started'
}

init_env() {

    export HOST_IP=`hostname --ip-address`

    if [ "$BIND_ON_LOCALHOST" == true ]; then
            export HOST_IP="127.0.0.1"
    else
        export BIND_ON_LOCALHOST=false
    fi

    if [ -z "$KEYSPACE" ]; then
        export KEYSPACE="cadence"
    fi

    if [ -z "$VISIBILITY_KEYSPACE" ]; then
        export VISIBILITY_KEYSPACE="cadence_visibility"
    fi

    if [ -z "$CASSANDRA_SEEDS" ]; then
        export CASSANDRA_SEEDS=$HOST_IP
    fi

    if [ -z "$CASSANDRA_CONSISTENCY" ]; then
        export CASSANDRA_CONSISTENCY="One"
    fi

    if [ -z "$RINGPOP_SEEDS" ]; then
        export RINGPOP_SEEDS=$HOST_IP:7933
    fi

    if [ -z "$NUM_HISTORY_SHARDS" ]; then
        export NUM_HISTORY_SHARDS=4
    fi
}

CADENCE_HOME=$1

if [ -z "$RF" ]; then
    RF=1
fi

if [ -z "$SERVICES" ]; then
    SERVICES="history,matching,frontend"
fi

init_env
wait_for_cassandra

if [ "$SKIP_SCHEMA_SETUP" != true ]; then
    setup_schema
fi

# fix up config
envsubst < config/docker_template.yaml > config/docker.yaml
./cadence --root $CADENCE_HOME --env docker start --services=$SERVICES