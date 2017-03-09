#!/bin/bash

# this script is used to setup the "cherami" keyspace
# and load all the tables in the .cql file within this keyspace,
# if cassandra is running
#
# this script is only intended to be used in test environments
# for updating prod schema, please refer to odp/cherami-tools and
# https://code.uberinternal.com/w/cherami/runbooks/

# the default cqlsh listen port is 9042
port=9042

# the default keyspace is cadence
# TODO: probably allow getting this from command line
workflow_keyspace="cadence"

DROP_UBER_MINIONS="$HOME/uber-minions"
DROP_CMD=$DROP_UBER_MINIONS/cmd/stress/
STRESS_LOG=stress_output.log

# This is our managed cassandra cluster.
host=compute2487-dca1.prod.uber.internal

echo Installing schema on managed cassandra cluster via $host

cqlsh --cqlversion=3.4.0 -f ./schema/drop_keyspace_stress_test.cql $host $port
cqlsh --cqlversion=3.4.0 -f ./schema/keyspace_prod.cql $host $port
cqlsh --cqlversion=3.4.0 -k $workflow_keyspace -f ./schema/workflow_test.cql $host $port
cqlsh --cqlversion=3.4.0 -k $workflow_keyspace -f ./schema/stress_schema_setup.cql $host $port

## We restart the stress by restarting the stress role on the uber-cadence.

# The below steps are for kicking off separately on cherami boxes.
#echo "Starting stress on"
#cd $DROP_CMD
#nohup ./stress -emitMetric=m3 -host="10.185.19.27,10.184.45.6,10.185.27.8,10.185.17.12,10.185.15.30" > $STRESS_LOG 2>&1 &
