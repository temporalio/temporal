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

# the default keyspace is workflow
# TODO: probably allow getting this from command line
workflow_keyspace="workflow"

# cmd/stress/stress -emitMetric=m3 -host="10.185.19.27,10.184.45.6,10.185.27.8,10.185.17.12,10.185.15.30"
for host in 10.185.19.27
do
  echo Installing schema on cassandra cluster via $host
  ./cassandra/bin/cqlsh -f ./schema/drop_keyspace_stress_test.cql $host $port
  ./cassandra/bin/cqlsh -f ./schema/keyspace_prod.cql $host $port
  ./cassandra/bin/cqlsh -k $workflow_keyspace -f ./schema/workflow_test.cql $host $port
done
