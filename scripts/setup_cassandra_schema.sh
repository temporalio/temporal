#!/bin/bash

# this script is used to setup the "cherami" keyspace
# and load all the tables in the .cql file within this keyspace,
# if cassandra is running
#
# this script is only intended to be used in test environments
# for updating prod schema, please refer to odp/cherami-tools and
# https://code.uberinternal.com/w/cherami/runbooks/

my_dir="$(dirname "$0")"
source "$my_dir/wait_for_cassandra.sh"

# the default cqlsh listen port is 9042
port=9042

# the default keyspace is workflow
# TODO: probably allow getting this from command line
workflow_keyspace="workflow"

wait_for_cassandra $port
res=$?
if [ $res -eq 0 ]; then
	./cassandra/bin/cqlsh -f ./schema/keyspace_test.cql
	./cassandra/bin/cqlsh -k $workflow_keyspace -f ./schema/workflow_test.cql
fi
