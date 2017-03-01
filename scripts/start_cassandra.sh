#!/bin/bash

# this script starts the cassandra daemon and make sure it is
# started by waiting on port 9042 which is the default listen
# port for cqlsh

my_dir="$(dirname "$0")"
source "$my_dir/wait_for_cassandra.sh"

port=9042
# if cassandra pid file already exists, don't overwrite the pid file
cass_pid_file="/tmp/cassandra.pid"
if [ ! -f $cass_pid_file ]
then
	# pid file doesn't exist. start cassandra and write it to pid file
	cassandra -p /tmp/cassandra.pid 2>&1 >/tmp/cassandra.log
fi
wait_for_cassandra $port
