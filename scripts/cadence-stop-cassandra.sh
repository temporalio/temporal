#!/bin/bash

# this script kills cassandra daemon if we have a /tmp/cassandra.pid
# file and cleans up the data and logs as well

my_dir="$(dirname "$0")"
. $my_dir/cadence-environment
LISTEN_ADDRESS=${LISTEN_ADDRESS:-127.0.0.1}

cass_pid_file="$TMPDIR/cassandra-$LISTEN_ADDRESS.pid"
if [ -f $cass_pid_file ]
then
	kill -9 `cat $cass_pid_file`
	rm $cass_pid_file $TMPDIR/cassandra.log
fi
# cleanup data
# rm -Rf ./cassandra/data ./cassandra/logs $TMPDIR/cassandra
