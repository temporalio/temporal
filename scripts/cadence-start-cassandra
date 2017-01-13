#!/bin/bash
set -e
# this script starts the cassandra daemon and make sure it is
# started by waiting on port 9042 which is the default listen
# port for cqlsh

my_dir="$(dirname "$0")"
. $my_dir/cadence-environment
LISTEN_ADDRESS=${LISTEN_ADDRESS:-127.0.0.1}

source "$my_dir/cadence-wait-cassandra"

port=9042
# if cassandra pid file already exists, don't overwrite the pid file
cass_pid_file="$TMPDIR/cassandra-$LISTEN_ADDRESS.pid"
if [ ! -f $cass_pid_file ]
then
	# pid file doesn't exist. start cassandra and write it to pid file
	$CADENCE_CASSANDRA_DIR/cassandra -p $cass_pid_file -Dcassandra.config=file://$TMPDIR/cassandra.yaml 2>&1 >$TMPDIR/cassandra-$LISTEN_ADDRESS.log
fi

set +e
wait_for_cassandra $port

if [ $? -eq 0 ]; then
      echo "Cassandra started.";
else
      echo "Cassandra failed to start!";
      cat $TMPDIR/cassandra-$LISTEN_ADDRESS.log;
      lsof -p `cat $cass_pid_file`
      exit 1;
fi
