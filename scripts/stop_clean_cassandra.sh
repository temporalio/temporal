#!/bin/bash

# this script kills cassandra daemon if we have a /tmp/cassandra.pid
# file and cleans up the data and logs as well

cass_pid_file="/tmp/cassandra.pid"
if [ -f $cass_pid_file ]
then
	kill -9 `cat $cass_pid_file`
	rm $cass_pid_file /tmp/cassandra.log
fi
# cleanup data
rm -Rf ./cassandra/data ./cassandra/logs /tmp/cassandra
