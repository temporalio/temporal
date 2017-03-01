#!/bin/bash

# this script simply implements the routine which waits for
# the daemon to show up on the given porti by using the netcat
# utility.
# by default, we retry 30 times (sleeping 1 sec between each retry)

wait_for_cassandra() {
    local port=$1
    local retrycount=30

    local i=0
    while true
    do
        # just fail after some max retries?
        if [ $i -gt $retrycount ]
        then
            echo "cassandra is not started"
            return 1
        fi
        
        if nc -z 127.0.0.1 $port >/dev/null 2>&1; then
        # cassandra is ready!
        # do a sanity check
        cqlsh -e "SELECT * FROM system.local LIMIT 1" > /dev/null 2>&1
        if [ $? -ne 0 ]
        then
            echo "unable to use cqlsh.. wait for some time"
            let i=i+1
            sleep 1
            continue
        fi
            return 0
        fi
        sleep 1
        let i=i+1
    done

    return 1
}
