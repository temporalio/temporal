#!/bin/bash

# the default cqlsh listen port is 9042
port=9042

if ["$1" != "onebox"]; then
    # Managed Cassandra
    host=compute2487-dca1.prod.uber.internal
    version=3.4.0
    namespace=cadence
    echo "Running checks against Managed Cassandra ..."
else
    # Onebox.
    host=127.0.0.1
    version=3.4.2
    namespace=$2
    echo "Running checks against local box ..."
fi

RED='\033[0;31m'
NC='\033[0m'

## Check workflow executions
echo "Checking Workflow Executions that are not completed ...."

ROWSLINE=$(cqlsh $host --cqlversion=$version -e "use $namespace; select workflow_id, run_id from executions where type=1 allow filtering;" | grep "dcb940ac-0c63-ffa2-ffea-a6c305881d71" -c)
if [ "$ROWSLINE" != "0" ]; then
    echo -e "${RED}WARNING: Left over workflow executions ...${NC}"
    echo "  workflow_id | run_id"
    cqlsh $host --cqlversion=$version -e "use $namespace; select workflow_id, run_id from executions where type=1 allow filtering;" | grep "dcb940ac-0c63-ffa2-ffea-a6c305881d71"
else
    echo "Workflow executions are clean"
fi

## Check transfer tasks
echo "Checking transfer tasks that are not completed ...."

ROWSLINE=$(cqlsh $host --cqlversion=$version -e "use $namespace; select * from executions where type=2 LIMIT 10 allow filtering;" | grep "rows")
if [ "$ROWSLINE" != "(0 rows)" ]; then
    echo -e "${RED}Warning: Left over transfer tasks ...${NC}"
    cqlsh $host --cqlversion=$version -e "use $namespace; select count(*) from executions where type=2 LIMIT 10 allow filtering;"
    cqlsh $host --cqlversion=$version -e "use $namespace; select * from executions where type=2 LIMIT 10 allow filtering;"
else
    echo "Transfer tasks are clean"
fi

## Check timer tasks
echo "Checking timer tasks that are not completed ...."

ROWSLINE=$(cqlsh $host --cqlversion=$version -e "use $namespace; select * from executions where type=3 LIMIT 10 allow filtering;" | grep "rows")
if [ "$ROWSLINE" != "(0 rows)" ]; then
    echo -e "${RED}Warning: Left over timers tasks...${NC}"
    cqlsh $host --cqlversion=$version -e "use $namespace; select count(*) from executions where type=3 LIMIT 10 allow filtering;"
    cqlsh $host --cqlversion=$version -e "use $namespace; select * from executions where type=3 LIMIT 10 allow filtering;"
else
    echo "Timer tasks are clean"
fi

## Check Task Queue
echo "Checking task queue that are not completed ...."

ROWSLINE=$(cqlsh $host --cqlversion=$version -e "use $namespace; select * from tasks where type=0 LIMIT 10 allow filtering;" | grep "rows")
if [ "$ROWSLINE" != "(0 rows)" ]; then
    echo -e "${RED}Warning: Left over tasks from task queue...${NC}"
    cqlsh $host --cqlversion=$version -e "use $namespace; select count(*) from tasks where type=0 LIMIT 10 allow filtering;"
    cqlsh $host --cqlversion=$version -e "use $namespace; select * from tasks where type=0 LIMIT 10 allow filtering;"
else
    echo "Task queues are clean"
fi
