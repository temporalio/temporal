# the default cqlsh listen port is 9042
port=9042
host=compute2487-dca1.prod.uber.internal

## Check workflow executions
echo "Checking Workflow Executions that are not completed ...."

ROWSLINE=$(cqlsh $host --cqlversion=3.4.0 -e "use cadence; select workflow_id, run_id from executions where type=1 LIMIT 10 allow filtering;" | grep "dcb940ac-0c63-ffa2-ffea-a6c305881d71" -c)
if [ "$ROWSLINE" != "0" ]; then
    echo "Warning: Left over workflow executions ..."
    cqlsh $host --cqlversion=3.4.0 -e "use cadence; select workflow_id, run_id from executions where type=1 LIMIT 10 allow filtering;" | grep "dcb940ac-0c63-ffa2-ffea-a6c305881d71"
else
    echo "Workflow executions are clean"
fi

## Check transfer tasks
echo "Checking transfer tasks that are not completed ...."

ROWSLINE=$(cqlsh $host --cqlversion=3.4.0 -e "use cadence; select * from executions where type=2 LIMIT 10 allow filtering;" | grep "rows")
if [ "$ROWSLINE" != "(0 rows)" ]; then
    echo "Warning: Left over transfer tasks ..."
    cqlsh $host --cqlversion=3.4.0 -e "use cadence; select count(*) from executions where type=2 LIMIT 10 allow filtering;"
    cqlsh $host --cqlversion=3.4.0 -e "use cadence; select * from executions where type=2 LIMIT 10 allow filtering;"
else
    echo "Transfer tasks are clean"
fi

## Check timer tasks
echo "Checking timer tasks that are not completed ...."

ROWSLINE=$(cqlsh $host --cqlversion=3.4.0 -e "use cadence; select * from executions where type=3 LIMIT 10 allow filtering;" | grep "rows")
if [ "$ROWSLINE" != "(0 rows)" ]; then
    echo "Warning: Left over timers tasks..."
    cqlsh $host --cqlversion=3.4.0 -e "use cadence; select count(*) from executions where type=3 LIMIT 10 allow filtering;"
    cqlsh $host --cqlversion=3.4.0 -e "use cadence; select * from executions where type=3 LIMIT 10 allow filtering;"
else
    echo "Timer tasks are clean"
fi

## Check Task Queue
echo "Checking task queue that are not completed ...."

ROWSLINE=$(cqlsh $host --cqlversion=3.4.0 -e "use cadence; select * from tasks where type=0 LIMIT 10 allow filtering;" | grep "rows")
if [ "$ROWSLINE" != "(0 rows)" ]; then
    echo "Warning: Left over timers tasks..."
    cqlsh $host --cqlversion=3.4.0 -e "use cadence; select count(*) from tasks where type=0 LIMIT 10 allow filtering;"
    cqlsh $host --cqlversion=3.4.0 -e "use cadence; select * from tasks where type=0 LIMIT 10 allow filtering;"
else
    echo "Task queues are clean"
fi
