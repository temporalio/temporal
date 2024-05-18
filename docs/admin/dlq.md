# Operating the Dead Letter Queue (DLQ)
When a task in the Temporal server encounters a terminal error, it may be enqueued in the Dead Letter Queue (DLQ).
A terminal error is a non-retryable error, such as failing to deserialize data due to data corruption.
To effectively manage these DLQ messages, follow these steps:

## Configuration

### History Replication DLQ
To enable the DLQ for history replication tasks, set `history.enableHistoryReplicationDLQV2` to true.

### History Tasks DLQ
To enable the DLQ for non-replication history tasks, set `history.TaskDLQEnabled` to true.
You can specify the maximum number of task execution attempts with unexpected errors using the dynamic config
`history.TaskDLQUnexpectedErrorAttempts`. The task will be sent to DLQ after the specified number of attempts.

If you need to capture tasks that fail with specific errors, you can utilize the config flag `history.TaskDLQErrorPattern`. 
This allows you to define a regular expression that will match one or more errors. However, exercise caution when 
specifying the regular expression to avoid unintended matches. Once configured, all task processing errors will be 
compared against this pattern, which could affect performance. Therefore, use this flag only when necessary.

## Detection
There is a metric `dlq_writes`, which is incremented each time a message is enqueued to the DLQ.
You can use this to determine when a task encountered a terminal error and needs manual resolution.

https://github.com/temporalio/temporal/blob/1de185f3d615bb5e7876804e89aeb7086d16833e/common/metrics/metric_defs.go#L917-L920

## Identification
Search for `Task enqueued to DLQ` in the logs and inspect the `dlq-message-id`, `xdc-source-cluster`, `xdc-target-cluster`, `queue-task-type` and `wf-namespace(-id)?` tags.
The source and target cluster tags are different only for replication tasks.
The namespace tag may have a `-id` suffix if we were unable to determine the namespace name from its ID.

## Investigation
From the task type, get the corresponding DLQ type from this map:
1. transfer: 1
2. timer: 2
3. replication: 3
4. visibility: 4

You can list the DLQ messages using the command:
`tdbg dlq --dlq-version v2 read --dlq-type {type}`. Substitute `{type}` with the integer value from step 3.
You can specify the maximum message ID to read using `--last-message-id` flag. 
In case of replication tasks, you can specify the source cluster using the flag `--cluster`.

Search `Marking task as terminally failed, will send to DLQ` in logs and find the terminal error that caused the task to be enqueued to the DLQ.

If you can't find the logs, you can list all DLQs using the command `tdbg dlq --dlq-version v2 list` and find non-empty queues. 
This command will list all queues in the decreasing order of message count.

Now these DLQ tasks can either be purged (removed from the DLQ), or merged (re-enqueued to the original queue which will
cause them to be retried).

## Resolution

### Deleting Tasks
To purge a message, execute the command `tdbg dlq --dlq-version v2 purge --dlq-type {type} --last-message-id {message_id}`. 
Note that this command will purge all messages with an ID less than or equal to the specified `message_id`. 
The output of this command will have a job token which can be used to manage the purge job.
Before executing this command, you can list the messages in the queue using the command mentioned in step 6 above to make sure more messages are not purged by mistake.

### Retrying Tasks
To merge a message, execute the command `tdbg dlq --dlq-version v2 merge --dlq-type {type} --last-message-id {message_id}`.
This command will merge all messages with an ID less than or equal to `message_id` back into the original queue for reprocessing.
The output of this command will have a job token that can be used to manage the merge job.

Once merge or purge command is executed, it will create a DLQ job which will process the DLQ messages. 
You can get the status of this job using the command `tdbg dlq --dlq-version v2 job describe --job-token {job-token}`. 
The value of job-token will be printed in the output of merge and purge commands.
The output of the describe command will have details like the last processed message ID, number of messages processed, etc.

### Cancelling Jobs
If you want to cancel a specific DLQ job, you can execute the command `tdbg dlq --dlq-version v2 job cancel --job-token {job-token} --reason {reason}`.
