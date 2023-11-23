# Managing DLQ Using tdbg
When a task in the Temporal server encounters a terminal error, it may be enqueued in the Dead Letter Queue (DLQ).
A terminal error is a non-retryable error, such as failing to deserialize data due to data corruption.
To effectively manage these DLQ messages, follow these steps:

1. There is a metric `dlq_writes`, which is incremented each time a message is enqueued to the DLQ.
   You can use this to determine when a task encountered a terminal error and needs manual resolution.
2. Search `Task enqueued to DLQ` in logs and see task details. From the log trace, you can identify the task type,
   source cluster, target cluster, and DLQ message ID. Source cluster and target cluster are different only for replication tasks.
3. From the task type, get the corresponding DLQ type from this map:
    1. transfer: 1
    2. timer: 2
    3. replication: 3
    4. visibility: 4
4. You can list the DLQ messages using the command
   `tdbg dlq --dlq-version v2 read --dlq-type {type}`. Substitute `{type}` with the integer value from step 3.
   You can specify the maximum message ID to read using --last-message-id flag. In case of replication tasks, you can specify the source cluster
   using the flag --cluster.
5. Search `Marking task as terminally failed, will send to DLQ` in logs and find the terminal error that caused the task to be enqueued to the DLQ.

If you can't find the logs, you can list all DLQs using the command `tdbg dlq --dlq-version v2 list` and find non-empty queues.
This command will list all queues in the decreasing order of message count.

Now these DLQ tasks can either be purged(removed from DLQ), or merged(Re-enqueued to the original queue and try to execute again).

To purge a message, execute the command `tdbg dlq --dlq-version v2 purge --dlq-type {type} --last_message_id {message_id}`. 
Note that this command will purge all messages with an ID less than or equal to the specified `message_id`. 
The output of this command will have a job token which can be used to manage the purge job.
Before executing this command, you can list the messages in the queue using the command mentioned in step 6 above to make sure more messages are not purged by mistake.

To merge a message, execute the command `tdbg dlq --dlq-version v2 merge --dlq-type {type} --last_message_id {message_id}`.
This command will merge all messages with an ID less than or equal to `message_id` back into the original queue for reprocessing.
The output of this command will have a job token that can be used to manage the merge job.

Once merge or purge command is executed, it will create a DLQ job which will process the DLQ messages. You can get the status of this job using the command
`tdbg dlq --dlq-version v2 job describe --job-token {job-token}`. The value of job-token will be printed in the output of merge and purge commands.
The output of the describe command will have details like the last processed message ID, number of messages processed, etc.

If you want to cancel a specific DLQ job, you can execute the command `tdbg dlq --dlq-version v2 job cancel --job-token {job-token} --reason {reason}`.
