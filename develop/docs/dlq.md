# Managing DLQ Using tdbg
When a task in the Temporal server encounters a terminal error, it may be enqueued in the Dead Letter Queue (DLQ). 
To effectively manage these DLQ messages, follow these steps:

1. There is a metric `dlq_writes`, which will be incremented each time a message is enqueued to the DLQ.
2. Search `Task enqueued to DLQ` in logs and see task details. Get the DLQ message ID from this log line.
3. From the log trace, you can identify the offending namespace, task type, source cluster, and target cluster of the task.
4. From the task type, get the corresponding dlq type from this map:
    1. transfer: 1
    2. timer: 2
    3. replication: 3
    4. visibility: 4
5. Search `Marking task as terminally failed, will send to DLQ` in logs and find the terminal error that caused the task to be enqueued to the DLQ.
   Get the message_id
6. You can list the DLQ messages using
   `tdbg dlq --dlq-version v2 read --dlq-type {type}`. Substitute `{type}` with the integer value from step 4.

If you couldn't find the logs, you can list all DLQs using the command `tdbg dlq --dlq-version v2 list` and find non-empty queues.
This command will list all queues in the decreasing order of message count.

Now these DLQ tasks can either be purged(remove from DLQ), or merged(Re-enqueue to the original queue and try to execute again).

To purge a message, execute command `tdbg dlq --dlq-version v2 purge --dlq-type {type} --last_message_id {message_id}`. 
Note that this command will purge all messages with an ID less than or equal to the specified `message_id`. 
The output of this command will have a JobToken which can be used to manage the purge job.
You can list the messages in the queue using the command mentioned in step 6 above to make sure more messages are not purged by mistake.

To merge a message, execute command `tdbg dlq --dlq-version v2 merge --dlq-type {type} --last_message_id {message_id}`.
This command will merge all messages with an ID less than or equal to `message_id` back into the original queue for reprocessing.
The output of this command will have a JobToken which can be used to manage the merge job.

Once merge or purge command is executed, it will create a dlq job which will process the DLQ messages. You can get the status of this job using the command
`tdbg dlq --dlq-version v2 job describe --job-token {job-token}`. The value of job-token will be printed in the output or merge and purge commands.
The output of this command will have details like last processed message id, number of messages processed etc.

If you want to cancel a specific dlq job, you can execute the command `tdbg dlq --dlq-version v2 job cancel --job-token {job-token} --reason {reason}`.
