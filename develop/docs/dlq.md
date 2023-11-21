# Managing DLQ Using tdbg
When a task in the Temporal server encounters a terminal error, it may be enqueued in the Dead Letter Queue (DLQ). 
To effectively manage these DLQ messages, follow these steps:

1. Search `Task enqueued to DLQ` in logs and see task details.
2. From the log trace, you can identify the offending namespace, task type, source cluster, and target cluster of the task.
3. From the task type, get the corresponding dlq type from this map:
    1. transfer: 1
    2. timer: 2
    3. replication: 3
    4. visibility 4
4. Search `Marking task as terminally failed, will send to DLQ` in logs and find the terminal error that caused the task to be enqueued to DLQ.
5. You can list the DLQ messages using
   `tdbg dlq --dlq-version v2 read --dlq-type {type}`. Substitute `{type}` with the integer value from step 3.

Now these DLQ tasks can either be purged(remove from DLQ), or merged(Re-enqueue to the original queue and try to execute again).

To purge this message, follow these steps:
1. Obtain the `message_id` by using the `tdbg dlq --dlq-version v2 read --dlq-type {type}` command.
2. Execute the purge command:
   `tdbg dlq --dlq-version v2 purge --dlq-type {type} --last_message_id {message_id}`. Note that this will purge all messages with an ID less than or equal to the specified `message_id`.

To merge this message, follow these steps:
1. Obtain the `message_id` by using the `tdbg dlq --dlq-version v2 read --dlq-type {type}` command.
2. Use the merge command:
   `tdbg dlq --dlq-version v2 merge --dlq-type {type} --last_message_id {message_id}`. This will merge all messages with an ID less than or equal to `message_id` back into the original queue for reprocessing.

Additionally, you can list all DLQs using the command `tdbg dlq --dlq-version v2 list` This command will list all queues in the decreasing order of message count.