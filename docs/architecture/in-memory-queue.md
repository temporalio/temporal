# In-memory timer queue
This queue is similar to normal persisted timer queue, but it exists in memory only and never gets  
persisted. It is created with generic `MemoryScheduledQueueFactory`, but currently serves only
[speculative Workflow Task](./speculative-workflow-task.md) timeouts, therefore the only queue this factory creates
is `SpeculativeWorkflowTaskTimeoutQueue` which uses same task executor as normal timer queue:
`TimerQueueActiveTaskExecutor`.

Implementation uses `PriorityQueue` by `VisibilityTimestamp`: a task on top is the task that
executed next.

In-memory queue supports only `WorkflowTaskTimeoutTask` and there are two timeout types
enforced by in-memory queue: `SCHEDULED_TO_START` and `START_TO_CLOSE`.

Executor of `WorkflowTaskTimeoutTask` from in-memory queue is the same as for normal timer queue,
although it does one extra check for speculative Workflow Task. It checks if a task being executed still the same
as stored in mutable state (`CheckSpeculativeWorkflowTaskTimeoutTask`). This is because MS can lose and create
a new speculative Workflow Task, which will be a different Workflow Task and a timeout task must be skipped for it. 

> #### TODO
> Future refactoring is necessary to make logic (and probably naming) clearer. It is not clear
> if in-memory queue might have other applications besides timeouts for speculative Workflow Tasks.
