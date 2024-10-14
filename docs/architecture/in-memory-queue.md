# In-memory Timer Queue

This queue is similar to the normal persisted timer queue, but it exists only in memory, i.e. it 
never gets persisted. It is created by a generic `MemoryScheduledQueueFactory`, but currently serves
only [speculative Workflow Task](./speculative-workflow-task.md) timeouts. Therefore, the only queue
this factory creates is `SpeculativeWorkflowTaskTimeoutQueue` which uses the same task executor as
the normal timer queue: `TimerQueueActiveTaskExecutor`.

Its implementation uses a `PriorityQueue` sorted by `VisibilityTimestamp`: the task on top is the
task that is executed next.

The in-memory queue only supports `WorkflowTaskTimeoutTask`, and enforces the
`SCHEDULE_TO_START` and `START_TO_CLOSE` timeouts.

Note that while the in-memory queue's executor of `WorkflowTaskTimeoutTask` is the same as for
the normal timer queue, it does one extra check for speculative Workflow Tasks:
`CheckSpeculativeWorkflowTaskTimeoutTask` checks if a task being executed is still the *same* task
that's stored in mutable state. This is important since the mutable state can lose and create a *new*
speculative Workflow Task, and therefore the old timeout task must be ignored. 

> #### TODO
> Future refactoring is necessary to make the logic (and probably naming) clearer. It is not clear
> yet if the in-memory queue has other applications besides timeouts for speculative Workflow Tasks.
