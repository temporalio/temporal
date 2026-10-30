# Speculative Workflow Task

## Workflow Task Types
There are three types of Workflow Task:
  1. Normal
  2. Transient
  3. Speculative

Every Workflow Task ships history events to the worker. It must always contain the two events
`WorkflowTaskScheduled` and `WorkflowTaskStarted` (must be last!). There might be some events in-between
them if they came in after the Workflow Task was scheduled but not yet started (e.g. when the Workflow
worker was down and didn't poll for Workflow Task).

A **normal Workflow Task** is created by the server when it needs a Workflow to make progress. If 
the Workflow Task fails (i.e. worker responds with a call to `RespondWorkflowTaskFailed` or an error
occurred while processing `RespondWorkflowTaskCompleted`) or times out (e.g. worker is disconnected),
the server writes a corresponding Workflow Task failed event to the history and increases the
attempt count in the mutable state.

For the next attempt, a **transient Workflow Task** is used: the *transient* Workflow Task events
`WorkflowTaskScheduled` and `WorkflowTaskStarted` are *not* written to the history, but attached to
the response from the `RecordWorkflowTaskStarted` API. The worker does not know the events are
transient, though. If the Workflow Task keeps failing, the attempt counter is increased in the
mutable state, and transient Workflow Task events are created again - but no new failure event is
written into the history again. When the Workflow Task finally completes, the `WorkflowTaskScheduled`
and `WorkflowTaskStarted` events are written to the history, followed by the `WorklfowTaskCompleted`
event.

> #### TODO
> Although the `WorkflowTaskInfo` struct has a `Type` field, `WORKFLOW_TASK_TYPE_TRANSIENT` value is
> currently not used. Instead, `ms.IsTransientWorkflowTask()` checks if the attempts count > 1.

**Speculative Workflow Task** is similar to the transient one in that it attaches the
`WorkflowTaskScheduled` and `WorkflowTaskStarted` events to the response from the
`RecordWorkflowTaskStarted` API. But there are some differences:
- it happens on the first attempt already
- it is *never* written to the database
- it is scheduled differently (see more details below)
- its `WorkflowTaskInfo.Type` is `WORKFLOW_TASK_TYPE_SPECULATIVE`

Similar to a CPU's *speculative execution* (which gives this Workflow Task its name) where a branch
execution can be thrown away, a speculative Workflow Task can be discarded as if it never existed.
The overall strategy is to optimistically assume the speculative Workflow Task will go through, but
if anything goes wrong, give up quickly and convert the speculative Workflow Task to a normal one.

Zero database writes also means that transfer and regular timer tasks can't be used here. Instead,
a special [in-memory-queue](./in-memory-queue.md) is used for speculative Workflow Task timeouts.

> #### TODO
> It is important to point out that the `WorkflowTaskScheduled` and `WorkflowTaskStarted` events
> for transient and speculative Workflow Task are only added to the `PollWorkflowTask` response - and
> not to the `GetWorkflowExecutionHistory` response. This has an unfortunate consequence: when the
> worker receives a speculative Workflow Task on a sticky task queue, but the Workflow is already
> evicted from its cache, it issues a `GetWorkflowExecutionHistory` request, which returns the
> history *without* speculative events. This leads to a `premature end of stream` error on the
> worker side. The worker fails the Workflow Task, clears stickiness, and everything works fine
> after that - but a failed Workflow Task appears in the history. Fortunately, it doesn't happen often.
>
> See PR #9325 for related work on ensuring transient events are not incorrectly returned to CLI/UI clients.

## Speculative Workflow Task & Workflow Update
Speculative Workflow Task was introduced to make it possible for Workflow Update to have zero writes
for when it is rejected. This is why it doesn't persist any events or the mutable state.

> #### TODO
> The task processig for Queries could be replaced by using speculative Workflow Tasks under the hood.

## Scheduling of Speculative Workflow Task
As of today, speculative Workflow Tasks are only used for Workflow Update, i.e. in the 
`UpdateWorkflowExecution` API handler. Since a normal transfer task can't be created (because that
would require a database write), it is added directly to the Matching service with a call to the
`AddWorkflowTask` API. 

It is crucial to note that Workflow lock *must* be released before this call because in case of
sync match, the Matching service will make a call to the history service to start the Workflow Task,
which will attempt to get the Workflow lock and result in a deadlock.

However, when the call to the matching service fails (e.g. due to networking issues), that error
can't be properly handled outside of the Workflow lock, or returned to the user. In that case, the
`UpdateWorkflowExecution` API caller will observe a short delay
(`SpeculativeWorkflowTaskScheduleToStartTimeout` is 5s) until the timeout timer task fires, then
the speculative Workflow Task is converted to a normal one and creates a transfer task which will
eventually reach matching and the worker.

The timeout timer task is is created for a `SCHEDULE_TO_START` timeout for every speculative
Workflow Task - even if it is on a *normal* task queue. In comparision, for a normal Workflow Task, the
`SCHEDULE_TO_START` timeout timer is only created for *sticky* task queues.

## Start of Speculative Workflow Task
Speculative Workflow Task's `WorkflowTaskScheduled` and `WorkflowTaskStarted` events are shipped
inside the `TransientWorkflowTask` field of `RecordWorkflowTaskStartedResponse` and are merged to 
the history before it is shipped to the worker. It is the same code path as for the transient
Workflow Task.

## Completion of Speculative Workflow Task

### `StartTime` in the Token
Because the server can lose a speculative Workflow Task, it will not always be completed. Moreover,
a new speculative Workflow Task can be created after the first one is lost, but the worker will
try to complete the first one. To prevent this, `StartedTime` was added to the Workflow Task token
and if it doesn't match the start time in mutable state, the Workflow Task can't be completed.

### Persist or Discard
While completing a speculative Workflow Task, the server makes a decision to either write the 
speculative events followed by a `WorkflowTaskCompleted` event - or discard the speculative events and
make the speculative Workflow Task disappear. The latter can only happen if the server knows that
this Workflow Task didn't change the Workflow state. Currently, the conditions are
(check `skipWorkflowTaskCompletedEvent()` func):
 - response doesn't have any commands,
 - response has only Update rejection messages.

The speculative Workflow Task can also ship other events (e.g. `ActivityTaskScheduled` or `TimerStarted`)
that were generated from previous Workflow Task commands (also known as command-events).
Unfortunately, older SDKs don't support receiving same events more
than once. If SDK supports this, it will set `DiscardSpeculativeWorkflowTaskWithEvents` flag to `true`
and the server will discard speculative Workflow Task even if it had events. These events can be shipped
multiply times if Updates keep being rejected. To prevent shipping a large set of events to the worker over
and over again, the server persists speculative Workflow Task if a number of events exceed
`DiscardSpeculativeWorkflowTaskMaximumEventsCount` threshold.

> #### NOTE
> This is possible because of an important server invariant: the Workflow history can only end with:
> - Workflow Task event (Scheduled, Started, Completed, Failed, Timeout),
> 
> or
> - command-event, generated from previous Workflow Task command.
> All these events don't change the Workflow state on the worker side. This invariant must not be 
> broken by other features.

When the server decides to discard a speculative Workflow Task, it needs to communicate this decision to 
the worker - because the SDK needs to roll back to a previous history event and discard all events after
that one. To do that, the server will set the `ResetHistoryEventId` field on the
`RespondWorkflowTaskCompletedResponse` to the mutable state's `LastCompletedWorkflowTaskStartedEventId`
(since the SDK uses `WorkflowTaskStartedEventID` as its history checkpoint).

### Heartbeat
Workflow Tasks can heartbeat: when the worker completes a Workflow Task with `ForceCreateNewWorkflowTask`
set to `true`, the server will create a new Workflow Task even if there are no new events. Currently, 
since speculative Workflow Tasks are only used for Workflow Update, it is very unlikely to occur here.
The Update validation logic is supposed to be quick, and the worker is expected to respond with a
rejection or acceptance message. If it does happen, the server will persist all speculative Workflow Task
events and create a new Workflow Task as normal.

> #### NOTE
> This is a design decision, which could be changed later: instead, the server could discard the
> speculative Workflow Task when it heartbeats and create a new speculative Workflow Task. No
> new events would be added to the history - but heartbeats would not be visible anymore.

## Conversion to Normal Workflow Task
If during the exection of a speculative Workflow Task, a mutable state write is required
(i.e., a new events comes in), then it is converted to a normal one, and written to the database.
This means the `Type` field value is changed to `WORKFLOW_TASK_TYPE_NORMAL`, an in-memory timer is
replaced with a persisted timer, and the corresponding speculative Workflow Task `WorkflowTaskScheduled`
and `WorkflowTaskStarted` events are written to the history
(`convertSpeculativeWorkflowTaskToNormal()` func). 

## Failure of Speculative Workflow Task
A Workflow Task failure indicates a bug in the Workflow code, SDK, or server. The most common scenarios are:
1. Worker calls `RespondWorkflowTaskFailed` API
2. Worker calls `RespondWorkflowTaskCompleted` API, but there is an error in the request or
   while processing the request.

When a speculative Workflow Task is failing, a `WorkflowTaskFailed` event is written to the history
(followed by `WorkflowTaskScheduled` and `WorkflowTaskStarted` events) because a Workflow Task
failure must be visible to the Workflow author. Then, it is retried the same way a normal
Workflow Task is: it becomes a transitive Workflow Task.

## Speculative Workflow Task Timeout
Speculative Workflow Task timeouts are enforced with a special [in-memory timer queue](./in-memory-queue.md).

A `SCHEDULE_TO_START` timeout timer is always created, regardless of whether a sticky or normal
task queues is used. A normal Workflow Task will usually only do that for a sticky task queue.

A `START_TO_CLOSE` timeout timer is created when a speculative Workflow Task is started. There is
only one active timer at any given time. The timer needs to be canceled when the Workflow Task
completes or fails, but it cannot be identified by its `ScheduledEventID` because there might be
another speculative Workflow Task with the same `ScheduledEventID`, and it could time out the
wrong Workflow Task. Therefore, a pointer to that timer is stored inside the mutable state.

The behaviour in the timeout handler is similar to when a Workflow Task fails: first the
`WorkflowTaskScheduled` and `WorkflowTaskStarted` events are written to the history (because they 
were not written for speculative Workflow Task) and then the `WorkflowTaskTimeout` event. The new 
Workflow Task is scheduled as normal, but because the attempt count is increased, it automatically
becomes a transient Workflow Task.

## Replication of Speculative Workflow Task
Speculative Workflow Tasks are not replicated. The worker can try to complete it in new cluster,
and the server will return a `NotFound` error.
