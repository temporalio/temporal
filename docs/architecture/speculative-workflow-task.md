# Speculative Workflow Task

## Workflow Task Types
There are three types of Workflow Task:
  1. Normal
  2. Transient
  3. Speculative

Every Workflow Task ships history to the worker. Last events must be `WorkflowTaskScheduled`
and `WorkflowTaskStarted`. There might be some events in between if they come after Workflow Task
was scheduled but not started (e.g., Workflow worker was down and didn't poll for Workflow Task).

**Normal Workflow Task** is created by server every time when server needs Workflow to make progress.
If Workflow Task fails (worker responds with call to `RespondWorkflowTaskFailed` or an error occurred while
processing `RespondWorkflowTaskCompleted`) or times out (worker is disconnected),
server writes corresponding Workflow Task failed event in the history and increase attempt count
in the mutable state. For the next attempt, Workflow Task events (`WorkflowTaskScheduled`
and `WorkflowTaskStarted`) are not written into the history but attached to the response of
`RecordWorkflowTaskStarted` API. These are transient Workflow Task events. Worker is not aware of any "transience"
of these events. If Workflow Task keeps failing, attempt counter is getting increased in mutable state,
but no new fail events are written into the history and new transient Workflow Task events are just recreated.
Workflow Task, which has transient Workflow Task events, is called **transient Workflow Task**.
When Workflow Task finally completes, `WorkflowTaskScheduled` and `WorkflowTaskStarted` events
are getting written to history followed by `WorklfowTaskCompleted` event.

> #### TODO
> Although `WorkflowTaskInfo` struct has `Type` field, `WORKFLOW_TASK_TYPE_TRANSIENT` value is currently
> not used and `ms.IsTransientWorkflowTask()` method checks if attempts count > 1. 

**Speculative WT** is similar to transient WT: it creates `WorkflowTaskScheduled`
and `WorkflowTaskStarted` events only in response of `RecordWorkflowTaskStarted` API,
but it does it from the very first attempt. Also after speculative Workflow Task is scheduled and mutable state is updated
it is not written to the database. `WorkflowTaskInfo.Type` field is assigned to `WORKFLOW_TASK_TYPE_SPECULATIVE` value. 
Essentially speculative Workflow Task can exist in memory only, and it never gets written to the database.
Similar to CPU *speculative execution* (which gives a speculative Workflow Task its name) where branch execution 
can be thrown away, a speculative Workflow Task can be dropped as it never existed.
Overall logic, is to try to do the best and allow speculative Workflow Task to go through, but if anything
goes wrong, quickly give up, convert speculative Workflow Task to normal and follow normal procedures.

Zero database writes also means that transfer and regular timer tasks can't be used for
speculative Workflow Tasks. Special [in-memory-queue](./in-memory-queue.md) is used for force speculative Workflow Task timeouts.

> #### TODO
> It is important to point out that `WorkflowTaskScheduled` and `WorkflowTaskStarted` events for transient
> and speculative Workflow Task are added to `PollWorkflowTask` response only but not to `GetWorkflowExecutionHistory` response.
> This has unpleasant consequence: when worker receives speculative Workflow Task on sticky task queue, but
> Workflow is already evicted from cache, it sends a request to `GetWorkflowExecutionHistory`, which
> returns history without speculative events, which leads to `premature end of stream` error on worker side.
> It fails Workflow Task, clears stickiness, and everything works fine after that, but one extra failed Workflow Task appears
> in the history. Fortunately, it doesn't happen often.

## Speculative Workflow Task & Workflow Update
Speculative Workflow Task was introduced to support zero writes for Workflow Update, this is why it doesn't write 
nor events, neither mutable state.

> #### TODO
> Another application can be a replacement for query task that will unify two different
> code paths (by introducing 3rd one and slowly deprecating existing two).

## Scheduling of Speculative Workflow Task
Because currently speculative Workflow Task is used for Workflow Update only it is created in
`UpdateWorkflowExecution` API handler only. And because a normal transfer task can't be created
(speculative Workflow Task doesn't write to the database) it is directly added to matching service
with a call to `AddWorkflowTask` API. It is crucial to notice that Workflow lock
must be released before this call because in case of sync match, matching service will
do a callback to history service to start Workflow Task. This call will also try to acquire Workflow lock.

But call to matching can fail (for various reasons), and then this error can't be properly handled
outside of Workflow lock or returned to the user. Instead, an in-memory timer task
is created for `SCHEDULED_TO_START` timeout for speculative Workflow Task even if it is on normal task queue
(for normal Workflow Task `SCHEDULED_TO_START` timeout timer is created only for sticky task queue).
If call to matching failed, `UpdateWorkflowExecution` API caller will observe short delay
(`SpeculativeWorkflowTaskScheduleToStartTimeout` = 5s), but underneath timeout timer will fire,
convert speculative Workflow Task to normal and create a transfer task which will eventually push it through.

## Start of Speculative Workflow Task
Speculative Workflow Task's `WorkflowTaskScheduled` and `WorkflowTaskStarted` events are shipped on 
`TransientWorkflowTask` field of `RecordWorkflowTaskStartedResponse` and merged to the history
before shipping to worker. This code path is not different from transient Workflow Task.

## Completion of Speculative Workflow Task
### StartTime in the Token
Because a server can lose a speculative Workflow Task, it will not always be completed. Moreover, new
speculative Workflow Task can be created after the first one is lost, and then worker will try to complete the first one.
To prevent this `StartedTime` was added to Workflow Task token and if it doesn't match to start time in mutable state,
Workflow Task can't be completed. All other checks aren't necessary anymore, but left there just in case.

### Persist or Drop
While completing speculative Workflow Task server makes a decision: write speculative events followed by
`WorkflowTaskCompleted` event or drop speculative events and make speculative Workflow Task disappear.
Server can drop events only if it knows that this Workflow Task didn't change the Workflow state. Currently,
conditions are (check `skipWorkflowTaskCompletedEvent()` func):
 - response doesn't have any commands,
 - response has only Update rejection messages.

> #### TODO
> There is one more condition that forces speculative Workflow Task to be persisted: if there are
> events in the history prior to speculative Workflow Task, which were shipped by this speculative Workflow Task
> to the worker, then it can't be dropped. This is because an old version of some SDKs didn't
> support getting same events twice which would happen when the server drops one speculative Workflow Task,
> and then creates another with the same events. Now old SDKs support it, and with some 
> compatibility flag, this condition can be omitted.

When a server decides to drop a speculative Workflow Task, it needs to communicate this decision to SDK. 
Because SDK needs to know where to roll back its history event pointer, i.e., after what event,
all other events need to be dropped. SDK uses `ResetHistoryEventId` field on `RespondWorkflowTaskCompletedRespose`.
Server set it to `LastCompletedWorkflowTaskStartedEventId` field value because
SDK uses `WorkflowTaskStartedEventID` as history checkpoint.

### Heartbeat
Workflow Task can heartbeat. If worker completes Workflow Task with `ForceCreateNewWorkflowTask` is set to `true`
then server will create new Workflow Task even there are no new events.
Because currently speculative Workflow Task is used for Workflow Update only, it is very unlikely
that speculative Workflow Task can be completed as a heartbeat. Update validation logic
is supposed to be quick and worker should respond with a rejection or acceptance message.
But if it happens, server will persist all speculative Workflow Task events and create new Workflow Task as normal.

> #### TODO
> This is just a design decision, which can be changed later. Server can drop speculative Workflow Task
> when it heartbeats and create new one as speculative too. No new events will be added to the
> history which will save history events but also will decrease visibility of heartbeats.

## Conversion to Normal Workflow Task
Speculative Workflow Task is never written to the database. If, while speculative Workflow Task is executed,
something triggers mutable state write (i.e., new events come in), then speculative Workflow Task is converted
to normal, and then written to the database. `Type` field value is changed to `WORKFLOW_TASK_TYPE_NORMAL`,
in-memory timer is replaced with normal persisted timer, and corresponding speculative Workflow Task
`WorkflowTaskScheduled` and `WorkflowTaskStarted` events are written to the history
(`convertSpeculativeWorkflowTaskToNormal()` func). 

## Failure of Speculative Workflow Task
Workflow Task failure indicates a bug in the Workflow code, SDK, or server.
There are two major cases when a Workflow Task fails:
1. Worker explicitly calls `RespondWorkflowTaskFailed` API,
2. Worker calls `RespondWorkflowTaskCompleted` API, but there was error in request or while processing the request.

When speculative Workflow Task is failing `WorkflowTaskFailed` event is written to the history (followed by
`WorkflowTaskScheduled` and `WorkflowTaskStarted` events) because Workflow Task failure needs to be visible
to Workflow author.

Speculative Workflow Task is retired the same way as normal Workflow Task, which means that it becomes a transitive Workflow Task:
2nd failed attempt is not written to the history.

## Speculative Workflow Task Timeout
Speculative Workflow Task timeouts are enforced with special [in-memory timer queue](./in-memory-queue.md).
Unlike for normal Workflow Task `SCHEDULE_TO_START` timeout timer is created if speculative Workflow Task
is scheduled on both sticky and **normal** task queue. `START_TO_CLOSE` timer is created
 when a Workflow Task is started. There is only one timer exists for speculative Workflow Task at any given time.
Pointer to that timer is stored inside mutable state because timer needs to be canceled
when Workflow Task completes or fails: speculative Workflow Task timer can't be identified by `ScheduledEventID`
because there might be another speculative Workflow Task with the same `ScheduledEventID`, and if not canceled
can times out wrong Workflow Task. 

The behaviour in timeout handler is similar to when a Workflow Task fails.
First `WorkflowTaskScheduled` and `WorkflowTaskStarted` events are written to the history
(because they were not written for speculative Workflow Task), then `WorkflowTaskTimeout` event.
New Workflow Task is scheduled as normal, but because attempt count is increased,
it automatically becomes a transient Workflow Task.

## Replication of Speculative Workflow Task
Speculative Workflow Task is not replicated. Worker can try to complete it in new cluster, and server
will return `NotFound` error.
