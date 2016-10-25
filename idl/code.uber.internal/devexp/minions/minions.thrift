namespace java com.uber.devexp.minions

exception BadRequestError {
  1: required string message
}

exception InternalServiceError {
  1: required string message
}

exception WorkflowExecutionAlreadyStartedError {
  1: required string message
}

exception EntityNotExistsError {
  1: required string message
}

enum TimeoutType {
  START_TO_CLOSE,
  SCHEDULE_TO_START,
  SCHEDULE_TO_CLOSE,
  HEARTBEAT,
}

enum DecisionType {
  ScheduleActivityTask,
  StartTimer,
  CompleteWorkflowExecution,
  FailWorkflowExecution,
}

enum EventType {
  WorkflowExecutionStarted,
  WorkflowExecutionCompleted,
  WorkflowExecutionFailed,
  WorkflowExecutionTimedOut,
  DecisionTaskScheduled,
  DecisionTaskStarted,
  DecisionTaskCompleted,
  DecisionTaskTimedOut
  ActivityTaskScheduled,
  ActivityTaskStarted,
  ActivityTaskCompleted,
  ActivityTaskFailed,
  ActivityTaskTimedOut,
  TimerStarted,
  TimerFired,
}

struct WorkflowType {
  10: optional string name
}

struct ActivityType {
  10: optional string name
}

struct TaskList {
  10: optional string name
}

struct WorkflowExecution {
  10: optional string workflowId
  20: optional string runId
}

struct ScheduleActivityTaskDecisionAttributes {
  10: optional string activityId
  20: optional ActivityType activityType
  30: optional TaskList taskList
  40: optional binary input
  45: optional i32 scheduleToCloseTimeoutSeconds
  50: optional i32 scheduleToStartTimeoutSeconds
  55: optional i32 startToCloseTimeoutSeconds
  60: optional i32 heartbeatTimeoutSeconds
}

struct StartTimerDecisionAttributes {
  10: optional string timerId
  20: optional i64 (js.type = "Long") startToFireTimeoutSeconds
}

struct CompleteWorkflowExecutionDecisionAttributes {
  10: optional binary result
}

struct FailWorkflowExecutionDecisionAttributes {
  10: optional string reason
  20: optional binary details
}

struct Decision {
  10: optional DecisionType decisionType
  20: optional ScheduleActivityTaskDecisionAttributes scheduleActivityTaskDecisionAttributes
  25: optional StartTimerDecisionAttributes startTimerDecisionAttributes
  30: optional CompleteWorkflowExecutionDecisionAttributes completeWorkflowExecutionDecisionAttributes
  35: optional FailWorkflowExecutionDecisionAttributes failWorkflowExecutionDecisionAttributes
}

struct WorkflowExecutionStartedEventAttributes {
  10: optional WorkflowType workflowType
  20: optional TaskList taskList
  30: optional binary input
  40: optional i32 executionStartToCloseTimeoutSeconds
  50: optional i32 taskStartToCloseTimeoutSeconds
  60: optional string identity
}

struct WorkflowExecutionCompletedEventAttributes {
  10: optional binary result
  20: optional i64 (js.type = "Long") decisionTaskCompletedEventId
}

struct WorkflowExecutionFailedEventAttributes {
  10: optional string reason
  20: optional binary details
  30: optional i64 (js.type = "Long") decisionTaskCompletedEventId
}

struct WorkflowExecutionTimedOutEventAttributes {
  10: optional TimeoutType timeoutType
}

struct DecisionTaskScheduledEventAttributes {
  10: optional TaskList taskList
  20: optional i32 startToCloseTimeoutSeconds
}

struct DecisionTaskStartedEventAttributes {
  10: optional i64 (js.type = "Long") scheduledEventId
  20: optional string identity
}

struct DecisionTaskCompletedEventAttributes {
  10: optional binary executionContext
  20: optional i64 (js.type = "Long") scheduledEventId
  30: optional i64 (js.type = "Long") startedEventId
  40: optional string identity
}

struct ActivityTaskScheduledEventAttributes {
  10: optional string activityId
  20: optional ActivityType activityType
  30: optional TaskList taskList
  40: optional binary input
  45: optional i32 scheduleToCloseTimeoutSeconds
  50: optional i32 scheduleToStartTimeoutSeconds
  55: optional i32 startToCloseTimeoutSeconds
  60: optional i32 heartbeatTimeoutSeconds
  90: optional i64 (js.type = "Long") decisionTaskCompletedEventId
}

struct ActivityTaskStartedEventAttributes {
  10: optional i64 (js.type = "Long") scheduledEventId
  20: optional string identity
}

struct ActivityTaskCompletedEventAttributes {
  10: optional binary result
  20: optional i64 (js.type = "Long") scheduledEventId
  30: optional i64 (js.type = "Long") startedEventId
  40: optional string identity
}

struct ActivityTaskFailedEventAttributes {
  10: optional string reason
  20: optional binary details
  30: optional i64 (js.type = "Long") scheduledEventId
  40: optional i64 (js.type = "Long") startedEventId
  50: optional string identity
}

struct ActivityTaskTimedOutEventAttributes {
  10: optional i64 (js.type = "Long") scheduledEventId
  20: optional i64 (js.type = "Long") startedEventId
  30: optional TimeoutType timeoutType
}

struct TimerStartedEventAttributes {
  10: optional string timerId
  20: optional i64 (js.type = "Long") startToFireTimeoutSeconds
  30: optional i64 (js.type = "Long") decisionTaskCompletedEventId
}

struct TimerFiredEventAttributes {
  10: optional string timerId
  20: optional i64 (js.type = "Long") startedEventId
}

struct HistoryEvent {
  10:  optional i64 (js.type = "Long") eventId
  20:  optional i64 (js.type = "Long") timestamp
  30:  optional EventType eventType
  35:  optional WorkflowExecutionStartedEventAttributes workflowExecutionStartedEventAttributes
  40:  optional WorkflowExecutionCompletedEventAttributes workflowExecutionCompletedEventAttributes
  45:  optional WorkflowExecutionFailedEventAttributes workflowExecutionFailedEventAttributes
  50:  optional WorkflowExecutionTimedOutEventAttributes workflowExecutionTimedOutEventAttributes
  55:  optional DecisionTaskScheduledEventAttributes decisionTaskScheduledEventAttributes
  60:  optional DecisionTaskStartedEventAttributes decisionTaskStartedEventAttributes
  65:  optional DecisionTaskCompletedEventAttributes decisionTaskCompletedEventAttributes
  70:  optional ActivityTaskScheduledEventAttributes activityTaskScheduledEventAttributes
  75:  optional ActivityTaskStartedEventAttributes activityTaskStartedEventAttributes
  80:  optional ActivityTaskCompletedEventAttributes activityTaskCompletedEventAttributes
  85:  optional ActivityTaskFailedEventAttributes activityTaskFailedEventAttributes
  90:  optional ActivityTaskTimedOutEventAttributes activityTaskTimedOutEventAttributes
  95:  optional TimerStartedEventAttributes timerStartedEventAttributes
  100: optional TimerFiredEventAttributes timerFiredEventAttributes
}

struct History {
  10: optional list<HistoryEvent> events
}

struct StartWorkflowExecutionRequest {
  10: optional string workflowId
  20: optional WorkflowType workflowType
  30: optional TaskList taskList
  40: optional binary input
  50: optional i32 executionStartToCloseTimeoutSeconds
  60: optional i32 taskStartToCloseTimeoutSeconds
  70: optional string identity
}

struct StartWorkflowExecutionResponse {
  10: optional string runId
}

struct PollForDecisionTaskRequest {
  10: optional TaskList taskList
  20: optional string identity
}

struct PollForDecisionTaskResponse {
  10: optional binary taskToken
  20: optional WorkflowExecution workflowExecution
  30: optional WorkflowType workflowType
  40: optional i64 (js.type = "Long") previousStartedEventId
  50: optional i64 (js.type = "Long") startedEventId
  60: optional History history
}

struct RespondDecisionTaskCompletedRequest {
  10: optional binary taskToken
  20: optional list<Decision> decisions
  30: optional binary executionContext
  40: optional string identity
}

struct PollForActivityTaskRequest {
  10: optional TaskList taskList
  20: optional string identity
}

struct PollForActivityTaskResponse {
  10: optional binary taskToken
  20: optional WorkflowExecution workflowExecution
  30: optional string activityId
  40: optional ActivityType activityType
  50: optional binary input
  60: optional i64 (js.type = "Long") startedEventId
}

struct RecordActivityTaskHeartbeatRequest {
  10: optional binary taskToken
  20: optional binary details
  30: optional string identity
}

struct RecordActivityTaskHeartbeatResponse {
}

struct RespondActivityTaskCompletedRequest {
  10: optional binary taskToken
  20: optional binary result
  30: optional string identity
}

struct RespondActivityTaskFailedRequest {
  10: optional binary taskToken
  20: optional string reason
  30: optional binary details
  40: optional string identity
}

/**
* WorkflowService API is exposed to provide support for long running applications.  Application is expected to call
* StartWorkflowExecution to create an instance for each instance of long running workflow.  Such applications are expected
* to have a worker which regularly polls for DecisionTask and ActivityTask from the WorkflowService.  For each
* DecisionTask, application is expected to process the history of events for that session and respond back with next
* decisions.  For each ActivityTask, application is expected to execute the actual logic for that task and respond back
* with completion or failure.  Worker is expected to regularly heartbeat while activity task is running.
**/
service WorkflowService {
  /**
  * StartWorkflowExecution starts a new long running workflow instance.  It will create the instance with
  * 'WorkflowExecutionStarted' event in history and also schedule the first DecisionTask for the worker to make the
  * first decision for this instance.  It will return 'WorkflowExecutionAlreadyStartedError', if an instance already
  * exists with same workflowId.
  **/
  StartWorkflowExecutionResponse StartWorkflowExecution(1: StartWorkflowExecutionRequest startRequest)
    throws (
      1: BadRequestError badRequestError,
      2: InternalServiceError internalServiceError,
      3: WorkflowExecutionAlreadyStartedError sessionAlreadyExistError,
    )

  /**
  * PollForDecisionTask is called by application worker to process DecisionTask from a specific taskList.  A
  * DecisionTask is dispatched to callers for active workflow executions, with pending decisions.
  * Application is then expected to call 'RespondDecisionTaskCompleted' API when it is done processing the DecisionTask.
  * It will also create a 'DecisionTaskStarted' event in the history for that session before handing off DecisionTask to
  * application worker.
  **/
  PollForDecisionTaskResponse PollForDecisionTask(1: PollForDecisionTaskRequest pollRequest)
    throws (
      1: BadRequestError badRequestError,
      2: InternalServiceError internalServiceError,
    )

  /**
  * RespondDecisionTaskCompleted is called by application worker to complete a DecisionTask handed as a result of
  * 'PollForDecisionTask' API call.  Completing a DecisionTask will result in new events for the workflow execution and
  * potentially new ActivityTask being created for corresponding decisions.  It will also create a DecisionTaskCompleted
  * event in the history for that session.  Use the 'taskToken' provided as response of PollForDecisionTask API call
  * for completing the DecisionTask.
  **/
  void RespondDecisionTaskCompleted(1: RespondDecisionTaskCompletedRequest completeRequest)
    throws (
      1: BadRequestError badRequestError,
      2: InternalServiceError internalServiceError,
    )

  /**
  * PollForActivityTask is called by application worker to process ActivityTask from a specific taskList.  ActivityTask
  * is dispatched to callers whenever a ScheduleTask decision is made for a workflow execution.
  * Application is expected to call 'RespondActivityTaskCompleted' or 'RespondActivityTaskFailed' once it is done
  * processing the task.
  * Application also needs to call 'RecordActivityTaskHeartbeat' API within 'heartbeatTimeoutSeconds' interval to
  * prevent the task from getting timed out.  An event 'ActivityTaskStarted' event is also written to workflow execution
  * history before the ActivityTask is dispatched to application worker.
  **/
  PollForActivityTaskResponse PollForActivityTask(1: PollForActivityTaskRequest pollRequest)
    throws (
      1: BadRequestError badRequestError,
      2: InternalServiceError internalServiceError,
    )

  /**
  * RecordActivityTaskHeartbeat is called by application worker while it is processing an ActivityTask.  If worker fails
  * to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
  * 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeat' will
  * fail with 'EntityNotExistsError' in such situations.  Use the 'taskToken' provided as response of
  * PollForActivityTask API call for heartbeating.
  **/
  RecordActivityTaskHeartbeatResponse RecordActivityTaskHeartbeat(1: RecordActivityTaskHeartbeatRequest heartbeatRequest)
    throws (
      1: BadRequestError badRequestError,
      2: InternalServiceError internalServiceError,
      3: EntityNotExistsError entityNotExistError,
    )

  /**
  * RespondActivityTaskCompleted is called by application worker when it is done processing an ActivityTask.  It will
  * result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new DecisionTask
  * created for the workflow so new decisions could be made.  Use the 'taskToken' provided as response of
  * PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
  * anymore due to activity timeout.
  **/
  void  RespondActivityTaskCompleted(1: RespondActivityTaskCompletedRequest completeRequest)
    throws (
      1: BadRequestError badRequestError,
      2: InternalServiceError internalServiceError,
      3: EntityNotExistsError entityNotExistError,
    )

  /**
  * RespondActivityTaskFailed is called by application worker when it is done processing an ActivityTask.  It will
  * result in a new 'ActivityTaskFailed' event being written to the workflow history and a new DecisionTask
  * created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
  * PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
  * anymore due to activity timeout.
  **/
  void  RespondActivityTaskFailed(1: RespondActivityTaskFailedRequest failRequest)
    throws (
      1: BadRequestError badRequestError,
      2: InternalServiceError internalServiceError,
      3: EntityNotExistsError entityNotExistError,
    )
}
