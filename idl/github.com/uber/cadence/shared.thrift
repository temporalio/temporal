namespace java com.uber.cadence

exception BadRequestError {
  1: required string message
}

exception InternalServiceError {
  1: required string message
}

exception WorkflowExecutionAlreadyStartedError {
  10: optional string message
  20: optional string startRequestId
  30: optional string runId
}

exception EntityNotExistsError {
  1: required string message
}

exception ServiceBusyError {
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
  RequestCancelActivityTask,
  StartTimer,
  CompleteWorkflowExecution,
  FailWorkflowExecution,
  CancelTimer,
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
  ActivityTaskCancelRequested,
  RequestCancelActivityTaskFailed,
  ActivityTaskCanceled,
  TimerStarted,
  TimerFired,
  CompleteWorkflowExecutionFailed,
  CancelTimerFailed,
  TimerCanceled,
}

enum WorkflowCompleteFailedCause {
  UNHANDLED_DECISION,
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

struct RequestCancelActivityTaskDecisionAttributes {
  10: optional string activityId
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

struct CancelTimerDecisionAttributes {
  10: optional string timerId
}

struct Decision {
  10: optional DecisionType decisionType
  20: optional ScheduleActivityTaskDecisionAttributes scheduleActivityTaskDecisionAttributes
  25: optional StartTimerDecisionAttributes startTimerDecisionAttributes
  30: optional CompleteWorkflowExecutionDecisionAttributes completeWorkflowExecutionDecisionAttributes
  35: optional FailWorkflowExecutionDecisionAttributes failWorkflowExecutionDecisionAttributes
  40: optional RequestCancelActivityTaskDecisionAttributes requestCancelActivityTaskDecisionAttributes
  50: optional CancelTimerDecisionAttributes cancelTimerDecisionAttributes
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

struct CompleteWorkflowExecutionFailedEventAttributes {
  10: optional WorkflowCompleteFailedCause cause
  20: optional i64 (js.type = "Long") decisionTaskCompletedEventId
}

struct DecisionTaskScheduledEventAttributes {
  10: optional TaskList taskList
  20: optional i32 startToCloseTimeoutSeconds
}

struct DecisionTaskStartedEventAttributes {
  10: optional i64 (js.type = "Long") scheduledEventId
  20: optional string identity
  30: optional string requestId
}

struct DecisionTaskCompletedEventAttributes {
  10: optional binary executionContext
  20: optional i64 (js.type = "Long") scheduledEventId
  30: optional i64 (js.type = "Long") startedEventId
  40: optional string identity
}

struct DecisionTaskTimedOutEventAttributes {
  10: optional i64 (js.type = "Long") scheduledEventId
  20: optional i64 (js.type = "Long") startedEventId
  30: optional TimeoutType timeoutType
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
  30: optional string requestId
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
  05: optional binary details
  10: optional i64 (js.type = "Long") scheduledEventId
  20: optional i64 (js.type = "Long") startedEventId
  30: optional TimeoutType timeoutType
}

struct ActivityTaskCancelRequestedEventAttributes {
  10: optional string activityId
  20: optional i64 (js.type = "Long") decisionTaskCompletedEventId
}

struct RequestCancelActivityTaskFailedEventAttributes{
  10: optional string activityId
  20: optional string cause
  30: optional i64 (js.type = "Long") decisionTaskCompletedEventId
}

struct ActivityTaskCanceledEventAttributes {
  10: optional binary details
  20: optional i64 (js.type = "Long") latestCancelRequestedEventId
  30: optional i64 (js.type = "Long") scheduledEventId
  40: optional i64 (js.type = "Long") startedEventId
  50: optional string identity
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

struct TimerCanceledEventAttributes {
  10: optional string timerId
  20: optional i64 (js.type = "Long") startedEventId
  30: optional i64 (js.type = "Long") decisionTaskCompletedEventId
  40: optional string identity
}

struct CancelTimerFailedEventAttributes {
  10: optional string timerId
  20: optional string cause
  30: optional i64 (js.type = "Long") decisionTaskCompletedEventId
  40: optional string identity
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
  63:  optional DecisionTaskTimedOutEventAttributes decisionTaskTimedOutEventAttributes
  65:  optional DecisionTaskCompletedEventAttributes decisionTaskCompletedEventAttributes
  70:  optional ActivityTaskScheduledEventAttributes activityTaskScheduledEventAttributes
  75:  optional ActivityTaskStartedEventAttributes activityTaskStartedEventAttributes
  80:  optional ActivityTaskCompletedEventAttributes activityTaskCompletedEventAttributes
  85:  optional ActivityTaskFailedEventAttributes activityTaskFailedEventAttributes
  90:  optional ActivityTaskTimedOutEventAttributes activityTaskTimedOutEventAttributes
  95:  optional TimerStartedEventAttributes timerStartedEventAttributes
  100: optional TimerFiredEventAttributes timerFiredEventAttributes
  105: optional CompleteWorkflowExecutionFailedEventAttributes completeWorkflowExecutionFailedEventAttributes
  110: optional ActivityTaskCancelRequestedEventAttributes activityTaskCancelRequestedEventAttributes
  120: optional RequestCancelActivityTaskFailedEventAttributes requestCancelActivityTaskFailedEventAttributes
  130: optional ActivityTaskCanceledEventAttributes activityTaskCanceledEventAttributes
  140: optional TimerCanceledEventAttributes timerCanceledEventAttributes
  150: optional CancelTimerFailedEventAttributes cancelTimerFailedEventAttributes
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
  80: optional string requestId
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
  10: optional bool cancelRequested
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

struct RespondActivityTaskCanceledRequest {
  10: optional binary taskToken
  20: optional binary details
  30: optional string identity
}

struct GetWorkflowExecutionHistoryRequest {
  10: optional WorkflowExecution execution
}

struct GetWorkflowExecutionHistoryResponse {
  10: optional History history
}