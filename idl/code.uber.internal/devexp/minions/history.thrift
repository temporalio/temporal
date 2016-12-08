include "shared.thrift"

namespace java com.uber.devexp.minions

exception EventAlreadyStartedError {
  1: required string message
}

struct RecordActivityTaskStartedRequest {
  10: optional shared.WorkflowExecution workflowExecution
  20: optional i64 (js.type = "Long") scheduleId
  30: optional string taskId
  40: optional shared.PollForActivityTaskRequest pollRequest
}

struct RecordActivityTaskStartedResponse {
  10: optional shared.HistoryEvent startedEvent
  20: optional shared.HistoryEvent scheduledEvent
}

struct RecordDecisionTaskStartedRequest {
  10: optional shared.WorkflowExecution workflowExecution
  20: optional i64 (js.type = "Long") scheduleId
  30: optional string taskId
  40: optional shared.PollForDecisionTaskRequest pollRequest
}

struct RecordDecisionTaskStartedResponse {
  10: optional shared.WorkflowType workflowType
  20: optional i64 (js.type = "Long") previousStartedEventId
  30: optional i64 (js.type = "Long") startedEventId
  40: optional shared.History history
}

/**
* HistoryService provides API to start a new long running workflow instance, as well as query and update the history
* of workflow instances already created.
**/
service HistoryService {
  /**
  * StartWorkflowExecution starts a new long running workflow instance.  It will create the instance with
  * 'WorkflowExecutionStarted' event in history and also schedule the first DecisionTask for the worker to make the
  * first decision for this instance.  It will return 'WorkflowExecutionAlreadyStartedError', if an instance already
  * exists with same workflowId.
  **/
  shared.StartWorkflowExecutionResponse StartWorkflowExecution(1: shared.StartWorkflowExecutionRequest startRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.WorkflowExecutionAlreadyStartedError sessionAlreadyExistError,
    )

  /**
  * RecordDecisionTaskStarted is called by the Matchingservice before it hands a decision task to the application worker in response to
  * a PollForDecisionTask call. It records in the history the event that the decision task has started. It will return 'EventAlreadyStartedError',
  * if the workflow's execution history already includes a record of the event starting.
  **/
  RecordDecisionTaskStartedResponse RecordDecisionTaskStarted(1: RecordDecisionTaskStartedRequest addRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: EventAlreadyStartedError eventAlreadyStartedError,
    )

  /**
  * RecordActivityTaskStarted is called by the Matchingservice before it hands a decision task to the application worker in response to
  * a PollForActivityTask call. It records in the history the event that the decision task has started. It will return 'EventAlreadyStartedError',
  * if the workflow's execution history already includes a record of the event starting.
  **/
  RecordActivityTaskStartedResponse RecordActivityTaskStarted(1: RecordActivityTaskStartedRequest addRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: EventAlreadyStartedError eventAlreadyStartedError,
    )

  /**
  * RespondDecisionTaskCompleted is called by application worker to complete a DecisionTask handed as a result of
  * 'PollForDecisionTask' API call.  Completing a DecisionTask will result in new events for the workflow execution and
  * potentially new ActivityTask being created for corresponding decisions.  It will also create a DecisionTaskCompleted
  * event in the history for that session.  Use the 'taskToken' provided as response of PollForDecisionTask API call
  * for completing the DecisionTask.
  **/
  void RespondDecisionTaskCompleted(1: shared.RespondDecisionTaskCompletedRequest completeRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
    )

  /**
  * RecordActivityTaskHeartbeat is called by application worker while it is processing an ActivityTask.  If worker fails
  * to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
  * 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeat' will
  * fail with 'EntityNotExistsError' in such situations.  Use the 'taskToken' provided as response of
  * PollForActivityTask API call for heartbeating.
  **/
  shared.RecordActivityTaskHeartbeatResponse RecordActivityTaskHeartbeat(1: shared.RecordActivityTaskHeartbeatRequest heartbeatRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
    )

  /**
  * RespondActivityTaskCompleted is called by application worker when it is done processing an ActivityTask.  It will
  * result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new DecisionTask
  * created for the workflow so new decisions could be made.  Use the 'taskToken' provided as response of
  * PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
  * anymore due to activity timeout.
  **/
  void  RespondActivityTaskCompleted(1: shared.RespondActivityTaskCompletedRequest completeRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
    )

  /**
  * RespondActivityTaskFailed is called by application worker when it is done processing an ActivityTask.  It will
  * result in a new 'ActivityTaskFailed' event being written to the workflow history and a new DecisionTask
  * created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
  * PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
  * anymore due to activity timeout.
  **/
  void  RespondActivityTaskFailed(1: shared.RespondActivityTaskFailedRequest failRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
    )
}
