// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

include "shared.thrift"

namespace java com.uber.cadence.history

exception EventAlreadyStartedError {
  1: required string message
}

exception ShardOwnershipLostError {
  10: optional string message
  20: optional string owner
}

struct ParentExecutionInfo {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution execution
  30: optional i64 (js.type = "Long") initiatedId
}

struct StartWorkflowExecutionRequest {
  10: optional string domainUUID
  20: optional shared.StartWorkflowExecutionRequest startRequest
  30: optional ParentExecutionInfo parentExecutionInfo
}

struct GetWorkflowExecutionHistoryRequest {
  10: optional string domainUUID
  20: optional shared.GetWorkflowExecutionHistoryRequest getRequest
}

struct RespondDecisionTaskCompletedRequest {
  10: optional string domainUUID
  20: optional shared.RespondDecisionTaskCompletedRequest completeRequest
}

struct RecordActivityTaskHeartbeatRequest {
  10: optional string domainUUID
  20: optional shared.RecordActivityTaskHeartbeatRequest heartbeatRequest
}

struct RespondActivityTaskCompletedRequest {
  10: optional string domainUUID
  20: optional shared.RespondActivityTaskCompletedRequest completeRequest
}

struct RespondActivityTaskFailedRequest {
  10: optional string domainUUID
  20: optional shared.RespondActivityTaskFailedRequest failedRequest
}

struct RespondActivityTaskCanceledRequest {
  10: optional string domainUUID
  20: optional shared.RespondActivityTaskCanceledRequest cancelRequest
}

struct RecordActivityTaskStartedRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution workflowExecution
  30: optional i64 (js.type = "Long") scheduleId
  40: optional i64 (js.type = "Long") taskId
  45: optional string requestId // Unique id of each poll request. Used to ensure at most once delivery of tasks.
  50: optional shared.PollForActivityTaskRequest pollRequest
}

struct RecordActivityTaskStartedResponse {
  10: optional shared.HistoryEvent startedEvent
  20: optional shared.HistoryEvent scheduledEvent
}

struct RecordDecisionTaskStartedRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution workflowExecution
  30: optional i64 (js.type = "Long") scheduleId
  40: optional i64 (js.type = "Long") taskId
  45: optional string requestId // Unique id of each poll request. Used to ensure at most once delivery of tasks.
  50: optional shared.PollForDecisionTaskRequest pollRequest
}

struct RecordDecisionTaskStartedResponse {
  10: optional shared.WorkflowType workflowType
  20: optional i64 (js.type = "Long") previousStartedEventId
  30: optional i64 (js.type = "Long") startedEventId
}

struct SignalWorkflowExecutionRequest {
  10: optional string domainUUID
  20: optional shared.SignalWorkflowExecutionRequest signalRequest
}

struct TerminateWorkflowExecutionRequest {
  10: optional string domainUUID
  20: optional shared.TerminateWorkflowExecutionRequest terminateRequest
}

struct RequestCancelWorkflowExecutionRequest {
  10: optional string domainUUID
  20: optional shared.RequestCancelWorkflowExecutionRequest cancelRequest
  30: optional i64 (js.type = "Long") externalInitiatedEventId
  40: optional shared.WorkflowExecution externalWorkflowExecution
}

struct ScheduleDecisionTaskRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution workflowExecution
}

/**
* RecordChildExecutionCompletedRequest is used for reporting the completion of child execution to parent workflow
* execution which started it.  When a child execution is completed it creates this request and calls the
* RecordChildExecutionCompleted API with the workflowExecution of parent.  It also sets the completedExecution of the
* child as it could potentially be different than the ChildExecutionStartedEvent of parent in the situation when
* child creates multiple runs through ContinueAsNew before finally completing.
**/
struct RecordChildExecutionCompletedRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution workflowExecution
  30: optional i64 (js.type = "Long") initiatedId
  40: optional shared.WorkflowExecution completedExecution
  50: optional shared.HistoryEvent completionEvent
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
  shared.StartWorkflowExecutionResponse StartWorkflowExecution(1: StartWorkflowExecutionRequest startRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.WorkflowExecutionAlreadyStartedError sessionAlreadyExistError,
      4: ShardOwnershipLostError shardOwnershipLostError,
    )

  /**
  * Returns the history of specified workflow execution.  It fails with 'EntityNotExistError' if speficied workflow
  * execution in unknown to the service.
  **/
  shared.GetWorkflowExecutionHistoryResponse GetWorkflowExecutionHistory(1: GetWorkflowExecutionHistoryRequest getRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
      4: ShardOwnershipLostError shardOwnershipLostError,
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
      4: shared.EntityNotExistsError entityNotExistError,
      5: ShardOwnershipLostError shardOwnershipLostError,
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
      4: shared.EntityNotExistsError entityNotExistError,
      5: ShardOwnershipLostError shardOwnershipLostError,
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
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
      4: ShardOwnershipLostError shardOwnershipLostError,
    )

  /**
  * RecordActivityTaskHeartbeat is called by application worker while it is processing an ActivityTask.  If worker fails
  * to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
  * 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeat' will
  * fail with 'EntityNotExistsError' in such situations.  Use the 'taskToken' provided as response of
  * PollForActivityTask API call for heartbeating.
  **/
  shared.RecordActivityTaskHeartbeatResponse RecordActivityTaskHeartbeat(1: RecordActivityTaskHeartbeatRequest heartbeatRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
      4: ShardOwnershipLostError shardOwnershipLostError,
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
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
      4: ShardOwnershipLostError shardOwnershipLostError,
    )

  /**
  * RespondActivityTaskFailed is called by application worker when it is done processing an ActivityTask.  It will
  * result in a new 'ActivityTaskFailed' event being written to the workflow history and a new DecisionTask
  * created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
  * PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
  * anymore due to activity timeout.
  **/
  void RespondActivityTaskFailed(1: RespondActivityTaskFailedRequest failRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
      4: ShardOwnershipLostError shardOwnershipLostError,
    )

  /**
  * RespondActivityTaskCanceled is called by application worker when it is successfully canceled an ActivityTask.  It will
  * result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new DecisionTask
  * created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
  * PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
  * anymore due to activity timeout.
  **/
  void RespondActivityTaskCanceled(1: RespondActivityTaskCanceledRequest canceledRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
      4: ShardOwnershipLostError shardOwnershipLostError,
    )

  /**
  * SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
  * WorkflowExecutionSignaled event recorded in the history and a decision task being created for the execution.
  **/
  void SignalWorkflowExecution(1: SignalWorkflowExecutionRequest signalRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
      4: ShardOwnershipLostError shardOwnershipLostError,
    )

  /**
  * TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
  * in the history and immediately terminating the execution instance.
  **/
  void TerminateWorkflowExecution(1: TerminateWorkflowExecutionRequest terminateRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
      4: ShardOwnershipLostError shardOwnershipLostError,
    )

  /**
  * RequestCancelWorkflowExecution is called by application worker when it wants to request cancellation of a workflow instance.
  * It will result in a new 'WorkflowExecutionCancelRequested' event being written to the workflow history and a new DecisionTask
  * created for the workflow instance so new decisions could be made. It fails with 'EntityNotExistsError' if the workflow is not valid
  * anymore due to completion or doesn't exist.
  **/
  void RequestCancelWorkflowExecution(1: RequestCancelWorkflowExecutionRequest cancelRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
      4: ShardOwnershipLostError shardOwnershipLostError,
    )

  /**
  * ScheduleDecisionTask is used for creating a decision task for already started workflow execution.  This is mainly
  * used by transfer queue processor during the processing of StartChildWorkflowExecution task, where it first starts
  * child execution without creating the decision task and then calls this API after updating the mutable state of
  * parent execution.
  **/
  void ScheduleDecisionTask(1: ScheduleDecisionTaskRequest scheduleRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
      4: ShardOwnershipLostError shardOwnershipLostError,
    )

  /**
  * RecordChildExecutionCompleted is used for reporting the completion of child workflow execution to parent.
  * This is mainly called by transfer queue processor during the processing of DeleteExecution task.
  **/
  void RecordChildExecutionCompleted(1: RecordChildExecutionCompletedRequest completionRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
      4: ShardOwnershipLostError shardOwnershipLostError,
    )
}
