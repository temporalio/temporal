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

namespace java com.uber.cadence.matching

struct PollForDecisionTaskRequest {
  10: optional string domainUUID
  15: optional string pollerID
  20: optional shared.PollForDecisionTaskRequest pollRequest
}

struct PollForDecisionTaskResponse {
  10: optional binary taskToken
  20: optional shared.WorkflowExecution workflowExecution
  30: optional shared.WorkflowType workflowType
  40: optional i64 (js.type = "Long") previousStartedEventId
  50: optional i64 (js.type = "Long") startedEventId
  55: optional bool stickyExecutionEnabled
  60: optional shared.WorkflowQuery query
}

struct PollForActivityTaskRequest {
  10: optional string domainUUID
  15: optional string pollerID
  20: optional shared.PollForActivityTaskRequest pollRequest
}

struct AddDecisionTaskRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution execution
  30: optional shared.TaskList taskList
  40: optional i64 (js.type = "Long") scheduleId
  50: optional i32 scheduleToStartTimeoutSeconds
}

struct AddActivityTaskRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution execution
  30: optional string sourceDomainUUID
  40: optional shared.TaskList taskList
  50: optional i64 (js.type = "Long") scheduleId
  60: optional i32 scheduleToStartTimeoutSeconds
}

struct QueryWorkflowRequest {
  10: optional string domainUUID
  20: optional shared.TaskList taskList
  30: optional shared.QueryWorkflowRequest queryRequest
}

struct RespondQueryTaskCompletedRequest {
  10: optional string domainUUID
  20: optional shared.TaskList taskList
  30: optional string taskID
  40: optional shared.RespondQueryTaskCompletedRequest completedRequest
}

struct CancelOutstandingPollRequest {
  10: optional string domainUUID
  20: optional i32 taskListType
  30: optional shared.TaskList taskList
  40: optional string pollerID
}

/**
* MatchingService API is exposed to provide support for polling from long running applications.
* Such applications are expected to have a worker which regularly polls for DecisionTask and ActivityTask.  For each
* DecisionTask, application is expected to process the history of events for that session and respond back with next
* decisions.  For each ActivityTask, application is expected to execute the actual logic for that task and respond back
* with completion or failure.
**/
service MatchingService {
  /**
  * PollForDecisionTask is called by frontend to process DecisionTask from a specific taskList.  A
  * DecisionTask is dispatched to callers for active workflow executions, with pending decisions.
  **/
  PollForDecisionTaskResponse PollForDecisionTask(1: PollForDecisionTaskRequest pollRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
    )

  /**
  * PollForActivityTask is called by frontend to process ActivityTask from a specific taskList.  ActivityTask
  * is dispatched to callers whenever a ScheduleTask decision is made for a workflow execution.
  **/
  shared.PollForActivityTaskResponse PollForActivityTask(1: PollForActivityTaskRequest pollRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
    )

  /**
  * AddDecisionTask is called by the history service when a decision task is scheduled, so that it can be dispatched
  * by the MatchingEngine.
  **/
  void AddDecisionTask(1: AddDecisionTaskRequest addRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.ServiceBusyError serviceBusyError,
    )

  /**
  * AddActivityTask is called by the history service when a decision task is scheduled, so that it can be dispatched
  * by the MatchingEngine.
  **/
  void AddActivityTask(1: AddActivityTaskRequest addRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.ServiceBusyError serviceBusyError,
    )

  /**
  * QueryWorkflow is called by frontend to query a workflow.
  **/
  shared.QueryWorkflowResponse QueryWorkflow(1: QueryWorkflowRequest queryRequest)
	throws (
	  1: shared.BadRequestError badRequestError,
	  2: shared.InternalServiceError internalServiceError,
	  3: shared.EntityNotExistsError entityNotExistError,
	  4: shared.QueryFailedError queryFailedError,
	)

  /**
  * RespondQueryTaskCompleted is called by frontend to respond query completed.
  **/
  void RespondQueryTaskCompleted(1: RespondQueryTaskCompletedRequest request)
	throws (
	  1: shared.BadRequestError badRequestError,
	  2: shared.InternalServiceError internalServiceError,
	  3: shared.EntityNotExistsError entityNotExistError,
	)

  /**
    * CancelOutstandingPoll is called by frontend to unblock long polls on matching for zombie pollers.
    * Our rpc stack does not support context propagation, so when a client connection goes away frontend sees
    * cancellation of context for that handler, but any corresponding calls (long-poll) to matching service does not
    * see the cancellation propagated so it can unblock corresponding long-polls on its end.  This results is tasks
    * being dispatched to zombie pollers in this situation.  This API is added so everytime frontend makes a long-poll
    * api call to matching it passes in a pollerID and then calls this API when it detects client connection is closed
    * to unblock long polls for this poller and prevent tasks being sent to these zombie pollers.
    **/
  void CancelOutstandingPoll(1: CancelOutstandingPollRequest request)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
    )

}
