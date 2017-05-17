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
  20: optional shared.PollForDecisionTaskRequest pollRequest
}

struct PollForDecisionTaskResponse {
  10: optional binary taskToken
  20: optional shared.WorkflowExecution workflowExecution
  30: optional shared.WorkflowType workflowType
  40: optional i64 (js.type = "Long") previousStartedEventId
  50: optional i64 (js.type = "Long") startedEventId
}

struct PollForActivityTaskRequest {
  10: optional string domainUUID
  20: optional shared.PollForActivityTaskRequest pollRequest
}

struct AddDecisionTaskRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution execution
  30: optional shared.TaskList taskList
  40: optional i64 (js.type = "Long") scheduleId
}

struct AddActivityTaskRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution execution
  30: optional string sourceDomainUUID
  40: optional shared.TaskList taskList
  50: optional i64 (js.type = "Long") scheduleId
  60: optional i32 scheduleToStartTimeoutSeconds
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
}