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

namespace java com.temporalio.temporal.matching

struct PollForDecisionTaskRequest {
  10: optional string domainUUID
  15: optional string pollerID
  20: optional shared.PollForDecisionTaskRequest pollRequest
  30: optional string forwardedFrom
}

struct PollForDecisionTaskResponse {
  10: optional binary taskToken
  20: optional shared.WorkflowExecution workflowExecution
  30: optional shared.WorkflowType workflowType
  40: optional i64 (js.type = "Long") previousStartedEventId
  50: optional i64 (js.type = "Long") startedEventId
  51: optional i64 (js.type = "Long") attempt
  60: optional i64 (js.type = "Long") nextEventId
  65: optional i64 (js.type = "Long") backlogCountHint
  70: optional bool stickyExecutionEnabled
  80: optional shared.WorkflowQuery query
  90: optional shared.TransientDecisionInfo decisionInfo
  100: optional shared.TaskList WorkflowExecutionTaskList
  110: optional i32 eventStoreVersion
  120: optional binary branchToken
  130: optional i64 (js.type = "Long") scheduledTimestamp
  140: optional i64 (js.type = "Long") startedTimestamp
  150: optional map<string, shared.WorkflowQuery> queries
}

struct PollForActivityTaskRequest {
  10: optional string domainUUID
  15: optional string pollerID
  20: optional shared.PollForActivityTaskRequest pollRequest
  30: optional string forwardedFrom
}

struct AddDecisionTaskRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution execution
  30: optional shared.TaskList taskList
  40: optional i64 (js.type = "Long") scheduleId
  50: optional i32 scheduleToStartTimeoutSeconds
  60: optional string forwardedFrom
}

struct AddActivityTaskRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution execution
  30: optional string sourceDomainUUID
  40: optional shared.TaskList taskList
  50: optional i64 (js.type = "Long") scheduleId
  60: optional i32 scheduleToStartTimeoutSeconds
  70: optional string forwardedFrom
}

struct QueryWorkflowRequest {
  10: optional string domainUUID
  20: optional shared.TaskList taskList
  30: optional shared.QueryWorkflowRequest queryRequest
  40: optional string forwardedFrom
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

struct DescribeTaskListRequest {
  10: optional string domainUUID
  20: optional shared.DescribeTaskListRequest descRequest
}

struct ListTaskListPartitionsRequest {
  10: optional string domain
  20: optional shared.TaskList taskList
}
