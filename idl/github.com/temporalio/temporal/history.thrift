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
include "replicator.thrift"

namespace java com.temporalio.temporal.history

struct ParentExecutionInfo {
  10: optional string domainUUID
  15: optional string domain
  20: optional shared.WorkflowExecution execution
  30: optional i64 (js.type = "Long") initiatedId
}

struct StartWorkflowExecutionRequest {
  10: optional string domainUUID
  20: optional shared.StartWorkflowExecutionRequest startRequest
  30: optional ParentExecutionInfo parentExecutionInfo
  40: optional i32 attempt
  50: optional i64 (js.type = "Long") expirationTimestamp
  55: optional shared.ContinueAsNewInitiator continueAsNewInitiator
  56: optional string continuedFailureReason
  57: optional binary continuedFailureDetails
  58: optional binary lastCompletionResult
  60: optional i32 firstDecisionTaskBackoffSeconds
}

struct DescribeMutableStateRequest{
  10: optional string domainUUID
  20: optional shared.WorkflowExecution execution
}

struct DescribeMutableStateResponse{
  30: optional string mutableStateInCache
  40: optional string mutableStateInDatabase
}

struct GetMutableStateRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution execution
  30: optional i64 (js.type = "Long") expectedNextEventId
  40: optional binary currentBranchToken
}

struct GetMutableStateResponse {
  10: optional shared.WorkflowExecution execution
  20: optional shared.WorkflowType workflowType
  30: optional i64 (js.type = "Long") NextEventId
  35: optional i64 (js.type = "Long") PreviousStartedEventId
  40: optional i64 (js.type = "Long") LastFirstEventId
  50: optional shared.TaskList taskList
  60: optional shared.TaskList stickyTaskList
  70: optional string clientLibraryVersion
  80: optional string clientFeatureVersion
  90: optional string clientImpl
  //TODO: isWorkflowRunning is deprecating. workflowState is going replace this field
  100: optional bool isWorkflowRunning
  110: optional i32 stickyTaskListScheduleToStartTimeout
  120: optional i32 eventStoreVersion
  130: optional binary currentBranchToken
  140: optional map<string, shared.ReplicationInfo> replicationInfo
  // TODO: when migrating to gRPC, make this a enum
  // TODO: when migrating to gRPC, unify internal & external representation
  // NOTE: workflowState & workflowCloseState are the same as persistence representation
  150: optional i32 workflowState
  160: optional i32 workflowCloseState
  170: optional shared.VersionHistories versionHistories
  180: optional bool isStickyTaskListEnabled
}

struct PollMutableStateRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution execution
  30: optional i64 (js.type = "Long") expectedNextEventId
  40: optional binary currentBranchToken
}

struct PollMutableStateResponse {
  10: optional shared.WorkflowExecution execution
  20: optional shared.WorkflowType workflowType
  30: optional i64 (js.type = "Long") NextEventId
  35: optional i64 (js.type = "Long") PreviousStartedEventId
  40: optional i64 (js.type = "Long") LastFirstEventId
  50: optional shared.TaskList taskList
  60: optional shared.TaskList stickyTaskList
  70: optional string clientLibraryVersion
  80: optional string clientFeatureVersion
  90: optional string clientImpl
  100: optional i32 stickyTaskListScheduleToStartTimeout
  110: optional binary currentBranchToken
  120: optional map<string, shared.ReplicationInfo> replicationInfo
  130: optional shared.VersionHistories versionHistories
  // TODO: when migrating to gRPC, make this a enum
  // TODO: when migrating to gRPC, unify internal & external representation
  // NOTE: workflowState & workflowCloseState are the same as persistence representation
  140: optional i32 workflowState
  150: optional i32 workflowCloseState
}

struct ResetStickyTaskListRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution execution
}

struct ResetStickyTaskListResponse {
  // The reason to keep this response is to allow returning
  // information in the future.
}

struct RespondDecisionTaskCompletedRequest {
  10: optional string domainUUID
  20: optional shared.RespondDecisionTaskCompletedRequest completeRequest
}

struct RespondDecisionTaskCompletedResponse {
  10: optional RecordDecisionTaskStartedResponse startedResponse
}

struct RespondDecisionTaskFailedRequest {
  10: optional string domainUUID
  20: optional shared.RespondDecisionTaskFailedRequest failedRequest
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

struct RefreshWorkflowTasksRequest {
  10: optional string domainUIID
  20: optional shared.RefreshWorkflowTasksRequest request
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
  20: optional shared.HistoryEvent scheduledEvent
  30: optional i64 (js.type = "Long") startedTimestamp
  40: optional i64 (js.type = "Long") attempt
  50: optional i64 (js.type = "Long") scheduledTimestampOfThisAttempt
  60: optional binary heartbeatDetails
  70: optional shared.WorkflowType workflowType
  80: optional string workflowDomain
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
  30: optional i64 (js.type = "Long") scheduledEventId
  40: optional i64 (js.type = "Long") startedEventId
  50: optional i64 (js.type = "Long") nextEventId
  60: optional i64 (js.type = "Long") attempt
  70: optional bool stickyExecutionEnabled
  80: optional shared.TransientDecisionInfo decisionInfo
  90: optional shared.TaskList WorkflowExecutionTaskList
  100: optional i32 eventStoreVersion
  110: optional binary branchToken
  120: optional i64 (js.type = "Long") scheduledTimestamp
  130: optional i64 (js.type = "Long") startedTimestamp
  140: optional map<string, shared.WorkflowQuery> queries
}

struct SignalWorkflowExecutionRequest {
  10: optional string domainUUID
  20: optional shared.SignalWorkflowExecutionRequest signalRequest
  30: optional shared.WorkflowExecution externalWorkflowExecution
  40: optional bool childWorkflowOnly
}

struct SignalWithStartWorkflowExecutionRequest {
  10: optional string domainUUID
  20: optional shared.SignalWithStartWorkflowExecutionRequest signalWithStartRequest
}

struct RemoveSignalMutableStateRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution workflowExecution
  30: optional string requestId
}

struct TerminateWorkflowExecutionRequest {
  10: optional string domainUUID
  20: optional shared.TerminateWorkflowExecutionRequest terminateRequest
}

struct ResetWorkflowExecutionRequest {
  10: optional string domainUUID
  20: optional shared.ResetWorkflowExecutionRequest resetRequest
}

struct RequestCancelWorkflowExecutionRequest {
  10: optional string domainUUID
  20: optional shared.RequestCancelWorkflowExecutionRequest cancelRequest
  30: optional i64 (js.type = "Long") externalInitiatedEventId
  40: optional shared.WorkflowExecution externalWorkflowExecution
  50: optional bool childWorkflowOnly
}

struct ScheduleDecisionTaskRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution workflowExecution
  30: optional bool isFirstDecision
}

struct DescribeWorkflowExecutionRequest {
  10: optional string domainUUID
  20: optional shared.DescribeWorkflowExecutionRequest request
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

struct ReplicateEventsRequest {
  10: optional string sourceCluster
  20: optional string domainUUID
  30: optional shared.WorkflowExecution workflowExecution
  40: optional i64 (js.type = "Long") firstEventId
  50: optional i64 (js.type = "Long") nextEventId
  60: optional i64 (js.type = "Long") version
  70: optional map<string, shared.ReplicationInfo> replicationInfo
  80: optional shared.History history
  90: optional shared.History newRunHistory
  100: optional bool forceBufferEvents // this attribute is deprecated
  110: optional i32 eventStoreVersion
  120: optional i32 newRunEventStoreVersion
  130: optional bool resetWorkflow
  140: optional bool newRunNDC
}

struct ReplicateRawEventsRequest {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution workflowExecution
  30: optional map<string, shared.ReplicationInfo> replicationInfo
  40: optional shared.DataBlob history
  50: optional shared.DataBlob newRunHistory
  60: optional i32 eventStoreVersion
  70: optional i32 newRunEventStoreVersion
}

struct ReplicateEventsV2Request {
  10: optional string domainUUID
  20: optional shared.WorkflowExecution workflowExecution
  30: optional list<shared.VersionHistoryItem> versionHistoryItems
  40: optional shared.DataBlob events
  // new run events does not need version history since there is no prior events
  60: optional shared.DataBlob newRunEvents
}

struct SyncShardStatusRequest {
  10: optional string sourceCluster
  20: optional i64 (js.type = "Long") shardId
  30: optional i64 (js.type = "Long") timestamp
}

struct SyncActivityRequest {
  10: optional string domainId
  20: optional string workflowId
  30: optional string runId
  40: optional i64 (js.type = "Long") version
  50: optional i64 (js.type = "Long") scheduledId
  60: optional i64 (js.type = "Long") scheduledTime
  70: optional i64 (js.type = "Long") startedId
  80: optional i64 (js.type = "Long") startedTime
  90: optional i64 (js.type = "Long") lastHeartbeatTime
  100: optional binary details
  110: optional i32 attempt
  120: optional string lastFailureReason
  130: optional string lastWorkerIdentity
  140: optional binary lastFailureDetails
  150: optional shared.VersionHistory versionHistory
}

struct QueryWorkflowRequest {
  10: optional string domainUUID
  20: optional shared.QueryWorkflowRequest request
}

struct QueryWorkflowResponse {
  10: optional shared.QueryWorkflowResponse response
}

struct ReapplyEventsRequest {
  10: optional string domainUUID
  20: optional shared.ReapplyEventsRequest request
}

