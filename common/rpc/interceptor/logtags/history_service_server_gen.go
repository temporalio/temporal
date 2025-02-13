// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

// Code generated by cmd/tools/genrpcserverinterceptors. DO NOT EDIT.

package logtags

import (
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log/tag"
)

func (wt *WorkflowTags) extractFromHistoryServiceServerMessage(message any) []tag.Tag {
	switch r := message.(type) {
	case *historyservice.AddTasksRequest:
		return nil
	case *historyservice.AddTasksResponse:
		return nil
	case *historyservice.CloseShardRequest:
		return nil
	case *historyservice.CloseShardResponse:
		return nil
	case *historyservice.CompleteNexusOperationRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetCompletion().GetWorkflowId()),
			tag.WorkflowRunID(r.GetCompletion().GetRunId()),
		}
	case *historyservice.CompleteNexusOperationResponse:
		return nil
	case *historyservice.CreateWorkflowRuleRequest:
		return nil
	case *historyservice.CreateWorkflowRuleResponse:
		return nil
	case *historyservice.DeepHealthCheckRequest:
		return nil
	case *historyservice.DeepHealthCheckResponse:
		return nil
	case *historyservice.DeleteDLQTasksRequest:
		return nil
	case *historyservice.DeleteDLQTasksResponse:
		return nil
	case *historyservice.DeleteWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.DeleteWorkflowExecutionResponse:
		return nil
	case *historyservice.DeleteWorkflowRuleRequest:
		return nil
	case *historyservice.DeleteWorkflowRuleResponse:
		return nil
	case *historyservice.DeleteWorkflowVisibilityRecordRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.DeleteWorkflowVisibilityRecordResponse:
		return nil
	case *historyservice.DescribeHistoryHostRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.DescribeHistoryHostResponse:
		return nil
	case *historyservice.DescribeMutableStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.DescribeMutableStateResponse:
		return nil
	case *historyservice.DescribeWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.DescribeWorkflowExecutionResponse:
		return nil
	case *historyservice.DescribeWorkflowRuleRequest:
		return nil
	case *historyservice.DescribeWorkflowRuleResponse:
		return nil
	case *historyservice.ExecuteMultiOperationRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
		}
	case *historyservice.ExecuteMultiOperationResponse:
		return nil
	case *historyservice.ForceDeleteWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.ForceDeleteWorkflowExecutionResponse:
		return nil
	case *historyservice.GenerateLastHistoryReplicationTasksRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.GenerateLastHistoryReplicationTasksResponse:
		return nil
	case *historyservice.GetDLQMessagesRequest:
		return nil
	case *historyservice.GetDLQMessagesResponse:
		return nil
	case *historyservice.GetDLQReplicationMessagesRequest:
		return nil
	case *historyservice.GetDLQReplicationMessagesResponse:
		return nil
	case *historyservice.GetDLQTasksRequest:
		return nil
	case *historyservice.GetDLQTasksResponse:
		return nil
	case *historyservice.GetMutableStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.GetMutableStateResponse:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.GetReplicationMessagesRequest:
		return nil
	case *historyservice.GetReplicationMessagesResponse:
		return nil
	case *historyservice.GetReplicationStatusRequest:
		return nil
	case *historyservice.GetReplicationStatusResponse:
		return nil
	case *historyservice.GetShardRequest:
		return nil
	case *historyservice.GetShardResponse:
		return nil
	case *historyservice.GetWorkflowExecutionHistoryRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.GetWorkflowExecutionHistoryResponseWithRaw:
		return nil
	case *historyservice.GetWorkflowExecutionHistoryReverseRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.GetWorkflowExecutionHistoryReverseResponse:
		return nil
	case *historyservice.GetWorkflowExecutionRawHistoryRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.GetWorkflowExecutionRawHistoryResponse:
		return nil
	case *historyservice.GetWorkflowExecutionRawHistoryV2Request:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.GetWorkflowExecutionRawHistoryV2Response:
		return nil
	case *historyservice.ImportWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.ImportWorkflowExecutionResponse:
		return nil
	case *historyservice.InvokeStateMachineMethodRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *historyservice.InvokeStateMachineMethodResponse:
		return nil
	case *historyservice.IsActivityTaskValidRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.IsActivityTaskValidResponse:
		return nil
	case *historyservice.IsWorkflowTaskValidRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.IsWorkflowTaskValidResponse:
		return nil
	case *historyservice.ListQueuesRequest:
		return nil
	case *historyservice.ListQueuesResponse:
		return nil
	case *historyservice.ListTasksRequest:
		return nil
	case *historyservice.ListTasksResponse:
		return nil
	case *historyservice.ListWorkflowRulesRequest:
		return nil
	case *historyservice.ListWorkflowRulesResponse:
		return nil
	case *historyservice.MergeDLQMessagesRequest:
		return nil
	case *historyservice.MergeDLQMessagesResponse:
		return nil
	case *historyservice.PauseActivityRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetFrontendRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetFrontendRequest().GetExecution().GetRunId()),
		}
	case *historyservice.PauseActivityResponse:
		return nil
	case *historyservice.PollMutableStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.PollMutableStateResponse:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.PollWorkflowExecutionUpdateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetUpdateRef().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetUpdateRef().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.PollWorkflowExecutionUpdateResponse:
		return nil
	case *historyservice.PurgeDLQMessagesRequest:
		return nil
	case *historyservice.PurgeDLQMessagesResponse:
		return nil
	case *historyservice.QueryWorkflowRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.QueryWorkflowResponse:
		return nil
	case *historyservice.ReapplyEventsRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.ReapplyEventsResponse:
		return nil
	case *historyservice.RebuildMutableStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.RebuildMutableStateResponse:
		return nil
	case *historyservice.RecordActivityTaskHeartbeatRequest:
		return wt.fromTaskToken(r.GetHeartbeatRequest().GetTaskToken())
	case *historyservice.RecordActivityTaskHeartbeatResponse:
		return nil
	case *historyservice.RecordActivityTaskStartedRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.RecordActivityTaskStartedResponse:
		return nil
	case *historyservice.RecordChildExecutionCompletedRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetParentExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetParentExecution().GetRunId()),
		}
	case *historyservice.RecordChildExecutionCompletedResponse:
		return nil
	case *historyservice.RecordWorkflowTaskStartedRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.RecordWorkflowTaskStartedResponseWithRawHistory:
		return nil
	case *historyservice.RefreshWorkflowTasksRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.RefreshWorkflowTasksResponse:
		return nil
	case *historyservice.RemoveSignalMutableStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.RemoveSignalMutableStateResponse:
		return nil
	case *historyservice.RemoveTaskRequest:
		return nil
	case *historyservice.RemoveTaskResponse:
		return nil
	case *historyservice.ReplicateEventsV2Request:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.ReplicateEventsV2Response:
		return nil
	case *historyservice.ReplicateWorkflowStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowState().GetExecutionInfo().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowState().GetExecutionState().GetRunId()),
		}
	case *historyservice.ReplicateWorkflowStateResponse:
		return nil
	case *historyservice.RequestCancelWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetCancelRequest().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetCancelRequest().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.RequestCancelWorkflowExecutionResponse:
		return nil
	case *historyservice.ResetActivityRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetFrontendRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetFrontendRequest().GetExecution().GetRunId()),
		}
	case *historyservice.ResetActivityResponse:
		return nil
	case *historyservice.ResetStickyTaskQueueRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.ResetStickyTaskQueueResponse:
		return nil
	case *historyservice.ResetWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetResetRequest().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetResetRequest().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.ResetWorkflowExecutionResponse:
		return []tag.Tag{
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *historyservice.RespondActivityTaskCanceledRequest:
		return wt.fromTaskToken(r.GetCancelRequest().GetTaskToken())
	case *historyservice.RespondActivityTaskCanceledResponse:
		return nil
	case *historyservice.RespondActivityTaskCompletedRequest:
		return wt.fromTaskToken(r.GetCompleteRequest().GetTaskToken())
	case *historyservice.RespondActivityTaskCompletedResponse:
		return nil
	case *historyservice.RespondActivityTaskFailedRequest:
		return wt.fromTaskToken(r.GetFailedRequest().GetTaskToken())
	case *historyservice.RespondActivityTaskFailedResponse:
		return nil
	case *historyservice.RespondWorkflowTaskCompletedRequest:
		return wt.fromTaskToken(r.GetCompleteRequest().GetTaskToken())
	case *historyservice.RespondWorkflowTaskCompletedResponse:
		return nil
	case *historyservice.RespondWorkflowTaskFailedRequest:
		return wt.fromTaskToken(r.GetFailedRequest().GetTaskToken())
	case *historyservice.RespondWorkflowTaskFailedResponse:
		return nil
	case *historyservice.ScheduleWorkflowTaskRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.ScheduleWorkflowTaskResponse:
		return nil
	case *historyservice.SignalWithStartWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetSignalWithStartRequest().GetWorkflowId()),
		}
	case *historyservice.SignalWithStartWorkflowExecutionResponse:
		return []tag.Tag{
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *historyservice.SignalWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetSignalRequest().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetSignalRequest().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.SignalWorkflowExecutionResponse:
		return nil
	case *historyservice.StartWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetStartRequest().GetWorkflowId()),
		}
	case *historyservice.StartWorkflowExecutionResponse:
		return []tag.Tag{
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *historyservice.SyncActivityRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *historyservice.SyncActivityResponse:
		return nil
	case *historyservice.SyncShardStatusRequest:
		return nil
	case *historyservice.SyncShardStatusResponse:
		return nil
	case *historyservice.SyncWorkflowStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.SyncWorkflowStateResponse:
		return nil
	case *historyservice.TerminateWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetTerminateRequest().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetTerminateRequest().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.TerminateWorkflowExecutionResponse:
		return nil
	case *historyservice.UnpauseActivityRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetFrontendRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetFrontendRequest().GetExecution().GetRunId()),
		}
	case *historyservice.UnpauseActivityResponse:
		return nil
	case *historyservice.UpdateActivityOptionsRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetUpdateRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetUpdateRequest().GetExecution().GetRunId()),
		}
	case *historyservice.UpdateActivityOptionsResponse:
		return nil
	case *historyservice.UpdateWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.UpdateWorkflowExecutionResponse:
		return nil
	case *historyservice.UpdateWorkflowExecutionOptionsRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetUpdateRequest().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetUpdateRequest().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.UpdateWorkflowExecutionOptionsResponse:
		return nil
	case *historyservice.VerifyChildExecutionCompletionRecordedRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetParentExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetParentExecution().GetRunId()),
		}
	case *historyservice.VerifyChildExecutionCompletionRecordedResponse:
		return nil
	case *historyservice.VerifyFirstWorkflowTaskScheduledRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.VerifyFirstWorkflowTaskScheduledResponse:
		return nil
	default:
		return nil
	}
}
