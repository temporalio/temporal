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

func (wt *WorkflowTags) extractFromHistoryServiceServerRequest(req any) []tag.Tag {
	switch r := req.(type) {
	case *historyservice.AddTasksRequest:
		return nil
	case *historyservice.CloseShardRequest:
		return nil
	case *historyservice.CompleteNexusOperationRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetCompletion().GetWorkflowId()),
			tag.WorkflowRunID(r.GetCompletion().GetRunId()),
		}
	case *historyservice.DeepHealthCheckRequest:
		return nil
	case *historyservice.DeleteDLQTasksRequest:
		return nil
	case *historyservice.DeleteWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.DeleteWorkflowVisibilityRecordRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.DescribeHistoryHostRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.DescribeMutableStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.DescribeWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.ExecuteMultiOperationRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
		}
	case *historyservice.ForceDeleteWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.GenerateLastHistoryReplicationTasksRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.GetDLQMessagesRequest:
		return nil
	case *historyservice.GetDLQReplicationMessagesRequest:
		return nil
	case *historyservice.GetDLQTasksRequest:
		return nil
	case *historyservice.GetMutableStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.GetReplicationMessagesRequest:
		return nil
	case *historyservice.GetReplicationStatusRequest:
		return nil
	case *historyservice.GetShardRequest:
		return nil
	case *historyservice.GetWorkflowExecutionHistoryRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.GetWorkflowExecutionHistoryReverseRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.GetWorkflowExecutionRawHistoryRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.GetWorkflowExecutionRawHistoryV2Request:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.ImportWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.InvokeStateMachineMethodRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *historyservice.IsActivityTaskValidRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.IsWorkflowTaskValidRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.ListQueuesRequest:
		return nil
	case *historyservice.ListTasksRequest:
		return nil
	case *historyservice.MergeDLQMessagesRequest:
		return nil
	case *historyservice.PollMutableStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.PollWorkflowExecutionUpdateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetUpdateRef().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetUpdateRef().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.PurgeDLQMessagesRequest:
		return nil
	case *historyservice.QueryWorkflowRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.ReapplyEventsRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.RebuildMutableStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.RecordActivityTaskHeartbeatRequest:
		return wt.fromTaskToken(r.GetHeartbeatRequest().GetTaskToken())
	case *historyservice.RecordActivityTaskStartedRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.RecordChildExecutionCompletedRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetParentExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetParentExecution().GetRunId()),
		}
	case *historyservice.RecordWorkflowTaskStartedRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.RefreshWorkflowTasksRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetExecution().GetRunId()),
		}
	case *historyservice.RemoveSignalMutableStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.RemoveTaskRequest:
		return nil
	case *historyservice.ReplicateEventsV2Request:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.ReplicateWorkflowStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowState().GetExecutionInfo().GetWorkflowId()),
		}
	case *historyservice.RequestCancelWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetCancelRequest().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetCancelRequest().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.ResetStickyTaskQueueRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.ResetWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetResetRequest().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetResetRequest().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.RespondActivityTaskCanceledRequest:
		return wt.fromTaskToken(r.GetCancelRequest().GetTaskToken())
	case *historyservice.RespondActivityTaskCompletedRequest:
		return wt.fromTaskToken(r.GetCompleteRequest().GetTaskToken())
	case *historyservice.RespondActivityTaskFailedRequest:
		return wt.fromTaskToken(r.GetFailedRequest().GetTaskToken())
	case *historyservice.RespondWorkflowTaskCompletedRequest:
		return wt.fromTaskToken(r.GetCompleteRequest().GetTaskToken())
	case *historyservice.RespondWorkflowTaskFailedRequest:
		return wt.fromTaskToken(r.GetFailedRequest().GetTaskToken())
	case *historyservice.ScheduleWorkflowTaskRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.SignalWithStartWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetSignalWithStartRequest().GetWorkflowId()),
		}
	case *historyservice.SignalWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetSignalRequest().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetSignalRequest().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.StartWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetStartRequest().GetWorkflowId()),
		}
	case *historyservice.SyncActivityRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *historyservice.SyncShardStatusRequest:
		return nil
	case *historyservice.SyncWorkflowStateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *historyservice.TerminateWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetTerminateRequest().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetTerminateRequest().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.UpdateActivityOptionsRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *historyservice.UpdateWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetRequest().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetRequest().GetWorkflowExecution().GetRunId()),
		}
	case *historyservice.VerifyChildExecutionCompletionRecordedRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetParentExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetParentExecution().GetRunId()),
		}
	case *historyservice.VerifyFirstWorkflowTaskScheduledRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	default:
		return nil
	}
}
