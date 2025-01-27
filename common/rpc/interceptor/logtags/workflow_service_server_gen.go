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
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log/tag"
)

func (wt *WorkflowTags) extractFromWorkflowServiceServerPayload(payload any) []tag.Tag {
	switch r := payload.(type) {
	case *workflowservice.CountWorkflowExecutionsRequest:
		return nil
	case *workflowservice.CountWorkflowExecutionsResponse:
		return nil
	case *workflowservice.CreateScheduleRequest:
		return nil
	case *workflowservice.CreateScheduleResponse:
		return nil
	case *workflowservice.DeleteScheduleRequest:
		return nil
	case *workflowservice.DeleteScheduleResponse:
		return nil
	case *workflowservice.DeleteWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.DeleteWorkflowExecutionResponse:
		return nil
	case *workflowservice.DeprecateNamespaceRequest:
		return nil
	case *workflowservice.DeprecateNamespaceResponse:
		return nil
	case *workflowservice.DescribeBatchOperationRequest:
		return nil
	case *workflowservice.DescribeBatchOperationResponse:
		return nil
	case *workflowservice.DescribeDeploymentRequest:
		return nil
	case *workflowservice.DescribeDeploymentResponse:
		return nil
	case *workflowservice.DescribeNamespaceRequest:
		return nil
	case *workflowservice.DescribeNamespaceResponse:
		return nil
	case *workflowservice.DescribeScheduleRequest:
		return nil
	case *workflowservice.DescribeScheduleResponse:
		return nil
	case *workflowservice.DescribeTaskQueueRequest:
		return nil
	case *workflowservice.DescribeTaskQueueResponse:
		return nil
	case *workflowservice.DescribeWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *workflowservice.DescribeWorkflowExecutionResponse:
		return nil
	case *workflowservice.ExecuteMultiOperationRequest:
		return nil
	case *workflowservice.ExecuteMultiOperationResponse:
		return nil
	case *workflowservice.GetClusterInfoRequest:
		return nil
	case *workflowservice.GetClusterInfoResponse:
		return nil
	case *workflowservice.GetCurrentDeploymentRequest:
		return nil
	case *workflowservice.GetCurrentDeploymentResponse:
		return nil
	case *workflowservice.GetDeploymentReachabilityRequest:
		return nil
	case *workflowservice.GetDeploymentReachabilityResponse:
		return nil
	case *workflowservice.GetSearchAttributesRequest:
		return nil
	case *workflowservice.GetSearchAttributesResponse:
		return nil
	case *workflowservice.GetSystemInfoRequest:
		return nil
	case *workflowservice.GetSystemInfoResponse:
		return nil
	case *workflowservice.GetWorkerBuildIdCompatibilityRequest:
		return nil
	case *workflowservice.GetWorkerBuildIdCompatibilityResponse:
		return nil
	case *workflowservice.GetWorkerTaskReachabilityRequest:
		return nil
	case *workflowservice.GetWorkerTaskReachabilityResponse:
		return nil
	case *workflowservice.GetWorkerVersioningRulesRequest:
		return nil
	case *workflowservice.GetWorkerVersioningRulesResponse:
		return nil
	case *workflowservice.GetWorkflowExecutionHistoryRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *workflowservice.GetWorkflowExecutionHistoryResponse:
		return nil
	case *workflowservice.GetWorkflowExecutionHistoryReverseRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *workflowservice.GetWorkflowExecutionHistoryReverseResponse:
		return nil
	case *workflowservice.ListArchivedWorkflowExecutionsRequest:
		return nil
	case *workflowservice.ListArchivedWorkflowExecutionsResponse:
		return nil
	case *workflowservice.ListBatchOperationsRequest:
		return nil
	case *workflowservice.ListBatchOperationsResponse:
		return nil
	case *workflowservice.ListClosedWorkflowExecutionsRequest:
		return nil
	case *workflowservice.ListClosedWorkflowExecutionsResponse:
		return nil
	case *workflowservice.ListDeploymentsRequest:
		return nil
	case *workflowservice.ListDeploymentsResponse:
		return nil
	case *workflowservice.ListNamespacesRequest:
		return nil
	case *workflowservice.ListNamespacesResponse:
		return nil
	case *workflowservice.ListOpenWorkflowExecutionsRequest:
		return nil
	case *workflowservice.ListOpenWorkflowExecutionsResponse:
		return nil
	case *workflowservice.ListScheduleMatchingTimesRequest:
		return nil
	case *workflowservice.ListScheduleMatchingTimesResponse:
		return nil
	case *workflowservice.ListSchedulesRequest:
		return nil
	case *workflowservice.ListSchedulesResponse:
		return nil
	case *workflowservice.ListTaskQueuePartitionsRequest:
		return nil
	case *workflowservice.ListTaskQueuePartitionsResponse:
		return nil
	case *workflowservice.ListWorkflowExecutionsRequest:
		return nil
	case *workflowservice.ListWorkflowExecutionsResponse:
		return nil
	case *workflowservice.PatchScheduleRequest:
		return nil
	case *workflowservice.PatchScheduleResponse:
		return nil
	case *workflowservice.PauseActivityByIdRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.PauseActivityByIdResponse:
		return nil
	case *workflowservice.PollActivityTaskQueueRequest:
		return nil
	case *workflowservice.PollActivityTaskQueueResponse:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.PollNexusTaskQueueRequest:
		return nil
	case *workflowservice.PollNexusTaskQueueResponse:
		return wt.fromTaskToken(r.GetTaskToken())
	case *workflowservice.PollWorkflowExecutionUpdateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetUpdateRef().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetUpdateRef().GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.PollWorkflowExecutionUpdateResponse:
		return []tag.Tag{
			tag.WorkflowID(r.GetUpdateRef().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetUpdateRef().GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.PollWorkflowTaskQueueRequest:
		return nil
	case *workflowservice.PollWorkflowTaskQueueResponse:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.QueryWorkflowRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *workflowservice.QueryWorkflowResponse:
		return nil
	case *workflowservice.RecordActivityTaskHeartbeatRequest:
		return wt.fromTaskToken(r.GetTaskToken())
	case *workflowservice.RecordActivityTaskHeartbeatResponse:
		return nil
	case *workflowservice.RecordActivityTaskHeartbeatByIdRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.RecordActivityTaskHeartbeatByIdResponse:
		return nil
	case *workflowservice.RegisterNamespaceRequest:
		return nil
	case *workflowservice.RegisterNamespaceResponse:
		return nil
	case *workflowservice.RequestCancelWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.RequestCancelWorkflowExecutionResponse:
		return nil
	case *workflowservice.ResetActivityByIdRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.ResetActivityByIdResponse:
		return nil
	case *workflowservice.ResetStickyTaskQueueRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *workflowservice.ResetStickyTaskQueueResponse:
		return nil
	case *workflowservice.ResetWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.ResetWorkflowExecutionResponse:
		return []tag.Tag{
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.RespondActivityTaskCanceledRequest:
		return wt.fromTaskToken(r.GetTaskToken())
	case *workflowservice.RespondActivityTaskCanceledResponse:
		return nil
	case *workflowservice.RespondActivityTaskCanceledByIdRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.RespondActivityTaskCanceledByIdResponse:
		return nil
	case *workflowservice.RespondActivityTaskCompletedRequest:
		return wt.fromTaskToken(r.GetTaskToken())
	case *workflowservice.RespondActivityTaskCompletedResponse:
		return nil
	case *workflowservice.RespondActivityTaskCompletedByIdRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.RespondActivityTaskCompletedByIdResponse:
		return nil
	case *workflowservice.RespondActivityTaskFailedRequest:
		return wt.fromTaskToken(r.GetTaskToken())
	case *workflowservice.RespondActivityTaskFailedResponse:
		return nil
	case *workflowservice.RespondActivityTaskFailedByIdRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.RespondActivityTaskFailedByIdResponse:
		return nil
	case *workflowservice.RespondNexusTaskCompletedRequest:
		return nil
	case *workflowservice.RespondNexusTaskCompletedResponse:
		return nil
	case *workflowservice.RespondNexusTaskFailedRequest:
		return nil
	case *workflowservice.RespondNexusTaskFailedResponse:
		return nil
	case *workflowservice.RespondQueryTaskCompletedRequest:
		return nil
	case *workflowservice.RespondQueryTaskCompletedResponse:
		return nil
	case *workflowservice.RespondWorkflowTaskCompletedRequest:
		return wt.fromTaskToken(r.GetTaskToken())
	case *workflowservice.RespondWorkflowTaskCompletedResponse:
		return nil
	case *workflowservice.RespondWorkflowTaskFailedRequest:
		return wt.fromTaskToken(r.GetTaskToken())
	case *workflowservice.RespondWorkflowTaskFailedResponse:
		return nil
	case *workflowservice.ScanWorkflowExecutionsRequest:
		return nil
	case *workflowservice.ScanWorkflowExecutionsResponse:
		return nil
	case *workflowservice.SetCurrentDeploymentRequest:
		return nil
	case *workflowservice.SetCurrentDeploymentResponse:
		return nil
	case *workflowservice.ShutdownWorkerRequest:
		return nil
	case *workflowservice.ShutdownWorkerResponse:
		return nil
	case *workflowservice.SignalWithStartWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
		}
	case *workflowservice.SignalWithStartWorkflowExecutionResponse:
		return []tag.Tag{
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.SignalWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.SignalWorkflowExecutionResponse:
		return nil
	case *workflowservice.StartBatchOperationRequest:
		return nil
	case *workflowservice.StartBatchOperationResponse:
		return nil
	case *workflowservice.StartWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
		}
	case *workflowservice.StartWorkflowExecutionResponse:
		return []tag.Tag{
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.StopBatchOperationRequest:
		return nil
	case *workflowservice.StopBatchOperationResponse:
		return nil
	case *workflowservice.TerminateWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.TerminateWorkflowExecutionResponse:
		return nil
	case *workflowservice.UnpauseActivityByIdRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.UnpauseActivityByIdResponse:
		return nil
	case *workflowservice.UpdateActivityOptionsByIdRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.UpdateActivityOptionsByIdResponse:
		return nil
	case *workflowservice.UpdateNamespaceRequest:
		return nil
	case *workflowservice.UpdateNamespaceResponse:
		return nil
	case *workflowservice.UpdateScheduleRequest:
		return nil
	case *workflowservice.UpdateScheduleResponse:
		return nil
	case *workflowservice.UpdateWorkerBuildIdCompatibilityRequest:
		return nil
	case *workflowservice.UpdateWorkerBuildIdCompatibilityResponse:
		return nil
	case *workflowservice.UpdateWorkerVersioningRulesRequest:
		return nil
	case *workflowservice.UpdateWorkerVersioningRulesResponse:
		return nil
	case *workflowservice.UpdateWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.UpdateWorkflowExecutionResponse:
		return []tag.Tag{
			tag.WorkflowID(r.GetUpdateRef().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetUpdateRef().GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.UpdateWorkflowExecutionOptionsRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.UpdateWorkflowExecutionOptionsResponse:
		return nil
	default:
		return nil
	}
}
