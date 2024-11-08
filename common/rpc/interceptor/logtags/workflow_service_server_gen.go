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

func (wt *WorkflowTags) extractFromWorkflowServiceServerRequest(req any) []tag.Tag {
	switch r := req.(type) {
	case *workflowservice.CountWorkflowExecutionsRequest:
		return nil
	case *workflowservice.CreateScheduleRequest:
		return nil
	case *workflowservice.DeleteScheduleRequest:
		return nil
	case *workflowservice.DeleteWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.DeprecateNamespaceRequest:
		return nil
	case *workflowservice.DescribeBatchOperationRequest:
		return nil
	case *workflowservice.DescribeNamespaceRequest:
		return nil
	case *workflowservice.DescribeScheduleRequest:
		return nil
	case *workflowservice.DescribeTaskQueueRequest:
		return nil
	case *workflowservice.DescribeWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *workflowservice.ExecuteMultiOperationRequest:
		return nil
	case *workflowservice.GetClusterInfoRequest:
		return nil
	case *workflowservice.GetSearchAttributesRequest:
		return nil
	case *workflowservice.GetSystemInfoRequest:
		return nil
	case *workflowservice.GetWorkerBuildIdCompatibilityRequest:
		return nil
	case *workflowservice.GetWorkerTaskReachabilityRequest:
		return nil
	case *workflowservice.GetWorkerVersioningRulesRequest:
		return nil
	case *workflowservice.GetWorkflowExecutionHistoryRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *workflowservice.GetWorkflowExecutionHistoryReverseRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *workflowservice.ListArchivedWorkflowExecutionsRequest:
		return nil
	case *workflowservice.ListBatchOperationsRequest:
		return nil
	case *workflowservice.ListClosedWorkflowExecutionsRequest:
		return nil
	case *workflowservice.ListNamespacesRequest:
		return nil
	case *workflowservice.ListOpenWorkflowExecutionsRequest:
		return nil
	case *workflowservice.ListScheduleMatchingTimesRequest:
		return nil
	case *workflowservice.ListSchedulesRequest:
		return nil
	case *workflowservice.ListTaskQueuePartitionsRequest:
		return nil
	case *workflowservice.ListWorkflowExecutionsRequest:
		return nil
	case *workflowservice.PatchScheduleRequest:
		return nil
	case *workflowservice.PollActivityTaskQueueRequest:
		return nil
	case *workflowservice.PollNexusTaskQueueRequest:
		return nil
	case *workflowservice.PollWorkflowExecutionUpdateRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetUpdateRef().GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetUpdateRef().GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.PollWorkflowTaskQueueRequest:
		return nil
	case *workflowservice.QueryWorkflowRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *workflowservice.RecordActivityTaskHeartbeatRequest:
		return wt.fromTaskToken(r.GetTaskToken())
	case *workflowservice.RecordActivityTaskHeartbeatByIdRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.RegisterNamespaceRequest:
		return nil
	case *workflowservice.RequestCancelWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.ResetStickyTaskQueueRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetExecution().GetRunId()),
		}
	case *workflowservice.ResetWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.RespondActivityTaskCanceledRequest:
		return wt.fromTaskToken(r.GetTaskToken())
	case *workflowservice.RespondActivityTaskCanceledByIdRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.RespondActivityTaskCompletedRequest:
		return wt.fromTaskToken(r.GetTaskToken())
	case *workflowservice.RespondActivityTaskCompletedByIdRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.RespondActivityTaskFailedRequest:
		return wt.fromTaskToken(r.GetTaskToken())
	case *workflowservice.RespondActivityTaskFailedByIdRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.RespondNexusTaskCompletedRequest:
		return nil
	case *workflowservice.RespondNexusTaskFailedRequest:
		return nil
	case *workflowservice.RespondQueryTaskCompletedRequest:
		return nil
	case *workflowservice.RespondWorkflowTaskCompletedRequest:
		return wt.fromTaskToken(r.GetTaskToken())
	case *workflowservice.RespondWorkflowTaskFailedRequest:
		return wt.fromTaskToken(r.GetTaskToken())
	case *workflowservice.ScanWorkflowExecutionsRequest:
		return nil
	case *workflowservice.ShutdownWorkerRequest:
		return nil
	case *workflowservice.SignalWithStartWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
		}
	case *workflowservice.SignalWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.StartBatchOperationRequest:
		return nil
	case *workflowservice.StartWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
		}
	case *workflowservice.StopBatchOperationRequest:
		return nil
	case *workflowservice.TerminateWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.UpdateActivityOptionsByIdRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowId()),
			tag.WorkflowRunID(r.GetRunId()),
		}
	case *workflowservice.UpdateNamespaceRequest:
		return nil
	case *workflowservice.UpdateScheduleRequest:
		return nil
	case *workflowservice.UpdateWorkerBuildIdCompatibilityRequest:
		return nil
	case *workflowservice.UpdateWorkerVersioningRulesRequest:
		return nil
	case *workflowservice.UpdateWorkflowExecutionRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	case *workflowservice.UpdateWorkflowExecutionOptionsRequest:
		return []tag.Tag{
			tag.WorkflowID(r.GetWorkflowExecution().GetWorkflowId()),
			tag.WorkflowRunID(r.GetWorkflowExecution().GetRunId()),
		}
	default:
		return nil
	}
}
