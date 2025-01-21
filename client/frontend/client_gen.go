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

// Code generated by cmd/tools/genrpcwrappers. DO NOT EDIT.

package frontend

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

func (c *clientImpl) CountWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.CountWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.CountWorkflowExecutions(ctx, request, opts...)
}

func (c *clientImpl) CreateSchedule(
	ctx context.Context,
	request *workflowservice.CreateScheduleRequest,
	opts ...grpc.CallOption,
) (*workflowservice.CreateScheduleResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.CreateSchedule(ctx, request, opts...)
}

func (c *clientImpl) DeleteSchedule(
	ctx context.Context,
	request *workflowservice.DeleteScheduleRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DeleteScheduleResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DeleteSchedule(ctx, request, opts...)
}

func (c *clientImpl) DeleteWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DeleteWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DeleteWorkflowExecutionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DeleteWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) DeprecateNamespace(
	ctx context.Context,
	request *workflowservice.DeprecateNamespaceRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DeprecateNamespaceResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DeprecateNamespace(ctx, request, opts...)
}

func (c *clientImpl) DescribeBatchOperation(
	ctx context.Context,
	request *workflowservice.DescribeBatchOperationRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeBatchOperationResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DescribeBatchOperation(ctx, request, opts...)
}

func (c *clientImpl) DescribeDeployment(
	ctx context.Context,
	request *workflowservice.DescribeDeploymentRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeDeploymentResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DescribeDeployment(ctx, request, opts...)
}

func (c *clientImpl) DescribeNamespace(
	ctx context.Context,
	request *workflowservice.DescribeNamespaceRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeNamespaceResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DescribeNamespace(ctx, request, opts...)
}

func (c *clientImpl) DescribeSchedule(
	ctx context.Context,
	request *workflowservice.DescribeScheduleRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeScheduleResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DescribeSchedule(ctx, request, opts...)
}

func (c *clientImpl) DescribeTaskQueue(
	ctx context.Context,
	request *workflowservice.DescribeTaskQueueRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeTaskQueueResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DescribeTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) DescribeWorkerDeploymentVersion(
	ctx context.Context,
	request *workflowservice.DescribeWorkerDeploymentVersionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeWorkerDeploymentVersionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DescribeWorkerDeploymentVersion(ctx, request, opts...)
}

func (c *clientImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.DescribeWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) ExecuteMultiOperation(
	ctx context.Context,
	request *workflowservice.ExecuteMultiOperationRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ExecuteMultiOperationResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ExecuteMultiOperation(ctx, request, opts...)
}

func (c *clientImpl) GetClusterInfo(
	ctx context.Context,
	request *workflowservice.GetClusterInfoRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetClusterInfoResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetClusterInfo(ctx, request, opts...)
}

func (c *clientImpl) GetCurrentDeployment(
	ctx context.Context,
	request *workflowservice.GetCurrentDeploymentRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetCurrentDeploymentResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetCurrentDeployment(ctx, request, opts...)
}

func (c *clientImpl) GetDeploymentReachability(
	ctx context.Context,
	request *workflowservice.GetDeploymentReachabilityRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetDeploymentReachabilityResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetDeploymentReachability(ctx, request, opts...)
}

func (c *clientImpl) GetSearchAttributes(
	ctx context.Context,
	request *workflowservice.GetSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetSearchAttributesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetSearchAttributes(ctx, request, opts...)
}

func (c *clientImpl) GetSystemInfo(
	ctx context.Context,
	request *workflowservice.GetSystemInfoRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetSystemInfoResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetSystemInfo(ctx, request, opts...)
}

func (c *clientImpl) GetWorkerBuildIdCompatibility(
	ctx context.Context,
	request *workflowservice.GetWorkerBuildIdCompatibilityRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkerBuildIdCompatibilityResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetWorkerBuildIdCompatibility(ctx, request, opts...)
}

func (c *clientImpl) GetWorkerTaskReachability(
	ctx context.Context,
	request *workflowservice.GetWorkerTaskReachabilityRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkerTaskReachabilityResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetWorkerTaskReachability(ctx, request, opts...)
}

func (c *clientImpl) GetWorkerVersioningRules(
	ctx context.Context,
	request *workflowservice.GetWorkerVersioningRulesRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkerVersioningRulesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetWorkerVersioningRules(ctx, request, opts...)
}

func (c *clientImpl) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetWorkflowExecutionHistory(ctx, request, opts...)
}

func (c *clientImpl) GetWorkflowExecutionHistoryReverse(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryReverseRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.GetWorkflowExecutionHistoryReverse(ctx, request, opts...)
}

func (c *clientImpl) ListArchivedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListArchivedWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return c.client.ListArchivedWorkflowExecutions(ctx, request, opts...)
}

func (c *clientImpl) ListBatchOperations(
	ctx context.Context,
	request *workflowservice.ListBatchOperationsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListBatchOperationsResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ListBatchOperations(ctx, request, opts...)
}

func (c *clientImpl) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListClosedWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ListClosedWorkflowExecutions(ctx, request, opts...)
}

func (c *clientImpl) ListDeployments(
	ctx context.Context,
	request *workflowservice.ListDeploymentsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListDeploymentsResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ListDeployments(ctx, request, opts...)
}

func (c *clientImpl) ListNamespaces(
	ctx context.Context,
	request *workflowservice.ListNamespacesRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListNamespacesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ListNamespaces(ctx, request, opts...)
}

func (c *clientImpl) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListOpenWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ListOpenWorkflowExecutions(ctx, request, opts...)
}

func (c *clientImpl) ListScheduleMatchingTimes(
	ctx context.Context,
	request *workflowservice.ListScheduleMatchingTimesRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListScheduleMatchingTimesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ListScheduleMatchingTimes(ctx, request, opts...)
}

func (c *clientImpl) ListSchedules(
	ctx context.Context,
	request *workflowservice.ListSchedulesRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListSchedulesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ListSchedules(ctx, request, opts...)
}

func (c *clientImpl) ListTaskQueuePartitions(
	ctx context.Context,
	request *workflowservice.ListTaskQueuePartitionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListTaskQueuePartitionsResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ListTaskQueuePartitions(ctx, request, opts...)
}

func (c *clientImpl) ListWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ListWorkflowExecutions(ctx, request, opts...)
}

func (c *clientImpl) PatchSchedule(
	ctx context.Context,
	request *workflowservice.PatchScheduleRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PatchScheduleResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.PatchSchedule(ctx, request, opts...)
}

func (c *clientImpl) PauseActivityById(
	ctx context.Context,
	request *workflowservice.PauseActivityByIdRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PauseActivityByIdResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.PauseActivityById(ctx, request, opts...)
}

func (c *clientImpl) PollActivityTaskQueue(
	ctx context.Context,
	request *workflowservice.PollActivityTaskQueueRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollActivityTaskQueueResponse, error) {
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return c.client.PollActivityTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) PollNexusTaskQueue(
	ctx context.Context,
	request *workflowservice.PollNexusTaskQueueRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollNexusTaskQueueResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.PollNexusTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) PollWorkflowExecutionUpdate(
	ctx context.Context,
	request *workflowservice.PollWorkflowExecutionUpdateRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.PollWorkflowExecutionUpdate(ctx, request, opts...)
}

func (c *clientImpl) PollWorkflowTaskQueue(
	ctx context.Context,
	request *workflowservice.PollWorkflowTaskQueueRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return c.client.PollWorkflowTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) QueryWorkflow(
	ctx context.Context,
	request *workflowservice.QueryWorkflowRequest,
	opts ...grpc.CallOption,
) (*workflowservice.QueryWorkflowResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.QueryWorkflow(ctx, request, opts...)
}

func (c *clientImpl) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RecordActivityTaskHeartbeatResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RecordActivityTaskHeartbeat(ctx, request, opts...)
}

func (c *clientImpl) RecordActivityTaskHeartbeatById(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatByIdRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RecordActivityTaskHeartbeatByIdResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RecordActivityTaskHeartbeatById(ctx, request, opts...)
}

func (c *clientImpl) RegisterNamespace(
	ctx context.Context,
	request *workflowservice.RegisterNamespaceRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RegisterNamespaceResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RegisterNamespace(ctx, request, opts...)
}

func (c *clientImpl) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *workflowservice.RequestCancelWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RequestCancelWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) ResetActivityById(
	ctx context.Context,
	request *workflowservice.ResetActivityByIdRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ResetActivityByIdResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ResetActivityById(ctx, request, opts...)
}

func (c *clientImpl) ResetStickyTaskQueue(
	ctx context.Context,
	request *workflowservice.ResetStickyTaskQueueRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ResetStickyTaskQueueResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ResetStickyTaskQueue(ctx, request, opts...)
}

func (c *clientImpl) ResetWorkflowExecution(
	ctx context.Context,
	request *workflowservice.ResetWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ResetWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) RespondActivityTaskCanceled(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCanceledResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RespondActivityTaskCanceled(ctx, request, opts...)
}

func (c *clientImpl) RespondActivityTaskCanceledById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledByIdRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCanceledByIdResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RespondActivityTaskCanceledById(ctx, request, opts...)
}

func (c *clientImpl) RespondActivityTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RespondActivityTaskCompleted(ctx, request, opts...)
}

func (c *clientImpl) RespondActivityTaskCompletedById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedByIdRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCompletedByIdResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RespondActivityTaskCompletedById(ctx, request, opts...)
}

func (c *clientImpl) RespondActivityTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskFailedResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RespondActivityTaskFailed(ctx, request, opts...)
}

func (c *clientImpl) RespondActivityTaskFailedById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedByIdRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskFailedByIdResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RespondActivityTaskFailedById(ctx, request, opts...)
}

func (c *clientImpl) RespondNexusTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondNexusTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondNexusTaskCompletedResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RespondNexusTaskCompleted(ctx, request, opts...)
}

func (c *clientImpl) RespondNexusTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondNexusTaskFailedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondNexusTaskFailedResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RespondNexusTaskFailed(ctx, request, opts...)
}

func (c *clientImpl) RespondQueryTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondQueryTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondQueryTaskCompletedResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RespondQueryTaskCompleted(ctx, request, opts...)
}

func (c *clientImpl) RespondWorkflowTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RespondWorkflowTaskCompleted(ctx, request, opts...)
}

func (c *clientImpl) RespondWorkflowTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskFailedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondWorkflowTaskFailedResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.RespondWorkflowTaskFailed(ctx, request, opts...)
}

func (c *clientImpl) ScanWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ScanWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ScanWorkflowExecutions(ctx, request, opts...)
}

func (c *clientImpl) SetCurrentDeployment(
	ctx context.Context,
	request *workflowservice.SetCurrentDeploymentRequest,
	opts ...grpc.CallOption,
) (*workflowservice.SetCurrentDeploymentResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.SetCurrentDeployment(ctx, request, opts...)
}

func (c *clientImpl) SetWorkerDeploymentCurrentVersion(
	ctx context.Context,
	request *workflowservice.SetWorkerDeploymentCurrentVersionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.SetWorkerDeploymentCurrentVersionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.SetWorkerDeploymentCurrentVersion(ctx, request, opts...)
}

func (c *clientImpl) SetWorkerDeploymentRampingVersion(
	ctx context.Context,
	request *workflowservice.SetWorkerDeploymentRampingVersionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.SetWorkerDeploymentRampingVersionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.SetWorkerDeploymentRampingVersion(ctx, request, opts...)
}

func (c *clientImpl) ShutdownWorker(
	ctx context.Context,
	request *workflowservice.ShutdownWorkerRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ShutdownWorkerResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.ShutdownWorker(ctx, request, opts...)
}

func (c *clientImpl) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.SignalWithStartWorkflowExecutionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.SignalWithStartWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) SignalWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.SignalWorkflowExecutionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.SignalWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) StartBatchOperation(
	ctx context.Context,
	request *workflowservice.StartBatchOperationRequest,
	opts ...grpc.CallOption,
) (*workflowservice.StartBatchOperationResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.StartBatchOperation(ctx, request, opts...)
}

func (c *clientImpl) StartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.StartWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) StopBatchOperation(
	ctx context.Context,
	request *workflowservice.StopBatchOperationRequest,
	opts ...grpc.CallOption,
) (*workflowservice.StopBatchOperationResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.StopBatchOperation(ctx, request, opts...)
}

func (c *clientImpl) TerminateWorkflowExecution(
	ctx context.Context,
	request *workflowservice.TerminateWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.TerminateWorkflowExecutionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.TerminateWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) UnpauseActivityById(
	ctx context.Context,
	request *workflowservice.UnpauseActivityByIdRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UnpauseActivityByIdResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.UnpauseActivityById(ctx, request, opts...)
}

func (c *clientImpl) UpdateActivityOptionsById(
	ctx context.Context,
	request *workflowservice.UpdateActivityOptionsByIdRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateActivityOptionsByIdResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.UpdateActivityOptionsById(ctx, request, opts...)
}

func (c *clientImpl) UpdateNamespace(
	ctx context.Context,
	request *workflowservice.UpdateNamespaceRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateNamespaceResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.UpdateNamespace(ctx, request, opts...)
}

func (c *clientImpl) UpdateSchedule(
	ctx context.Context,
	request *workflowservice.UpdateScheduleRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateScheduleResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.UpdateSchedule(ctx, request, opts...)
}

func (c *clientImpl) UpdateWorkerBuildIdCompatibility(
	ctx context.Context,
	request *workflowservice.UpdateWorkerBuildIdCompatibilityRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateWorkerBuildIdCompatibilityResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.UpdateWorkerBuildIdCompatibility(ctx, request, opts...)
}

func (c *clientImpl) UpdateWorkerVersioningRules(
	ctx context.Context,
	request *workflowservice.UpdateWorkerVersioningRulesRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateWorkerVersioningRulesResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.UpdateWorkerVersioningRules(ctx, request, opts...)
}

func (c *clientImpl) UpdateWorkflowExecution(
	ctx context.Context,
	request *workflowservice.UpdateWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.UpdateWorkflowExecution(ctx, request, opts...)
}

func (c *clientImpl) UpdateWorkflowExecutionOptions(
	ctx context.Context,
	request *workflowservice.UpdateWorkflowExecutionOptionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateWorkflowExecutionOptionsResponse, error) {
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return c.client.UpdateWorkflowExecutionOptions(ctx, request, opts...)
}
