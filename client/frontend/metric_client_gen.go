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

// Code generated by cmd/tools/rpcwrappers. DO NOT EDIT.

package frontend

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

func (c *metricClient) CountWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.CountWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.CountWorkflowExecutionsResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientCountWorkflowExecutions")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.CountWorkflowExecutions(ctx, request, opts...)
}

func (c *metricClient) CreateSchedule(
	ctx context.Context,
	request *workflowservice.CreateScheduleRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.CreateScheduleResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientCreateSchedule")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.CreateSchedule(ctx, request, opts...)
}

func (c *metricClient) DeleteSchedule(
	ctx context.Context,
	request *workflowservice.DeleteScheduleRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.DeleteScheduleResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientDeleteSchedule")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DeleteSchedule(ctx, request, opts...)
}

func (c *metricClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DeleteWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.DeleteWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientDeleteWorkflowExecution")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DeleteWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) DeprecateNamespace(
	ctx context.Context,
	request *workflowservice.DeprecateNamespaceRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.DeprecateNamespaceResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientDeprecateNamespace")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DeprecateNamespace(ctx, request, opts...)
}

func (c *metricClient) DescribeBatchOperation(
	ctx context.Context,
	request *workflowservice.DescribeBatchOperationRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.DescribeBatchOperationResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientDescribeBatchOperation")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeBatchOperation(ctx, request, opts...)
}

func (c *metricClient) DescribeNamespace(
	ctx context.Context,
	request *workflowservice.DescribeNamespaceRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.DescribeNamespaceResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientDescribeNamespace")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeNamespace(ctx, request, opts...)
}

func (c *metricClient) DescribeSchedule(
	ctx context.Context,
	request *workflowservice.DescribeScheduleRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.DescribeScheduleResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientDescribeSchedule")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeSchedule(ctx, request, opts...)
}

func (c *metricClient) DescribeTaskQueue(
	ctx context.Context,
	request *workflowservice.DescribeTaskQueueRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.DescribeTaskQueueResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientDescribeTaskQueue")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeTaskQueue(ctx, request, opts...)
}

func (c *metricClient) DescribeWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.DescribeWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientDescribeWorkflowExecution")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) ExecuteMultiOperation(
	ctx context.Context,
	request *workflowservice.ExecuteMultiOperationRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ExecuteMultiOperationResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientExecuteMultiOperation")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ExecuteMultiOperation(ctx, request, opts...)
}

func (c *metricClient) GetClusterInfo(
	ctx context.Context,
	request *workflowservice.GetClusterInfoRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.GetClusterInfoResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientGetClusterInfo")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetClusterInfo(ctx, request, opts...)
}

func (c *metricClient) GetSearchAttributes(
	ctx context.Context,
	request *workflowservice.GetSearchAttributesRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.GetSearchAttributesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientGetSearchAttributes")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetSearchAttributes(ctx, request, opts...)
}

func (c *metricClient) GetSystemInfo(
	ctx context.Context,
	request *workflowservice.GetSystemInfoRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.GetSystemInfoResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientGetSystemInfo")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetSystemInfo(ctx, request, opts...)
}

func (c *metricClient) GetWorkerBuildIdCompatibility(
	ctx context.Context,
	request *workflowservice.GetWorkerBuildIdCompatibilityRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.GetWorkerBuildIdCompatibilityResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientGetWorkerBuildIdCompatibility")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetWorkerBuildIdCompatibility(ctx, request, opts...)
}

func (c *metricClient) GetWorkerTaskReachability(
	ctx context.Context,
	request *workflowservice.GetWorkerTaskReachabilityRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.GetWorkerTaskReachabilityResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientGetWorkerTaskReachability")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetWorkerTaskReachability(ctx, request, opts...)
}

func (c *metricClient) GetWorkerVersioningRules(
	ctx context.Context,
	request *workflowservice.GetWorkerVersioningRulesRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.GetWorkerVersioningRulesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientGetWorkerVersioningRules")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetWorkerVersioningRules(ctx, request, opts...)
}

func (c *metricClient) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.GetWorkflowExecutionHistoryResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientGetWorkflowExecutionHistory")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetWorkflowExecutionHistory(ctx, request, opts...)
}

func (c *metricClient) GetWorkflowExecutionHistoryReverse(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryReverseRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.GetWorkflowExecutionHistoryReverseResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientGetWorkflowExecutionHistoryReverse")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetWorkflowExecutionHistoryReverse(ctx, request, opts...)
}

func (c *metricClient) ListArchivedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListArchivedWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ListArchivedWorkflowExecutionsResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientListArchivedWorkflowExecutions")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListArchivedWorkflowExecutions(ctx, request, opts...)
}

func (c *metricClient) ListBatchOperations(
	ctx context.Context,
	request *workflowservice.ListBatchOperationsRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ListBatchOperationsResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientListBatchOperations")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListBatchOperations(ctx, request, opts...)
}

func (c *metricClient) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListClosedWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ListClosedWorkflowExecutionsResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientListClosedWorkflowExecutions")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListClosedWorkflowExecutions(ctx, request, opts...)
}

func (c *metricClient) ListNamespaces(
	ctx context.Context,
	request *workflowservice.ListNamespacesRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ListNamespacesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientListNamespaces")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListNamespaces(ctx, request, opts...)
}

func (c *metricClient) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListOpenWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ListOpenWorkflowExecutionsResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientListOpenWorkflowExecutions")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListOpenWorkflowExecutions(ctx, request, opts...)
}

func (c *metricClient) ListScheduleMatchingTimes(
	ctx context.Context,
	request *workflowservice.ListScheduleMatchingTimesRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ListScheduleMatchingTimesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientListScheduleMatchingTimes")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListScheduleMatchingTimes(ctx, request, opts...)
}

func (c *metricClient) ListSchedules(
	ctx context.Context,
	request *workflowservice.ListSchedulesRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ListSchedulesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientListSchedules")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListSchedules(ctx, request, opts...)
}

func (c *metricClient) ListTaskQueuePartitions(
	ctx context.Context,
	request *workflowservice.ListTaskQueuePartitionsRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ListTaskQueuePartitionsResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientListTaskQueuePartitions")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListTaskQueuePartitions(ctx, request, opts...)
}

func (c *metricClient) ListWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ListWorkflowExecutionsResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientListWorkflowExecutions")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListWorkflowExecutions(ctx, request, opts...)
}

func (c *metricClient) PatchSchedule(
	ctx context.Context,
	request *workflowservice.PatchScheduleRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.PatchScheduleResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientPatchSchedule")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.PatchSchedule(ctx, request, opts...)
}

func (c *metricClient) PollActivityTaskQueue(
	ctx context.Context,
	request *workflowservice.PollActivityTaskQueueRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.PollActivityTaskQueueResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientPollActivityTaskQueue")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.PollActivityTaskQueue(ctx, request, opts...)
}

func (c *metricClient) PollNexusTaskQueue(
	ctx context.Context,
	request *workflowservice.PollNexusTaskQueueRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.PollNexusTaskQueueResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientPollNexusTaskQueue")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.PollNexusTaskQueue(ctx, request, opts...)
}

func (c *metricClient) PollWorkflowExecutionUpdate(
	ctx context.Context,
	request *workflowservice.PollWorkflowExecutionUpdateRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.PollWorkflowExecutionUpdateResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientPollWorkflowExecutionUpdate")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.PollWorkflowExecutionUpdate(ctx, request, opts...)
}

func (c *metricClient) PollWorkflowTaskQueue(
	ctx context.Context,
	request *workflowservice.PollWorkflowTaskQueueRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.PollWorkflowTaskQueueResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientPollWorkflowTaskQueue")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.PollWorkflowTaskQueue(ctx, request, opts...)
}

func (c *metricClient) QueryWorkflow(
	ctx context.Context,
	request *workflowservice.QueryWorkflowRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.QueryWorkflowResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientQueryWorkflow")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.QueryWorkflow(ctx, request, opts...)
}

func (c *metricClient) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RecordActivityTaskHeartbeatResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRecordActivityTaskHeartbeat")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RecordActivityTaskHeartbeat(ctx, request, opts...)
}

func (c *metricClient) RecordActivityTaskHeartbeatById(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatByIdRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RecordActivityTaskHeartbeatByIdResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRecordActivityTaskHeartbeatById")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RecordActivityTaskHeartbeatById(ctx, request, opts...)
}

func (c *metricClient) RegisterNamespace(
	ctx context.Context,
	request *workflowservice.RegisterNamespaceRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RegisterNamespaceResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRegisterNamespace")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RegisterNamespace(ctx, request, opts...)
}

func (c *metricClient) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *workflowservice.RequestCancelWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RequestCancelWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRequestCancelWorkflowExecution")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RequestCancelWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) ResetStickyTaskQueue(
	ctx context.Context,
	request *workflowservice.ResetStickyTaskQueueRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ResetStickyTaskQueueResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientResetStickyTaskQueue")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ResetStickyTaskQueue(ctx, request, opts...)
}

func (c *metricClient) ResetWorkflowExecution(
	ctx context.Context,
	request *workflowservice.ResetWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ResetWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientResetWorkflowExecution")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ResetWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) RespondActivityTaskCanceled(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RespondActivityTaskCanceledResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRespondActivityTaskCanceled")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondActivityTaskCanceled(ctx, request, opts...)
}

func (c *metricClient) RespondActivityTaskCanceledById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledByIdRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RespondActivityTaskCanceledByIdResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRespondActivityTaskCanceledById")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondActivityTaskCanceledById(ctx, request, opts...)
}

func (c *metricClient) RespondActivityTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RespondActivityTaskCompletedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRespondActivityTaskCompleted")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondActivityTaskCompleted(ctx, request, opts...)
}

func (c *metricClient) RespondActivityTaskCompletedById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedByIdRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RespondActivityTaskCompletedByIdResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRespondActivityTaskCompletedById")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondActivityTaskCompletedById(ctx, request, opts...)
}

func (c *metricClient) RespondActivityTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RespondActivityTaskFailedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRespondActivityTaskFailed")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondActivityTaskFailed(ctx, request, opts...)
}

func (c *metricClient) RespondActivityTaskFailedById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedByIdRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RespondActivityTaskFailedByIdResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRespondActivityTaskFailedById")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondActivityTaskFailedById(ctx, request, opts...)
}

func (c *metricClient) RespondNexusTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondNexusTaskCompletedRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RespondNexusTaskCompletedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRespondNexusTaskCompleted")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondNexusTaskCompleted(ctx, request, opts...)
}

func (c *metricClient) RespondNexusTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondNexusTaskFailedRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RespondNexusTaskFailedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRespondNexusTaskFailed")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondNexusTaskFailed(ctx, request, opts...)
}

func (c *metricClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondQueryTaskCompletedRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RespondQueryTaskCompletedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRespondQueryTaskCompleted")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondQueryTaskCompleted(ctx, request, opts...)
}

func (c *metricClient) RespondWorkflowTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RespondWorkflowTaskCompletedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRespondWorkflowTaskCompleted")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondWorkflowTaskCompleted(ctx, request, opts...)
}

func (c *metricClient) RespondWorkflowTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskFailedRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.RespondWorkflowTaskFailedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientRespondWorkflowTaskFailed")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondWorkflowTaskFailed(ctx, request, opts...)
}

func (c *metricClient) ScanWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ScanWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ScanWorkflowExecutionsResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientScanWorkflowExecutions")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ScanWorkflowExecutions(ctx, request, opts...)
}

func (c *metricClient) ShutdownWorker(
	ctx context.Context,
	request *workflowservice.ShutdownWorkerRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.ShutdownWorkerResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientShutdownWorker")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ShutdownWorker(ctx, request, opts...)
}

func (c *metricClient) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.SignalWithStartWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientSignalWithStartWorkflowExecution")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.SignalWithStartWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) SignalWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.SignalWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientSignalWorkflowExecution")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.SignalWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) StartBatchOperation(
	ctx context.Context,
	request *workflowservice.StartBatchOperationRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.StartBatchOperationResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientStartBatchOperation")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.StartBatchOperation(ctx, request, opts...)
}

func (c *metricClient) StartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.StartWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientStartWorkflowExecution")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.StartWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) StopBatchOperation(
	ctx context.Context,
	request *workflowservice.StopBatchOperationRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.StopBatchOperationResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientStopBatchOperation")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.StopBatchOperation(ctx, request, opts...)
}

func (c *metricClient) TerminateWorkflowExecution(
	ctx context.Context,
	request *workflowservice.TerminateWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.TerminateWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientTerminateWorkflowExecution")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.TerminateWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) UpdateNamespace(
	ctx context.Context,
	request *workflowservice.UpdateNamespaceRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.UpdateNamespaceResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientUpdateNamespace")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.UpdateNamespace(ctx, request, opts...)
}

func (c *metricClient) UpdateSchedule(
	ctx context.Context,
	request *workflowservice.UpdateScheduleRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.UpdateScheduleResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientUpdateSchedule")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.UpdateSchedule(ctx, request, opts...)
}

func (c *metricClient) UpdateWorkerBuildIdCompatibility(
	ctx context.Context,
	request *workflowservice.UpdateWorkerBuildIdCompatibilityRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.UpdateWorkerBuildIdCompatibilityResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientUpdateWorkerBuildIdCompatibility")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.UpdateWorkerBuildIdCompatibility(ctx, request, opts...)
}

func (c *metricClient) UpdateWorkerVersioningRules(
	ctx context.Context,
	request *workflowservice.UpdateWorkerVersioningRulesRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.UpdateWorkerVersioningRulesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientUpdateWorkerVersioningRules")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.UpdateWorkerVersioningRules(ctx, request, opts...)
}

func (c *metricClient) UpdateWorkflowExecution(
	ctx context.Context,
	request *workflowservice.UpdateWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *workflowservice.UpdateWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "FrontendClientUpdateWorkflowExecution")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.UpdateWorkflowExecution(ctx, request, opts...)
}
