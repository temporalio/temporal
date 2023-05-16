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

	"go.temporal.io/server/common/backoff"
)

func (c *retryableClient) CountWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.CountWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	var resp *workflowservice.CountWorkflowExecutionsResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.CountWorkflowExecutions(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) CreateSchedule(
	ctx context.Context,
	request *workflowservice.CreateScheduleRequest,
	opts ...grpc.CallOption,
) (*workflowservice.CreateScheduleResponse, error) {
	var resp *workflowservice.CreateScheduleResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.CreateSchedule(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DeleteSchedule(
	ctx context.Context,
	request *workflowservice.DeleteScheduleRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DeleteScheduleResponse, error) {
	var resp *workflowservice.DeleteScheduleResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DeleteSchedule(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DeleteWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DeleteWorkflowExecutionResponse, error) {
	var resp *workflowservice.DeleteWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DeleteWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DeprecateNamespace(
	ctx context.Context,
	request *workflowservice.DeprecateNamespaceRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DeprecateNamespaceResponse, error) {
	var resp *workflowservice.DeprecateNamespaceResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DeprecateNamespace(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeBatchOperation(
	ctx context.Context,
	request *workflowservice.DescribeBatchOperationRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeBatchOperationResponse, error) {
	var resp *workflowservice.DescribeBatchOperationResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DescribeBatchOperation(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeNamespace(
	ctx context.Context,
	request *workflowservice.DescribeNamespaceRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeNamespaceResponse, error) {
	var resp *workflowservice.DescribeNamespaceResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DescribeNamespace(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeSchedule(
	ctx context.Context,
	request *workflowservice.DescribeScheduleRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeScheduleResponse, error) {
	var resp *workflowservice.DescribeScheduleResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DescribeSchedule(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeTaskQueue(
	ctx context.Context,
	request *workflowservice.DescribeTaskQueueRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeTaskQueueResponse, error) {
	var resp *workflowservice.DescribeTaskQueueResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DescribeTaskQueue(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	var resp *workflowservice.DescribeWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.DescribeWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetClusterInfo(
	ctx context.Context,
	request *workflowservice.GetClusterInfoRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetClusterInfoResponse, error) {
	var resp *workflowservice.GetClusterInfoResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetClusterInfo(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetSearchAttributes(
	ctx context.Context,
	request *workflowservice.GetSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetSearchAttributesResponse, error) {
	var resp *workflowservice.GetSearchAttributesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetSearchAttributes(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetSystemInfo(
	ctx context.Context,
	request *workflowservice.GetSystemInfoRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetSystemInfoResponse, error) {
	var resp *workflowservice.GetSystemInfoResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetSystemInfo(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkerBuildIdCompatibility(
	ctx context.Context,
	request *workflowservice.GetWorkerBuildIdCompatibilityRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkerBuildIdCompatibilityResponse, error) {
	var resp *workflowservice.GetWorkerBuildIdCompatibilityResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetWorkerBuildIdCompatibility(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkerTaskReachability(
	ctx context.Context,
	request *workflowservice.GetWorkerTaskReachabilityRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkerTaskReachabilityResponse, error) {
	var resp *workflowservice.GetWorkerTaskReachabilityResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetWorkerTaskReachability(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	var resp *workflowservice.GetWorkflowExecutionHistoryResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetWorkflowExecutionHistory(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkflowExecutionHistoryReverse(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryReverseRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	var resp *workflowservice.GetWorkflowExecutionHistoryReverseResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetWorkflowExecutionHistoryReverse(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListArchivedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListArchivedWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	var resp *workflowservice.ListArchivedWorkflowExecutionsResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListArchivedWorkflowExecutions(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListBatchOperations(
	ctx context.Context,
	request *workflowservice.ListBatchOperationsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListBatchOperationsResponse, error) {
	var resp *workflowservice.ListBatchOperationsResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListBatchOperations(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListClosedWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	var resp *workflowservice.ListClosedWorkflowExecutionsResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListClosedWorkflowExecutions(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListNamespaces(
	ctx context.Context,
	request *workflowservice.ListNamespacesRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListNamespacesResponse, error) {
	var resp *workflowservice.ListNamespacesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListNamespaces(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListOpenWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	var resp *workflowservice.ListOpenWorkflowExecutionsResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListOpenWorkflowExecutions(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListScheduleMatchingTimes(
	ctx context.Context,
	request *workflowservice.ListScheduleMatchingTimesRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListScheduleMatchingTimesResponse, error) {
	var resp *workflowservice.ListScheduleMatchingTimesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListScheduleMatchingTimes(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListSchedules(
	ctx context.Context,
	request *workflowservice.ListSchedulesRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListSchedulesResponse, error) {
	var resp *workflowservice.ListSchedulesResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListSchedules(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListTaskQueuePartitions(
	ctx context.Context,
	request *workflowservice.ListTaskQueuePartitionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListTaskQueuePartitionsResponse, error) {
	var resp *workflowservice.ListTaskQueuePartitionsResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListTaskQueuePartitions(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	var resp *workflowservice.ListWorkflowExecutionsResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ListWorkflowExecutions(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PatchSchedule(
	ctx context.Context,
	request *workflowservice.PatchScheduleRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PatchScheduleResponse, error) {
	var resp *workflowservice.PatchScheduleResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.PatchSchedule(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollActivityTaskQueue(
	ctx context.Context,
	request *workflowservice.PollActivityTaskQueueRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollActivityTaskQueueResponse, error) {
	var resp *workflowservice.PollActivityTaskQueueResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.PollActivityTaskQueue(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollWorkflowExecutionUpdate(
	ctx context.Context,
	request *workflowservice.PollWorkflowExecutionUpdateRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
	var resp *workflowservice.PollWorkflowExecutionUpdateResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.PollWorkflowExecutionUpdate(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollWorkflowTaskQueue(
	ctx context.Context,
	request *workflowservice.PollWorkflowTaskQueueRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	var resp *workflowservice.PollWorkflowTaskQueueResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.PollWorkflowTaskQueue(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) QueryWorkflow(
	ctx context.Context,
	request *workflowservice.QueryWorkflowRequest,
	opts ...grpc.CallOption,
) (*workflowservice.QueryWorkflowResponse, error) {
	var resp *workflowservice.QueryWorkflowResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.QueryWorkflow(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RecordActivityTaskHeartbeatResponse, error) {
	var resp *workflowservice.RecordActivityTaskHeartbeatResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RecordActivityTaskHeartbeat(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RecordActivityTaskHeartbeatById(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatByIdRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RecordActivityTaskHeartbeatByIdResponse, error) {
	var resp *workflowservice.RecordActivityTaskHeartbeatByIdResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RecordActivityTaskHeartbeatById(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RegisterNamespace(
	ctx context.Context,
	request *workflowservice.RegisterNamespaceRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RegisterNamespaceResponse, error) {
	var resp *workflowservice.RegisterNamespaceResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RegisterNamespace(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *workflowservice.RequestCancelWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
	var resp *workflowservice.RequestCancelWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RequestCancelWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ResetStickyTaskQueue(
	ctx context.Context,
	request *workflowservice.ResetStickyTaskQueueRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ResetStickyTaskQueueResponse, error) {
	var resp *workflowservice.ResetStickyTaskQueueResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ResetStickyTaskQueue(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ResetWorkflowExecution(
	ctx context.Context,
	request *workflowservice.ResetWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	var resp *workflowservice.ResetWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ResetWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondActivityTaskCanceled(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCanceledResponse, error) {
	var resp *workflowservice.RespondActivityTaskCanceledResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RespondActivityTaskCanceled(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondActivityTaskCanceledById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledByIdRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCanceledByIdResponse, error) {
	var resp *workflowservice.RespondActivityTaskCanceledByIdResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RespondActivityTaskCanceledById(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondActivityTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	var resp *workflowservice.RespondActivityTaskCompletedResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RespondActivityTaskCompleted(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondActivityTaskCompletedById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedByIdRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCompletedByIdResponse, error) {
	var resp *workflowservice.RespondActivityTaskCompletedByIdResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RespondActivityTaskCompletedById(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondActivityTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskFailedResponse, error) {
	var resp *workflowservice.RespondActivityTaskFailedResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RespondActivityTaskFailed(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondActivityTaskFailedById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedByIdRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskFailedByIdResponse, error) {
	var resp *workflowservice.RespondActivityTaskFailedByIdResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RespondActivityTaskFailedById(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondQueryTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondQueryTaskCompletedResponse, error) {
	var resp *workflowservice.RespondQueryTaskCompletedResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RespondQueryTaskCompleted(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondWorkflowTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	var resp *workflowservice.RespondWorkflowTaskCompletedResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RespondWorkflowTaskCompleted(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondWorkflowTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskFailedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondWorkflowTaskFailedResponse, error) {
	var resp *workflowservice.RespondWorkflowTaskFailedResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.RespondWorkflowTaskFailed(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ScanWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ScanWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
	var resp *workflowservice.ScanWorkflowExecutionsResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.ScanWorkflowExecutions(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.SignalWithStartWorkflowExecutionResponse, error) {
	var resp *workflowservice.SignalWithStartWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.SignalWithStartWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) SignalWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.SignalWorkflowExecutionResponse, error) {
	var resp *workflowservice.SignalWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.SignalWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) StartBatchOperation(
	ctx context.Context,
	request *workflowservice.StartBatchOperationRequest,
	opts ...grpc.CallOption,
) (*workflowservice.StartBatchOperationResponse, error) {
	var resp *workflowservice.StartBatchOperationResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.StartBatchOperation(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) StartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	var resp *workflowservice.StartWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.StartWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) StopBatchOperation(
	ctx context.Context,
	request *workflowservice.StopBatchOperationRequest,
	opts ...grpc.CallOption,
) (*workflowservice.StopBatchOperationResponse, error) {
	var resp *workflowservice.StopBatchOperationResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.StopBatchOperation(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) TerminateWorkflowExecution(
	ctx context.Context,
	request *workflowservice.TerminateWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.TerminateWorkflowExecutionResponse, error) {
	var resp *workflowservice.TerminateWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.TerminateWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) UpdateNamespace(
	ctx context.Context,
	request *workflowservice.UpdateNamespaceRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateNamespaceResponse, error) {
	var resp *workflowservice.UpdateNamespaceResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.UpdateNamespace(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) UpdateSchedule(
	ctx context.Context,
	request *workflowservice.UpdateScheduleRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateScheduleResponse, error) {
	var resp *workflowservice.UpdateScheduleResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.UpdateSchedule(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) UpdateWorkerBuildIdCompatibility(
	ctx context.Context,
	request *workflowservice.UpdateWorkerBuildIdCompatibilityRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateWorkerBuildIdCompatibilityResponse, error) {
	var resp *workflowservice.UpdateWorkerBuildIdCompatibilityResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.UpdateWorkerBuildIdCompatibility(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) UpdateWorkflowExecution(
	ctx context.Context,
	request *workflowservice.UpdateWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
	var resp *workflowservice.UpdateWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.UpdateWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}
