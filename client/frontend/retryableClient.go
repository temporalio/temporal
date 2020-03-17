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

package frontend

import (
	"context"

	"go.temporal.io/temporal-proto/workflowservice"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal/common/backoff"
)

var _ Client = (*retryableClient)(nil)

type retryableClient struct {
	client      Client
	policy      backoff.RetryPolicy
	isRetryable backoff.IsRetryable
}

// NewRetryableClient creates a new instance of Client with retry policy
func NewRetryableClient(client Client, policy backoff.RetryPolicy, isRetryable backoff.IsRetryable) Client {
	return &retryableClient{
		client:      client,
		policy:      policy,
		isRetryable: isRetryable,
	}
}

func (c *retryableClient) DeprecateDomain(
	ctx context.Context,
	request *workflowservice.DeprecateDomainRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DeprecateDomainResponse, error) {
	var resp *workflowservice.DeprecateDomainResponse
	op := func() error {
		var err error
		resp, err = c.client.DeprecateDomain(ctx, request, opts...)
		return err
	}

	return resp, backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) DescribeDomain(
	ctx context.Context,
	request *workflowservice.DescribeDomainRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeDomainResponse, error) {
	var resp *workflowservice.DescribeDomainResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeDomain(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeTaskList(
	ctx context.Context,
	request *workflowservice.DescribeTaskListRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeTaskListResponse, error) {
	var resp *workflowservice.DescribeTaskListResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeTaskList(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	var resp *workflowservice.DescribeWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	var resp *workflowservice.GetWorkflowExecutionHistoryResponse
	op := func() error {
		var err error
		resp, err = c.client.GetWorkflowExecutionHistory(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionRawHistoryRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionRawHistoryResponse, error) {
	var resp *workflowservice.GetWorkflowExecutionRawHistoryResponse
	op := func() error {
		var err error
		resp, err = c.client.GetWorkflowExecutionRawHistory(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollForWorkflowExecutionRawHistory(
	ctx context.Context,
	request *workflowservice.PollForWorkflowExecutionRawHistoryRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollForWorkflowExecutionRawHistoryResponse, error) {

	var resp *workflowservice.PollForWorkflowExecutionRawHistoryResponse
	op := func() error {
		var err error
		resp, err = c.client.PollForWorkflowExecutionRawHistory(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListArchivedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListArchivedWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	var resp *workflowservice.ListArchivedWorkflowExecutionsResponse
	op := func() error {
		var err error
		resp, err = c.client.ListArchivedWorkflowExecutions(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListClosedWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	var resp *workflowservice.ListClosedWorkflowExecutionsResponse
	op := func() error {
		var err error
		resp, err = c.client.ListClosedWorkflowExecutions(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListDomains(
	ctx context.Context,
	request *workflowservice.ListDomainsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListDomainsResponse, error) {
	var resp *workflowservice.ListDomainsResponse
	op := func() error {
		var err error
		resp, err = c.client.ListDomains(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListOpenWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	var resp *workflowservice.ListOpenWorkflowExecutionsResponse
	op := func() error {
		var err error
		resp, err = c.client.ListOpenWorkflowExecutions(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	var resp *workflowservice.ListWorkflowExecutionsResponse
	op := func() error {
		var err error
		resp, err = c.client.ListWorkflowExecutions(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ScanWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ScanWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
	var resp *workflowservice.ScanWorkflowExecutionsResponse
	op := func() error {
		var err error
		resp, err = c.client.ScanWorkflowExecutions(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) CountWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.CountWorkflowExecutionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	var resp *workflowservice.CountWorkflowExecutionsResponse
	op := func() error {
		var err error
		resp, err = c.client.CountWorkflowExecutions(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetSearchAttributes(
	ctx context.Context,
	request *workflowservice.GetSearchAttributesRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetSearchAttributesResponse, error) {
	var resp *workflowservice.GetSearchAttributesResponse
	op := func() error {
		var err error
		resp, err = c.client.GetSearchAttributes(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollForActivityTask(
	ctx context.Context,
	request *workflowservice.PollForActivityTaskRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollForActivityTaskResponse, error) {
	var resp *workflowservice.PollForActivityTaskResponse
	op := func() error {
		var err error
		resp, err = c.client.PollForActivityTask(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollForDecisionTask(
	ctx context.Context,
	request *workflowservice.PollForDecisionTaskRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollForDecisionTaskResponse, error) {
	var resp *workflowservice.PollForDecisionTaskResponse
	op := func() error {
		var err error
		resp, err = c.client.PollForDecisionTask(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) QueryWorkflow(
	ctx context.Context,
	request *workflowservice.QueryWorkflowRequest,
	opts ...grpc.CallOption,
) (*workflowservice.QueryWorkflowResponse, error) {
	var resp *workflowservice.QueryWorkflowResponse
	op := func() error {
		var err error
		resp, err = c.client.QueryWorkflow(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RecordActivityTaskHeartbeatResponse, error) {
	var resp *workflowservice.RecordActivityTaskHeartbeatResponse
	op := func() error {
		var err error
		resp, err = c.client.RecordActivityTaskHeartbeat(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatByIDRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RecordActivityTaskHeartbeatByIDResponse, error) {
	var resp *workflowservice.RecordActivityTaskHeartbeatByIDResponse
	op := func() error {
		var err error
		resp, err = c.client.RecordActivityTaskHeartbeatByID(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RegisterDomain(
	ctx context.Context,
	request *workflowservice.RegisterDomainRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RegisterDomainResponse, error) {
	var resp *workflowservice.RegisterDomainResponse
	op := func() error {
		var err error
		resp, err = c.client.RegisterDomain(ctx, request, opts...)
		return err
	}

	return resp, backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *workflowservice.RequestCancelWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
	var resp *workflowservice.RequestCancelWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.RequestCancelWorkflowExecution(ctx, request, opts...)
		return err
	}

	return resp, backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) ResetStickyTaskList(
	ctx context.Context,
	request *workflowservice.ResetStickyTaskListRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ResetStickyTaskListResponse, error) {
	var resp *workflowservice.ResetStickyTaskListResponse
	op := func() error {
		var err error
		resp, err = c.client.ResetStickyTaskList(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ResetWorkflowExecution(
	ctx context.Context,
	request *workflowservice.ResetWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	var resp *workflowservice.ResetWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.ResetWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondActivityTaskCanceled(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCanceledResponse, error) {
	var resp *workflowservice.RespondActivityTaskCanceledResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondActivityTaskCanceled(ctx, request, opts...)
		return err
	}

	return resp, backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondActivityTaskCanceledByID(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledByIDRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCanceledByIDResponse, error) {
	var resp *workflowservice.RespondActivityTaskCanceledByIDResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondActivityTaskCanceledByID(ctx, request, opts...)
		return err
	}

	return resp, backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondActivityTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	var resp *workflowservice.RespondActivityTaskCompletedResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondActivityTaskCompleted(ctx, request, opts...)
		return err
	}

	return resp, backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondActivityTaskCompletedByID(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedByIDRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCompletedByIDResponse, error) {
	var resp *workflowservice.RespondActivityTaskCompletedByIDResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondActivityTaskCompletedByID(ctx, request, opts...)
		return err
	}

	return resp, backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondActivityTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskFailedResponse, error) {
	var resp *workflowservice.RespondActivityTaskFailedResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondActivityTaskFailed(ctx, request, opts...)
		return err
	}

	return resp, backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondActivityTaskFailedByID(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedByIDRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskFailedByIDResponse, error) {
	var resp *workflowservice.RespondActivityTaskFailedByIDResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondActivityTaskFailedByID(ctx, request, opts...)
		return err
	}

	return resp, backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondDecisionTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondDecisionTaskCompletedResponse, error) {
	var resp *workflowservice.RespondDecisionTaskCompletedResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondDecisionTaskCompleted(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondDecisionTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondDecisionTaskFailedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondDecisionTaskFailedResponse, error) {
	var resp *workflowservice.RespondDecisionTaskFailedResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondDecisionTaskFailed(ctx, request, opts...)
		return err
	}

	return resp, backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondQueryTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondQueryTaskCompletedResponse, error) {
	var resp *workflowservice.RespondQueryTaskCompletedResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondQueryTaskCompleted(ctx, request, opts...)
		return err
	}

	return resp, backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.SignalWithStartWorkflowExecutionResponse, error) {
	var resp *workflowservice.SignalWithStartWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.SignalWithStartWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) SignalWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.SignalWorkflowExecutionResponse, error) {
	var resp *workflowservice.SignalWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.SignalWorkflowExecution(ctx, request, opts...)
		return err
	}

	return resp, backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) StartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	var resp *workflowservice.StartWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.StartWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) TerminateWorkflowExecution(
	ctx context.Context,
	request *workflowservice.TerminateWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*workflowservice.TerminateWorkflowExecutionResponse, error) {
	var resp *workflowservice.TerminateWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.TerminateWorkflowExecution(ctx, request, opts...)
		return err
	}

	return resp, backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) UpdateDomain(
	ctx context.Context,
	request *workflowservice.UpdateDomainRequest,
	opts ...grpc.CallOption,
) (*workflowservice.UpdateDomainResponse, error) {
	var resp *workflowservice.UpdateDomainResponse
	op := func() error {
		var err error
		resp, err = c.client.UpdateDomain(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetClusterInfo(
	ctx context.Context,
	request *workflowservice.GetClusterInfoRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetClusterInfoResponse, error) {
	var resp *workflowservice.GetClusterInfoResponse
	op := func() error {
		var err error
		resp, err = c.client.GetClusterInfo(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListTaskListPartitions(
	ctx context.Context,
	request *workflowservice.ListTaskListPartitionsRequest,
	opts ...grpc.CallOption,
) (*workflowservice.ListTaskListPartitionsResponse, error) {
	var resp *workflowservice.ListTaskListPartitionsResponse
	op := func() error {
		var err error
		resp, err = c.client.ListTaskListPartitions(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}
