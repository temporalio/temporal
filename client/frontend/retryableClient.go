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

	"go.uber.org/yarpc"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/backoff"
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
	request *shared.DeprecateDomainRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.DeprecateDomain(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) DescribeDomain(
	ctx context.Context,
	request *shared.DescribeDomainRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeDomainResponse, error) {

	var resp *shared.DescribeDomainResponse
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
	request *shared.DescribeTaskListRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeTaskListResponse, error) {

	var resp *shared.DescribeTaskListResponse
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
	request *shared.DescribeWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.DescribeWorkflowExecutionResponse, error) {

	var resp *shared.DescribeWorkflowExecutionResponse
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
	request *shared.GetWorkflowExecutionHistoryRequest,
	opts ...yarpc.CallOption,
) (*shared.GetWorkflowExecutionHistoryResponse, error) {

	var resp *shared.GetWorkflowExecutionHistoryResponse
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
	request *shared.GetWorkflowExecutionRawHistoryRequest,
	opts ...yarpc.CallOption,
) (*shared.GetWorkflowExecutionRawHistoryResponse, error) {

	var resp *shared.GetWorkflowExecutionRawHistoryResponse
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
	request *shared.PollForWorkflowExecutionRawHistoryRequest,
	opts ...yarpc.CallOption,
) (*shared.PollForWorkflowExecutionRawHistoryResponse, error) {

	var resp *shared.PollForWorkflowExecutionRawHistoryResponse
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
	request *shared.ListArchivedWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListArchivedWorkflowExecutionsResponse, error) {

	var resp *shared.ListArchivedWorkflowExecutionsResponse
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
	request *shared.ListClosedWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListClosedWorkflowExecutionsResponse, error) {

	var resp *shared.ListClosedWorkflowExecutionsResponse
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
	request *shared.ListDomainsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListDomainsResponse, error) {

	var resp *shared.ListDomainsResponse
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
	request *shared.ListOpenWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListOpenWorkflowExecutionsResponse, error) {

	var resp *shared.ListOpenWorkflowExecutionsResponse
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
	request *shared.ListWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListWorkflowExecutionsResponse, error) {

	var resp *shared.ListWorkflowExecutionsResponse
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
	request *shared.ListWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListWorkflowExecutionsResponse, error) {

	var resp *shared.ListWorkflowExecutionsResponse
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
	request *shared.CountWorkflowExecutionsRequest,
	opts ...yarpc.CallOption,
) (*shared.CountWorkflowExecutionsResponse, error) {

	var resp *shared.CountWorkflowExecutionsResponse
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
	opts ...yarpc.CallOption,
) (*shared.GetSearchAttributesResponse, error) {

	var resp *shared.GetSearchAttributesResponse
	op := func() error {
		var err error
		resp, err = c.client.GetSearchAttributes(ctx, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollForActivityTask(
	ctx context.Context,
	request *shared.PollForActivityTaskRequest,
	opts ...yarpc.CallOption,
) (*shared.PollForActivityTaskResponse, error) {

	var resp *shared.PollForActivityTaskResponse
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
	request *shared.PollForDecisionTaskRequest,
	opts ...yarpc.CallOption,
) (*shared.PollForDecisionTaskResponse, error) {

	var resp *shared.PollForDecisionTaskResponse
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
	request *shared.QueryWorkflowRequest,
	opts ...yarpc.CallOption,
) (*shared.QueryWorkflowResponse, error) {

	var resp *shared.QueryWorkflowResponse
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
	request *shared.RecordActivityTaskHeartbeatRequest,
	opts ...yarpc.CallOption,
) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	var resp *shared.RecordActivityTaskHeartbeatResponse
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
	request *shared.RecordActivityTaskHeartbeatByIDRequest,
	opts ...yarpc.CallOption,
) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	var resp *shared.RecordActivityTaskHeartbeatResponse
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
	request *shared.RegisterDomainRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.RegisterDomain(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *shared.RequestCancelWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.RequestCancelWorkflowExecution(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) ResetStickyTaskList(
	ctx context.Context,
	request *shared.ResetStickyTaskListRequest,
	opts ...yarpc.CallOption,
) (*shared.ResetStickyTaskListResponse, error) {

	var resp *shared.ResetStickyTaskListResponse
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
	request *shared.ResetWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.ResetWorkflowExecutionResponse, error) {

	var resp *shared.ResetWorkflowExecutionResponse
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
	request *shared.RespondActivityTaskCanceledRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.RespondActivityTaskCanceled(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondActivityTaskCanceledByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCanceledByIDRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.RespondActivityTaskCanceledByID(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondActivityTaskCompleted(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.RespondActivityTaskCompleted(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondActivityTaskCompletedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedByIDRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.RespondActivityTaskCompletedByID(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondActivityTaskFailed(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.RespondActivityTaskFailed(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondActivityTaskFailedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedByIDRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.RespondActivityTaskFailedByID(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *shared.RespondDecisionTaskCompletedRequest,
	opts ...yarpc.CallOption,
) (*shared.RespondDecisionTaskCompletedResponse, error) {

	var resp *shared.RespondDecisionTaskCompletedResponse
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
	request *shared.RespondDecisionTaskFailedRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.RespondDecisionTaskFailed(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *shared.RespondQueryTaskCompletedRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.RespondQueryTaskCompleted(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *shared.SignalWithStartWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.StartWorkflowExecutionResponse, error) {

	var resp *shared.StartWorkflowExecutionResponse
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
	request *shared.SignalWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.SignalWorkflowExecution(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) StartWorkflowExecution(
	ctx context.Context,
	request *shared.StartWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*shared.StartWorkflowExecutionResponse, error) {

	var resp *shared.StartWorkflowExecutionResponse
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
	request *shared.TerminateWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.TerminateWorkflowExecution(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) UpdateDomain(
	ctx context.Context,
	request *shared.UpdateDomainRequest,
	opts ...yarpc.CallOption,
) (*shared.UpdateDomainResponse, error) {

	var resp *shared.UpdateDomainResponse
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
	opts ...yarpc.CallOption,
) (*shared.ClusterInfo, error) {
	var resp *shared.ClusterInfo
	op := func() error {
		var err error
		resp, err = c.client.GetClusterInfo(ctx, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListTaskListPartitions(
	ctx context.Context,
	request *shared.ListTaskListPartitionsRequest,
	opts ...yarpc.CallOption,
) (*shared.ListTaskListPartitionsResponse, error) {
	var resp *shared.ListTaskListPartitionsResponse
	op := func() error {
		var err error
		resp, err = c.client.ListTaskListPartitions(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}
