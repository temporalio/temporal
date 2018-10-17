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

package history

import (
	"context"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/backoff"
	"go.uber.org/yarpc"
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

func (c *retryableClient) StartWorkflowExecution(
	ctx context.Context,
	request *h.StartWorkflowExecutionRequest,
	opts ...yarpc.CallOption) (*shared.StartWorkflowExecutionResponse, error) {

	var resp *shared.StartWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.StartWorkflowExecution(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeHistoryHost(
	ctx context.Context,
	request *shared.DescribeHistoryHostRequest,
	opts ...yarpc.CallOption) (*shared.DescribeHistoryHostResponse, error) {

	var resp *shared.DescribeHistoryHostResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeHistoryHost(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeMutableState(
	ctx context.Context,
	request *h.DescribeMutableStateRequest,
	opts ...yarpc.CallOption) (*h.DescribeMutableStateResponse, error) {

	var resp *h.DescribeMutableStateResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeMutableState(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetMutableState(
	ctx context.Context,
	request *h.GetMutableStateRequest,
	opts ...yarpc.CallOption) (*h.GetMutableStateResponse, error) {

	var resp *h.GetMutableStateResponse
	op := func() error {
		var err error
		resp, err = c.client.GetMutableState(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ResetStickyTaskList(
	ctx context.Context,
	request *h.ResetStickyTaskListRequest,
	opts ...yarpc.CallOption) (*h.ResetStickyTaskListResponse, error) {

	var resp *h.ResetStickyTaskListResponse
	op := func() error {
		var err error
		resp, err = c.client.ResetStickyTaskList(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeWorkflowExecution(
	ctx context.Context,
	request *h.DescribeWorkflowExecutionRequest,
	opts ...yarpc.CallOption) (*shared.DescribeWorkflowExecutionResponse, error) {

	var resp *shared.DescribeWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeWorkflowExecution(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RecordDecisionTaskStarted(
	ctx context.Context,
	request *h.RecordDecisionTaskStartedRequest,
	opts ...yarpc.CallOption) (*h.RecordDecisionTaskStartedResponse, error) {

	var resp *h.RecordDecisionTaskStartedResponse
	op := func() error {
		var err error
		resp, err = c.client.RecordDecisionTaskStarted(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RecordActivityTaskStarted(
	ctx context.Context,
	request *h.RecordActivityTaskStartedRequest,
	opts ...yarpc.CallOption) (*h.RecordActivityTaskStartedResponse, error) {

	var resp *h.RecordActivityTaskStartedResponse
	op := func() error {
		var err error
		resp, err = c.client.RecordActivityTaskStarted(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *h.RespondDecisionTaskCompletedRequest,
	opts ...yarpc.CallOption) (*h.RespondDecisionTaskCompletedResponse, error) {

	var resp *h.RespondDecisionTaskCompletedResponse
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
	request *h.RespondDecisionTaskFailedRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.RespondDecisionTaskFailed(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondActivityTaskCompleted(
	ctx context.Context,
	request *h.RespondActivityTaskCompletedRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.RespondActivityTaskCompleted(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondActivityTaskFailed(
	ctx context.Context,
	request *h.RespondActivityTaskFailedRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.RespondActivityTaskFailed(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RespondActivityTaskCanceled(
	ctx context.Context,
	request *h.RespondActivityTaskCanceledRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.RespondActivityTaskCanceled(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *h.RecordActivityTaskHeartbeatRequest,
	opts ...yarpc.CallOption) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	var resp *shared.RecordActivityTaskHeartbeatResponse
	op := func() error {
		var err error
		resp, err = c.client.RecordActivityTaskHeartbeat(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *h.RequestCancelWorkflowExecutionRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.RequestCancelWorkflowExecution(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) SignalWorkflowExecution(
	ctx context.Context,
	request *h.SignalWorkflowExecutionRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.SignalWorkflowExecution(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *h.SignalWithStartWorkflowExecutionRequest,
	opts ...yarpc.CallOption) (*shared.StartWorkflowExecutionResponse, error) {

	var resp *shared.StartWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.SignalWithStartWorkflowExecution(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RemoveSignalMutableState(
	ctx context.Context,
	request *h.RemoveSignalMutableStateRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.RemoveSignalMutableState(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) TerminateWorkflowExecution(
	ctx context.Context,
	request *h.TerminateWorkflowExecutionRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.TerminateWorkflowExecution(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) ScheduleDecisionTask(
	ctx context.Context,
	request *h.ScheduleDecisionTaskRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.ScheduleDecisionTask(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) RecordChildExecutionCompleted(
	ctx context.Context,
	request *h.RecordChildExecutionCompletedRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.RecordChildExecutionCompleted(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) ReplicateEvents(
	ctx context.Context,
	request *h.ReplicateEventsRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.ReplicateEvents(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) SyncShardStatus(
	ctx context.Context,
	request *h.SyncShardStatusRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.SyncShardStatus(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) SyncActivity(
	ctx context.Context,
	request *h.SyncActivityRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.SyncActivity(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}
