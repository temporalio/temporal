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

package matching

import (
	"context"

	"go.uber.org/yarpc"

	m "github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
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

func (c *retryableClient) AddActivityTask(
	ctx context.Context,
	addRequest *m.AddActivityTaskRequest,
	opts ...yarpc.CallOption) error {
	op := func() error {
		return c.client.AddActivityTask(ctx, addRequest, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) AddDecisionTask(
	ctx context.Context,
	addRequest *m.AddDecisionTaskRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.AddDecisionTask(ctx, addRequest, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) PollForActivityTask(
	ctx context.Context,
	pollRequest *m.PollForActivityTaskRequest,
	opts ...yarpc.CallOption) (*workflow.PollForActivityTaskResponse, error) {

	var resp *workflow.PollForActivityTaskResponse
	op := func() error {
		var err error
		resp, err = c.client.PollForActivityTask(ctx, pollRequest, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollForDecisionTask(
	ctx context.Context,
	pollRequest *m.PollForDecisionTaskRequest,
	opts ...yarpc.CallOption) (*m.PollForDecisionTaskResponse, error) {

	var resp *m.PollForDecisionTaskResponse
	op := func() error {
		var err error
		resp, err = c.client.PollForDecisionTask(ctx, pollRequest, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) QueryWorkflow(
	ctx context.Context,
	queryRequest *m.QueryWorkflowRequest,
	opts ...yarpc.CallOption) (*workflow.QueryWorkflowResponse, error) {

	var resp *workflow.QueryWorkflowResponse
	op := func() error {
		var err error
		resp, err = c.client.QueryWorkflow(ctx, queryRequest, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *m.RespondQueryTaskCompletedRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.RespondQueryTaskCompleted(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) CancelOutstandingPoll(
	ctx context.Context,
	request *m.CancelOutstandingPollRequest,
	opts ...yarpc.CallOption) error {

	op := func() error {
		return c.client.CancelOutstandingPoll(ctx, request, opts...)
	}

	return backoff.Retry(op, c.policy, c.isRetryable)
}

func (c *retryableClient) DescribeTaskList(
	ctx context.Context,
	request *m.DescribeTaskListRequest,
	opts ...yarpc.CallOption) (*workflow.DescribeTaskListResponse, error) {

	var resp *workflow.DescribeTaskListResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeTaskList(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListTaskListPartitions(
	ctx context.Context,
	request *m.ListTaskListPartitionsRequest,
	opts ...yarpc.CallOption) (*workflow.ListTaskListPartitionsResponse, error) {

	var resp *workflow.ListTaskListPartitionsResponse
	op := func() error {
		var err error
		resp, err = c.client.ListTaskListPartitions(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}
