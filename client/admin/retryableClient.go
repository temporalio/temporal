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

package admin

import (
	"context"

	"go.uber.org/yarpc"

	"github.com/temporalio/temporal/.gen/go/replicator"
	"github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/.gen/proto/adminservice"
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

func (c *retryableClient) AddSearchAttribute(
	ctx context.Context,
	request *adminservice.AddSearchAttributeRequest,
	opts ...yarpc.CallOption,
) (*adminservice.AddSearchAttributeResponse, error) {

	var resp *adminservice.AddSearchAttributeResponse
	op := func() error {
		var err error
		resp, err = c.client.AddSearchAttribute(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeHistoryHost(
	ctx context.Context,
	request *adminservice.DescribeHistoryHostRequest,
	opts ...yarpc.CallOption,
) (*adminservice.DescribeHistoryHostResponse, error) {

	var resp *adminservice.DescribeHistoryHostResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeHistoryHost(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RemoveTask(
	ctx context.Context,
	request *adminservice.RemoveTaskRequest,
	opts ...yarpc.CallOption,
) (*adminservice.RemoveTaskResponse, error) {

	var resp *adminservice.RemoveTaskResponse
	op := func() error {
		var err error
		resp, err = c.client.RemoveTask(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) CloseShard(
	ctx context.Context,
	request *adminservice.CloseShardRequest,
	opts ...yarpc.CallOption,
) (*adminservice.CloseShardResponse, error) {

	var resp *adminservice.CloseShardResponse
	op := func() error {
		var err error
		resp, err = c.client.CloseShard(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeWorkflowExecution(
	ctx context.Context,
	request *adminservice.DescribeWorkflowExecutionRequest,
	opts ...yarpc.CallOption,
) (*adminservice.DescribeWorkflowExecutionResponse, error) {

	var resp *adminservice.DescribeWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryRequest,
	opts ...yarpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryResponse, error) {

	var resp *adminservice.GetWorkflowExecutionRawHistoryResponse
	op := func() error {
		var err error
		resp, err = c.client.GetWorkflowExecutionRawHistory(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	opts ...yarpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {

	var resp *adminservice.GetWorkflowExecutionRawHistoryV2Response
	op := func() error {
		var err error
		resp, err = c.client.GetWorkflowExecutionRawHistoryV2(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeCluster(
	ctx context.Context,
	request *adminservice.DescribeClusterRequest,
	opts ...yarpc.CallOption,
) (*adminservice.DescribeClusterResponse, error) {

	var resp *adminservice.DescribeClusterResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeCluster(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetReplicationMessages(
	ctx context.Context,
	request *replicator.GetReplicationMessagesRequest,
	opts ...yarpc.CallOption,
) (*replicator.GetReplicationMessagesResponse, error) {
	var resp *replicator.GetReplicationMessagesResponse
	op := func() error {
		var err error
		resp, err = c.client.GetReplicationMessages(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetDomainReplicationMessages(
	ctx context.Context,
	request *replicator.GetDomainReplicationMessagesRequest,
	opts ...yarpc.CallOption,
) (*replicator.GetDomainReplicationMessagesResponse, error) {
	var resp *replicator.GetDomainReplicationMessagesResponse
	op := func() error {
		var err error
		resp, err = c.client.GetDomainReplicationMessages(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetDLQReplicationMessages(
	ctx context.Context,
	request *replicator.GetDLQReplicationMessagesRequest,
	opts ...yarpc.CallOption,
) (*replicator.GetDLQReplicationMessagesResponse, error) {
	var resp *replicator.GetDLQReplicationMessagesResponse
	op := func() error {
		var err error
		resp, err = c.client.GetDLQReplicationMessages(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ReapplyEvents(
	ctx context.Context,
	request *shared.ReapplyEventsRequest,
	opts ...yarpc.CallOption,
) error {

	op := func() error {
		return c.client.ReapplyEvents(ctx, request, opts...)
	}
	return backoff.Retry(op, c.policy, c.isRetryable)
}
