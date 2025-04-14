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

package admin

import (
	"context"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/grpc"
)

var _ adminservice.AdminServiceClient = (*retryableClient)(nil)

type retryableClient struct {
	client      adminservice.AdminServiceClient
	policy      backoff.RetryPolicy
	isRetryable backoff.IsRetryable
}

// NewRetryableClient creates a new instance of adminservice.AdminServiceClient with retry policy
func NewRetryableClient(client adminservice.AdminServiceClient, policy backoff.RetryPolicy, isRetryable backoff.IsRetryable) adminservice.AdminServiceClient {
	return &retryableClient{
		client:      client,
		policy:      policy,
		isRetryable: isRetryable,
	}
}

func (c *retryableClient) StreamWorkflowReplicationMessages(
	ctx context.Context,
	opts ...grpc.CallOption,
) (adminservice.AdminService_StreamWorkflowReplicationMessagesClient, error) {
	var resp adminservice.AdminService_StreamWorkflowReplicationMessagesClient
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.StreamWorkflowReplicationMessages(ctx, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}
