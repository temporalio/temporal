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

package interceptor

import (
	"context"

	"google.golang.org/grpc"

	"go.temporal.io/server/common/backoff"
)

type (
	RetryableInterceptor struct {
		policy      backoff.RetryPolicy
		isRetryable backoff.IsRetryable
	}
)

var _ grpc.UnaryServerInterceptor = (*RetryableInterceptor)(nil).Intercept

func NewRetryableInterceptor(
	policy backoff.RetryPolicy,
	isRetryable backoff.IsRetryable,
) *RetryableInterceptor {
	return &RetryableInterceptor{
		policy:      policy,
		isRetryable: isRetryable,
	}
}

func (i *RetryableInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	var response interface{}
	op := func(ctx context.Context) error {
		var err error
		response, err = handler(ctx, req)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, i.policy, i.isRetryable)
	return response, err
}
