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
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/quotas"
)

const (
	RateLimitDefaultToken = 1
)

var (
	RateLimitServerBusy = serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, "service rate limit exceeded")
)

type (
	RateLimitInterceptor struct {
		rateLimiter quotas.RequestRateLimiter
		tokens      map[string]int
	}
)

var _ grpc.UnaryServerInterceptor = (*RateLimitInterceptor)(nil).Intercept

func NewRateLimitInterceptor(
	rateLimiter quotas.RequestRateLimiter,
	tokens map[string]int,
) *RateLimitInterceptor {
	return &RateLimitInterceptor{
		rateLimiter: rateLimiter,
		tokens:      tokens,
	}
}

func (i *RateLimitInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	_, methodName := splitMethodName(info.FullMethod)
	token, ok := i.tokens[methodName]
	if !ok {
		token = RateLimitDefaultToken
	}

	if !i.rateLimiter.Allow(time.Now().UTC(), quotas.NewRequest(
		methodName,
		token,
		"", // this interceptor layer does not throttle based on caller
	)) {
		return nil, RateLimitServerBusy
	}
	return handler(ctx, req)
}
