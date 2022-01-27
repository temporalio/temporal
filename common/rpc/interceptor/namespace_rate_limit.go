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

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
)

const (
	NamespaceRateLimitDefaultToken = 1
)

var (
	ErrNamespaceRateLimitServerBusy = serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, "namespace rate limit exceeded")
)

type (
	NamespaceRateLimitInterceptor struct {
		namespaceRegistry namespace.Registry
		rateLimiter       quotas.RequestRateLimiter
		tokens            map[string]int
	}
)

var _ grpc.UnaryServerInterceptor = (*NamespaceRateLimitInterceptor)(nil).Intercept

func NewNamespaceRateLimitInterceptor(
	namespaceRegistry namespace.Registry,
	rateLimiter quotas.RequestRateLimiter,
	tokens map[string]int,
) *NamespaceRateLimitInterceptor {
	return &NamespaceRateLimitInterceptor{
		namespaceRegistry: namespaceRegistry,
		rateLimiter:       rateLimiter,
		tokens:            tokens,
	}
}

func (ni *NamespaceRateLimitInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	_, methodName := splitMethodName(info.FullMethod)
	token, ok := ni.tokens[methodName]
	if !ok {
		token = NamespaceRateLimitDefaultToken
	}

	namespace := GetNamespace(ni.namespaceRegistry, req)
	if !ni.rateLimiter.Allow(time.Now().UTC(), quotas.NewRequest(
		methodName,
		token,
		namespace.String(),
	)) {
		return nil, ErrNamespaceRateLimitServerBusy
	}
	return handler(ctx, req)
}
