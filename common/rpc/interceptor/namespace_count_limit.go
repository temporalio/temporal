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
	"sync"
	"sync/atomic"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

var (
	ErrNamespaceCountLimitServerBusy = serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "namespace concurrent poller limit exceeded")
)

type (
	NamespaceCountLimitInterceptor struct {
		namespaceRegistry namespace.Registry
		logger            log.Logger

		countFn func(namespace string) int
		tokens  map[string]int

		sync.Mutex
		activeTokensCount map[string]*int32
	}
)

var _ grpc.UnaryServerInterceptor = (*NamespaceCountLimitInterceptor)(nil).Intercept

func NewNamespaceCountLimitInterceptor(
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	countFn func(namespace string) int,
	tokens map[string]int,
) *NamespaceCountLimitInterceptor {
	return &NamespaceCountLimitInterceptor{
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
		countFn:           countFn,
		tokens:            tokens,
		activeTokensCount: make(map[string]*int32),
	}
}

func (ni *NamespaceCountLimitInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	_, methodName := splitMethodName(info.FullMethod)
	// token will default to 0
	token := ni.tokens[methodName]
	if token != 0 {
		nsName := GetNamespace(ni.namespaceRegistry, req)
		counter := ni.counter(nsName, methodName)
		count := atomic.AddInt32(counter, int32(token))
		defer atomic.AddInt32(counter, -int32(token))

		handler := GetMetricsHandlerFromContext(ctx, ni.logger)
		handler.Gauge(metrics.ServicePendingRequests.GetMetricName()).Record(float64(count))

		// frontend.namespaceCount is applied per poller type temporarily to prevent
		// one poller type to take all token waiting in the long poll.
		if int(count) > ni.countFn(nsName.String()) {
			return nil, ErrNamespaceCountLimitServerBusy
		}
	}

	return handler(ctx, req)
}

func (ni *NamespaceCountLimitInterceptor) counter(
	namespace namespace.Name,
	methodName string,
) *int32 {
	key := ni.getTokenKey(namespace, methodName)

	ni.Lock()
	defer ni.Unlock()

	counter, ok := ni.activeTokensCount[key]
	if !ok {
		counter = new(int32)
		ni.activeTokensCount[key] = counter
	}
	return counter
}

func (ni *NamespaceCountLimitInterceptor) getTokenKey(
	namespace namespace.Name,
	methodName string,
) string {
	return namespace.String() + "/" + methodName
}
