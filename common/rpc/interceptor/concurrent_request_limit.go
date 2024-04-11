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
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/quotas/calculator"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

type (
	// ConcurrentRequestLimitInterceptor intercepts requests to the server and enforces a limit on the number of
	// requests that can be in-flight at any given time, according to the configured quotas.
	ConcurrentRequestLimitInterceptor struct {
		namespaceRegistry namespace.Registry
		logger            log.Logger
		quotaCalculator   calculator.NamespaceCalculator
		// tokens is a map of method name to the number of tokens that should be consumed for that method. If there is
		// no entry for a method, then no tokens will be consumed, so the method will not be limited.
		tokens map[string]int

		sync.Mutex
		activeTokensCount map[string]*int32
	}
)

var (
	_ grpc.UnaryServerInterceptor = (*ConcurrentRequestLimitInterceptor)(nil).Intercept

	ErrNamespaceCountLimitServerBusy = &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: "namespace concurrent poller limit exceeded",
	}
)

func NewConcurrentRequestLimitInterceptor(
	namespaceRegistry namespace.Registry,
	memberCounter calculator.MemberCounter,
	logger log.Logger,
	perInstanceQuota func(ns string) int,
	globalQuota func(ns string) int,
	tokens map[string]int,
) *ConcurrentRequestLimitInterceptor {
	return &ConcurrentRequestLimitInterceptor{
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
		quotaCalculator: calculator.NewLoggedNamespaceCalculator(
			calculator.ClusterAwareNamespaceQuotaCalculator{
				MemberCounter:    memberCounter,
				PerInstanceQuota: perInstanceQuota,
				GlobalQuota:      globalQuota,
			},
			log.With(logger, tag.ComponentLongPollHandler, tag.ScopeNamespace),
		),
		tokens:            tokens,
		activeTokensCount: make(map[string]*int32),
	}
}

func (ni *ConcurrentRequestLimitInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	nsName := MustGetNamespaceName(ni.namespaceRegistry, req)
	mh := GetMetricsHandlerFromContext(ctx, ni.logger)
	cleanup, err := ni.Allow(nsName, info.FullMethod, mh, req)
	defer cleanup()
	if err != nil {
		return nil, err
	}

	return handler(ctx, req)
}

func (ni *ConcurrentRequestLimitInterceptor) Allow(
	namespaceName namespace.Name,
	methodName string,
	mh metrics.Handler,
	req any,
) (func(), error) {
	// token will default to 0
	token := ni.tokens[methodName]

	if token == 0 {
		return func() {}, nil
	}
	// for GetWorkflowExecutionHistoryRequest, we only care about long poll requests
	longPollReq, ok := req.(*workflowservice.GetWorkflowExecutionHistoryRequest)
	if ok && !longPollReq.WaitNewEvent {
		// ignore non-long-poll GetHistory calls.
		return func() {}, nil
	}

	counter := ni.counter(namespaceName, methodName)
	count := atomic.AddInt32(counter, int32(token))
	cleanup := func() { atomic.AddInt32(counter, -int32(token)) }

	mh.Gauge(metrics.ServicePendingRequests.Name()).Record(float64(count))

	// frontend.namespaceCount is applied per poller type temporarily to prevent
	// one poller type to take all token waiting in the long poll.
	if float64(count) > ni.quotaCalculator.GetQuota(namespaceName.String()) {
		return cleanup, ErrNamespaceCountLimitServerBusy
	}
	return cleanup, nil
}

func (ni *ConcurrentRequestLimitInterceptor) counter(
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

func (ni *ConcurrentRequestLimitInterceptor) getTokenKey(
	namespace namespace.Name,
	methodName string,
) string {
	return namespace.String() + "/" + methodName
}
