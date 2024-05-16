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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/quotas/calculator"
	"go.temporal.io/server/common/quotas/quotastest"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
)

type nsCountLimitTestCase struct {
	// name of the test case
	name string
	// request to be intercepted by the ConcurrentRequestLimitInterceptor
	request any
	// numBlockedRequests is the number of pending requests that will be blocked including the final request.
	numBlockedRequests int
	// memberCounter returns the number of members in the namespace.
	memberCounter calculator.MemberCounter
	// perInstanceLimit is the limit on the number of pending requests per-instance.
	perInstanceLimit int
	// globalLimit is the limit on the number of pending requests across all instances.
	globalLimit int
	// methodName is the fully-qualified name of the gRPC method being intercepted.
	methodName string
	// tokens is a map of method slugs (e.g. just the part of the method name after the final slash) to the number of
	// tokens that will be consumed by that method.
	tokens map[string]int
	// expectRateLimit is true if the interceptor should respond with a rate limit error.
	expectRateLimit bool
}

// TestNamespaceCountLimitInterceptor_Intercept verifies that the ConcurrentRequestLimitInterceptor responds with a rate
// limit error when requests would exceed the concurrent poller limit for a namespace.
func TestNamespaceCountLimitInterceptor_Intercept(t *testing.T) {
	t.Parallel()
	for _, tc := range []nsCountLimitTestCase{
		{
			name:               "no limit exceeded",
			request:            nil,
			numBlockedRequests: 2,
			perInstanceLimit:   2,
			globalLimit:        4,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens: map[string]int{
				"/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace": 1,
			},
			expectRateLimit: false,
		},
		{
			name:               "per-instance limit exceeded",
			request:            nil,
			numBlockedRequests: 3,
			perInstanceLimit:   2,
			globalLimit:        4,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens: map[string]int{
				"/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace": 1,
			},
			expectRateLimit: true,
		},
		{
			name:               "global limit exceeded",
			request:            nil,
			numBlockedRequests: 3,
			perInstanceLimit:   3,
			globalLimit:        4,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens: map[string]int{
				"/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace": 1,
			},
			expectRateLimit: true,
		},
		{
			name:               "global limit zero",
			request:            nil,
			numBlockedRequests: 3,
			perInstanceLimit:   3,
			globalLimit:        0,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens: map[string]int{
				"/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace": 1,
			},
			expectRateLimit: false,
		},
		{
			name:               "method name does not consume token",
			request:            nil,
			numBlockedRequests: 3,
			perInstanceLimit:   2,
			globalLimit:        4,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens:             map[string]int{},
			expectRateLimit:    false,
		},
		{
			name:               "long poll request",
			request:            &workflowservice.GetWorkflowExecutionHistoryRequest{WaitNewEvent: true},
			numBlockedRequests: 3,
			perInstanceLimit:   2,
			globalLimit:        4,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory",
			tokens: map[string]int{
				"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory": 1,
			},
			expectRateLimit: true,
		},
		{
			name:               "non-long poll request",
			request:            &workflowservice.GetWorkflowExecutionHistoryRequest{WaitNewEvent: false},
			numBlockedRequests: 3,
			perInstanceLimit:   2,
			globalLimit:        4,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory",
			tokens: map[string]int{
				"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory": 1,
			},
			expectRateLimit: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// run the test case by simulating a bunch of blocked pollers, sending a final request, and verifying that it is either
// rate limited or not.
func (tc *nsCountLimitTestCase) run(t *testing.T) {
	ctrl := gomock.NewController(t)
	handler := tc.createRequestHandler()
	interceptor := tc.createInterceptor(ctrl)
	// Spawn a bunch of blocked requests in the background.
	tc.spawnBlockedRequests(handler, interceptor)

	// With all the blocked requests in flight, send the final request and verify whether it is rate limited or not.
	_, err := interceptor.Intercept(context.Background(), tc.request, &grpc.UnaryServerInfo{
		FullMethod: tc.methodName,
	}, noopHandler)

	if tc.expectRateLimit {
		assert.ErrorContains(t, err, "namespace concurrent poller limit exceeded")
	} else {
		assert.NoError(t, err)
	}

	// Clean up by unblocking all the requests.
	handler.Unblock()

	for i := 0; i < tc.numBlockedRequests-1; i++ {
		assert.NoError(t, <-handler.errs)
	}
}

func (tc *nsCountLimitTestCase) createRequestHandler() *testRequestHandler {
	return &testRequestHandler{
		started: make(chan struct{}),
		respond: make(chan struct{}),
		errs:    make(chan error, tc.numBlockedRequests-1),
	}
}

// spawnBlockedRequests sends a bunch of requests to the interceptor which will block until signaled.
func (tc *nsCountLimitTestCase) spawnBlockedRequests(
	handler *testRequestHandler,
	interceptor *ConcurrentRequestLimitInterceptor,
) {
	for i := 0; i < tc.numBlockedRequests-1; i++ {
		go func() {
			_, err := interceptor.Intercept(context.Background(), tc.request, &grpc.UnaryServerInfo{
				FullMethod: tc.methodName,
			}, handler.Handle)
			handler.errs <- err
		}()
	}

	for i := 0; i < tc.numBlockedRequests-1; i++ {
		<-handler.started
	}
}

func (tc *nsCountLimitTestCase) createInterceptor(ctrl *gomock.Controller) *ConcurrentRequestLimitInterceptor {
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespace(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()

	interceptor := NewConcurrentRequestLimitInterceptor(
		registry,
		tc.memberCounter,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(tc.perInstanceLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(tc.globalLimit),
		tc.tokens,
	)

	return interceptor
}

// noopHandler is a grpc.UnaryHandler which does nothing.
func noopHandler(context.Context, interface{}) (interface{}, error) {
	return nil, nil
}

// testRequestHandler provides a grpc.UnaryHandler which signals when it starts and does not respond until signaled.
type testRequestHandler struct {
	started chan struct{}
	respond chan struct{}
	errs    chan error
}

func (h testRequestHandler) Unblock() {
	close(h.respond)
}

// Handle signals that the request has started and then blocks until signaled to respond.
func (h testRequestHandler) Handle(context.Context, interface{}) (interface{}, error) {
	h.started <- struct{}{}
	<-h.respond

	return nil, nil
}
